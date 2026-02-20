import os
import logging
import requests
from datetime import datetime, timezone
from bson.objectid import ObjectId
from bson.errors import InvalidId
import gridfs
import json
import ast
from flask import request, jsonify, send_file, current_app, g
import pandas as pd
from app.engine.incident import generate_incident_report
import tempfile  # <--- Needed for temp file generation
from app.engine.reporting import generate_docx_report
# Import extensions (Database & Scheduler)
from app.extensions import mongo, scheduler
from . import tasks_bp
import io
import re

from flask_jwt_extended import jwt_required, get_jwt

def api_response(data=None, message="", status=200):
    return jsonify({
        "status": "success" if status < 400 else "error",
        "message": message,
        "data": data
    }), status
    
def get_utc_now():
    """Returns current time in UTC, timezone-aware."""
    return datetime.now(timezone.utc)

def format_to_iso_z(dt):
    """
    Converts a datetime object to 'YYYY-MM-DDTHH:MM:SSZ' string.
    Handles MongoDB naive datetimes by assuming they are UTC.
    """
    if not dt:
        return None
    
    # If the datetime object has no timezone (MongoDB default), set it to UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
        
    # isoformat() returns "+00:00" for UTC; we replace it with "Z"
    return dt.isoformat().replace("+00:00", "Z")


# --- BACKGROUND JOB (The Trigger) ---
def run_scheduled_job(task_id, app_instance, project_code, features=None):
    """
    Hybrid Trigger: 
    - If 'incident_report': Runs LOCALLY on Backend.
    - If 'audit/score': Sends to Core Service (Original Logic).
    """
    with app_instance.app_context():
        # --- Common Setup ---

        g.current_tenant = project_code

        try:
            # 1. Status Tracking
            logging.info(f"⚙️ Running Task {task_id} for Project: {project_code}")
            mongo.db.tasks.update_one(
                {'_id': ObjectId(task_id)}, 
                {'$set': {'status': 'processing'}}
            )

            # 2. Fetch Task
            task = mongo.db.tasks.find_one({'_id': ObjectId(task_id)})
            if not task:
                logging.error(f"Task {task_id} not found in DB.")
                return
            
            # =========================================================
            # 🟢 NEW: LOCAL INCIDENT PROCESSING BRANCH
            # =========================================================
            # (... This section remains exactly the same as your existing code ...)
            if task.get('analysis_type') == 'incident_report':
                logging.info(f"⚙️ Incident Report detected. Processing LOCALLY for Task {task_id}")
                
                try:
                    grid_out = current_app.fs.get(ObjectId(task['input_file_id']))
                    user_tz = task.get('user_tz', 'UTC')
                    grid_out.seek(0)
                    if task['filename'].endswith('.csv'):
                        df = pd.read_csv(grid_out)
                    else:
                        df = pd.read_excel(grid_out)
                    
                    feat_list = eval(features) if isinstance(features, str) else (features or [])
                    output_bytes = generate_incident_report(df, feat_list, user_tz)

                    filename = f"Incident_Report_{task_id}.xlsx"
                    excel_id = current_app.fs.put(
                        output_bytes, 
                        filename=filename,
                        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )

                    mongo.db.tasks.update_one({'_id': ObjectId(task_id)}, {
                        '$set': {
                            'status': 'complete',
                            'output_excel_id': excel_id,
                            'completed_at': get_utc_now()
                        }
                    })

                    mongo.db.INCIDENT_RESULTS.insert_one({
                        "task_id": task_id,
                        "report_file_id": excel_id,
                        "generated_at": get_utc_now(),
                        "file_name": filename
                    })

                    logging.info(f"✅ Local Incident Processing Complete for {task_id}")
                    return 

                except Exception as local_e:
                    logging.error(f"❌ Local Engine Failed: {local_e}")
                    mongo.db.tasks.update_one(
                        {'_id': ObjectId(task_id)}, 
                        {'$set': {'status': 'error', 'error_message': str(local_e)}}
                    )
                    return 

            # =========================================================
            # 🔵 CORE SERVICE TRIGGER (For Audits)
            # =========================================================
            
            # 1. Setup URL
            base_url = current_app.config['CORE_SERVICE_URL'].rstrip('/')
            core_url = f"{base_url}/internal/process-task"
        
            if not core_url:
                logging.error("❌ CORE_SERVICE_URL not found in Config.")
                return
            
            logging.info(f"🔍 Debug: Attempting to connect to CORE_URL: {core_url}")

            # 2. Update Status
            mongo.db.tasks.update_one(
                {'_id': ObjectId(task_id)}, 
                {'$set': {'status': 'sending_payload'}}
            )

            # --- 🟢 START OF NEW LOGIC INSERTION 🟢 ---
            
            # 3. Get the DOMAIN (The "What") - Ticket vs Call
            # This determines WHICH criteria we fetch from the DB
            audit_category = task.get('audit_category', 'ticket audit') 
            
            criteria_query = {
                'is_active': True,
                'type': audit_category 
            }
            # Fetch specific criteria
            criteria = list(mongo.db.criteria.find(criteria_query, {'_id': 0, 'name': 1, 'weight': 1}))

            if not criteria:
                logging.warning(f"⚠️ No active criteria found for category: '{audit_category}'. Sending empty list.")

            condition_doc = mongo.db.conditions.find_one({"type": "ticket_audit"})
            
            # Default fallback if DB is empty (Safety Check)
            if condition_doc:
                scoring_logic = condition_doc.get('scoring_logic')
            else:
                scoring_logic = {"Demonstrated": 10, "Needs Training": 5, "Immediate Retrain": 0}
            # =========================================================

            # 4. Get the PROCESS MODE (The "How") - Score Only vs Full Report
            # This tells Core Service whether to generate DOCX or just Excel
            analysis_mode = task.get('analysis_type', 'score_only')

            # 5. Fetch API Key (Preserving existing logic)
            config_doc = mongo.db.api_config.find_one({"name": "openai_api_key"})
            api_key = config_doc.get("key") if config_doc else None

            if not api_key:
                logging.error(f"❌ Aborting Task {task_id}: OpenAI API Key not found in DB Config.")
                return

            # 6. Get File (Preserving existing logic)
            grid_out = current_app.fs.get(ObjectId(task['input_file_id']))
            files = {'file': (task['filename'], grid_out, 'text/csv')}

            passport_task_id = f"{project_code}___{task_id}"

            # 7. Prepare Final Payload
            data = {
                'task_id': passport_task_id,
                'criteria': str(criteria),        # ✅ Correct Filtered Rules
                'analysis_type': analysis_mode,   # ✅ Correct Output Mode
                'audit_category': audit_category, # ✅ Pass Category context
                'api_key': api_key,
                'features': str(features) if features else "[]",
                'scoring_logic': json.dumps(scoring_logic)
            }

            # --- 🔴 END OF NEW LOGIC INSERTION 🔴 ---

            logging.info(f"🚀 Sending Task {task_id} (Passport: {passport_task_id}) to Core...")

            response = requests.post(core_url, files=files, data=data, timeout=14400)

            if response.status_code == 200:
                logging.info(f"✅ Core Service accepted payload for Task {task_id}")
            else:
                logging.error(f"⚠️ Core Service returned error: {response.text}")
                mongo.db.tasks.update_one(
                    {'_id': ObjectId(task_id)}, 
                    {'$set': {'status': 'error', 'error_message': f"Core Refused: {response.status_code}"}}
                )

        except Exception as e:
            logging.error(f"❌ Failed to process task: {e}")
            mongo.db.tasks.update_one(
                {'_id': ObjectId(task_id)}, 
                {'$set': {'status': 'error', 'error_message': str(e)}}
            )
# --- ROUTES ---
@tasks_bp.route('/api/tasks/upload', methods=['POST'])
@jwt_required()
def upload_file():
    """
    Handles file upload with enhanced validation and robust scheduling.
    """
    try:
        # --- RISK 1: No File Selected ---
        if 'file' not in request.files:
            return api_response(message='No file part provided', status=400)
        
        file = request.files['file']
        
        # --- RISK 2: Empty Filename ---
        if file.filename == '':
            return api_response(message='No selected file', status=400)

        # --- RISK 3: Unsupported Extension ---
        filename = file.filename
        if not filename.lower().endswith(('.csv', '.xlsx', '.xls')):
            return api_response(message='Invalid file type. Allowed: .csv, .xlsx, .xls', status=400)

        # --- RISK 4: Corrupt/Empty File (Zero Bytes) ---
        file.seek(0, os.SEEK_END)
        file_size = file.tell()
        file.seek(0) # Reset cursor for reading/saving
        if file_size == 0:
            return api_response(message='File is empty.', status=400)
        
        try:
            if filename.lower().endswith('.csv'):
                df_temp = pd.read_csv(file)
            else:
                df_temp = pd.read_excel(file)
            total_tickets_count = len(df_temp)
            file.seek(0) # Reset cursor again so GridFS can save it properly!
        except Exception as parse_e:
            return api_response(message=f"File parsing failed: {str(parse_e)}", status=400)

        # 2. Validation: Parse Inputs
        schedule_time_str = request.form.get('schedule_time') 
        analysis_type = request.form.get('reportType', 'score_only')
        audit_category = request.form.get('auditCategory', 'ticket audit') 

        # --- RISK 5: Missing/Invalid Form Data ---
        ALLOWED_TYPES = {'score_only', 'full_report', 'incident_report'}
        if analysis_type not in ALLOWED_TYPES:
            return api_response(message=f"Invalid reportType. Allowed: {list(ALLOWED_TYPES)}", status=400)

        if not audit_category or not isinstance(audit_category, str) or len(audit_category.strip()) == 0:
             return api_response(message="Invalid or missing auditCategory.", status=400)
        #-----------------------------------------------------------------------------------
        claims = get_jwt()
        current_project = claims.get('project')
        username = claims.get("username", "Unknown User") 
        user_tz = request.form.get('timezone', 'UTC')
        
        if not current_project:
            return api_response(message="Project context missing in token", status=400)
        #-----------------------------------------------------------------------------------

        # ---------------------------------------------------------
        # 🕒 IMPROVED SCHEDULING LOGIC (From your "Below" Code)
        # ---------------------------------------------------------
        run_date = get_utc_now() # ✅ Default to immediate UTC execution
        is_scheduled = False

        if schedule_time_str:
            try:
                # 1. Parse the string into a naive datetime
                naive_target_date = datetime.strptime(schedule_time_str, "%Y-%m-%dT%H:%M")
                
                # 2. Assign the UTC timezone to make it aware
                target_date = naive_target_date.replace(tzinfo=timezone.utc)
                
                # 3. Compare aware datetime with aware datetime
                if target_date < get_utc_now(): 
                    return api_response(message="Scheduled time cannot be in the past.", status=400)
                
                run_date = target_date
                is_scheduled = True
            except ValueError:
                return api_response(message="Invalid date format. Use YYYY-MM-DDTHH:MM", status=400)

        # --- RISK 6: DB/Scheduler Down ---
        try:
            # 3. Save to GridFS
            input_file_id = current_app.fs.put(file, filename=filename)

            # 4. Create Task Record
            # We insert directly with the calculated 'status' and 'run_date'
            task = {
                "filename": filename,
                "input_file_id": input_file_id, 
                "status": "scheduled" if is_scheduled else "queued",
                "analysis_type": analysis_type,   
                "audit_category": audit_category, 
                "created_at": get_utc_now(),
                "scheduled_for": run_date,
                "created_by": username,
                "user_tz": user_tz,
                "total_tickets": total_tickets_count
            }
            
            task_result = mongo.db.tasks.insert_one(task)
            task_id = str(task_result.inserted_id)

        except Exception as db_e:
            logging.error(f"❌ Database/GridFS Error: {db_e}")
            return api_response(message="Database write failed.", status=500)

        # 5. Schedule Job
        try:
            real_app_object = current_app._get_current_object()
            
            scheduler.add_job(
                id=task_id,
                func=run_scheduled_job,
                trigger='date',
                run_date=run_date,
                # 🟢 PASS 'current_project' AS THE 3RD ARGUMENT
                args=[task_id, real_app_object, current_project, None], 
                replace_existing=True,
                misfire_grace_time=60
            )
            
            logging.info(f"✅ Job scheduled for Task {task_id} at {run_date}")

        except Exception as sched_e:
            logging.error(f"❌ Scheduler Error task {task_id}: {sched_e}")
            # Rollback status
            mongo.db.tasks.update_one(
                {'_id': task_result.inserted_id}, 
                {'$set': {'status': 'error', 'error': 'Scheduling failed'}}
            )
            return api_response(message="File saved but scheduling failed.", status=500)

        # 6. Success
        return api_response(
            message="Task successfully queued",
            status=201, 
            data={
                "task_id": task_id,
                "filename": filename,
                "status": "scheduled" if is_scheduled else "queued",
                "scheduled_at": format_to_iso_z(run_date),
                "analysis_type": analysis_type,
                "audit_category": audit_category
            }
        )

    except Exception as e:
        logging.error(f"❌ Upload Critical Error: {e}")
        return api_response(message="Internal Server Error", status=500)
    
    
@tasks_bp.route('/api/status/<task_id>', methods=['GET'])
@jwt_required()
def get_task_status(task_id):
    """
    Check status. Returns output_ids if complete.
    Mitigation: Handles Invalid ID formats gracefully.
    """
    try:
        # 🟢 RISK MITIGATION: Invalid ID Format
        # We validate the ID format BEFORE querying the DB
        try:
            oid = ObjectId(task_id)
        except InvalidId:
            return jsonify({'status': 'error', 'error': 'Invalid Task ID format'}), 400

        # Now query with the safe ObjectId
        task = mongo.db.tasks.find_one({'_id': oid})
        
        # 🟢 RISK MITIGATION: Task Not Found (Already handled, just confirmed)
        if not task:
            return jsonify({'status': 'error', 'error': 'Task not found'}), 404

        status = task.get("status")
        response = {'status': status, 'task_id': str(task['_id'])}

        if status == "complete":
            response['excel_id'] = str(task.get('output_excel_id'))
            response['docx_id'] = str(task.get('output_docx_id'))
        elif status == "error":
            response['error'] = task.get('error_message')

        return jsonify(response)

    except Exception as e:
        # Catch-all for other unexpected errors
        return jsonify({'status': 'error', 'error': str(e)}), 500

@tasks_bp.route('/api/tasks/', methods=['GET'])
def api_get_tasks():
    """
    Returns list of tasks.
    Supports filtering by category: /api/tasks?category=call_audit
    Smart Filter: Checks 'audit_category' AND legacy 'analysis_type' fields.
    Mitigation: Added .limit(100) to prevent timeouts on large datasets.
    """
    try:
        category_filter = request.args.get('category')
        query = {}

        if category_filter:
            # 🟢 RISK MITIGATION: Regex Injection
            # Your existing code already handles this perfectly with re.escape
            safe_pattern = re.escape(category_filter).replace('_', '[ _]')
            regex_query = {'$regex': f'^{safe_pattern}$', '$options': 'i'}

            # SMART FILTER LOGIC
            if 'incident' in category_filter.lower():
                query = {
                    '$or': [
                        {'audit_category': regex_query},
                        {'analysis_type': 'incident_report'}
                    ]
                }
            elif 'ticket' in category_filter.lower():
                query = {
                    '$or': [
                        {'audit_category': regex_query},
                        {
                            'analysis_type': {'$in': ['score_only', 'full_report']},
                            'type': {'$ne': 'call_audit'} 
                        }
                    ]
                }
            else:
                query['audit_category'] = regex_query

        # Fetch from DB
        # 🟢 RISK MITIGATION: Large Dataset Timeout
        # Added .limit(100) to ensure the query returns fast even if DB has 10k+ records.
        # This acts as a safety valve until you implement full pagination.
        all_tasks_cursor = mongo.db.tasks.find(query).sort("created_at", -1).limit(100)
        
        tasks_list = []
        for task in all_tasks_cursor:
            # Determine category for Display (Handle legacy "unknowns")
            category = task.get('audit_category')
            if not category or category == 'unknown':
                a_type = task.get('analysis_type')
                if a_type == 'incident_report':
                    category = 'incident'
                elif a_type in ['score_only', 'full_report']:
                    category = 'ticket_audit'
                else:
                    category = task.get('type') or 'unknown'
            
            created_at_raw = task.get('created_at')
            formatted_time = created_at_raw.strftime('%Y-%m-%d %H:%M:%S') if created_at_raw else "N/A"

            tasks_list.append({
                '_id': str(task['_id']),
                'filename': task.get('filename') or "Batch/Unknown File",
                'status': task.get('status'),
                'audit_category': category,
                'analysis_type': task.get('analysis_type'),
                'uploaded_by': task.get('created_by', 'System/Unknown'), # Fallback for old tasks
                'uploaded_at': format_to_iso_z(task.get('created_at')), 
                'created_at': format_to_iso_z(task.get('created_at')),
                'scheduled_for': format_to_iso_z(task.get('scheduled_for')),
                'completed_at': format_to_iso_z(task.get('completed_at')),
                'output_excel_id': str(task.get('output_excel_id')) if task.get('output_excel_id') else None,
                'output_docx_id': str(task.get('output_docx_id')) if task.get('output_docx_id') else None
            })
            
        return jsonify(tasks_list)

    except Exception as e:
        logging.error(f"Error fetching tasks: {e}")
        return jsonify({"error": "Failed to fetch tasks"}), 500
    
@tasks_bp.route('/api/tasks/download/excel/<task_id>')
@jwt_required()
def download_excel_file(task_id):
    try:
        # 🟢 RISK MITIGATION: Invalid ID Format
        try:
            oid = ObjectId(task_id)
        except InvalidId:
            return jsonify({'status': 'error', 'error': 'Invalid Task ID format'}), 400

        task = mongo.db.tasks.find_one({'_id': oid})
        
        # 🟢 RISK MITIGATION: Task Not Found
        if not task:
            return jsonify({"error": "Task not found."}), 404
            
        excel_file_id = task.get('output_excel_id')
        
        # 🟢 RISK MITIGATION: File Not Ready (NoneType Error)
        if not excel_file_id:
            status = task.get('status', 'unknown')
            return jsonify({"error": f"Report not ready yet. Current status: {status}"}), 404
        
        # 🟢 RISK MITIGATION: Ghost File (GridFS NoFile)
        try:
            grid_out = current_app.fs.get(ObjectId(excel_file_id))
            
            return send_file(
                grid_out,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=grid_out.filename or 'data.xlsx'
            )
        except gridfs.errors.NoFile:
            return jsonify({"error": "File record exists, but physical file is missing from Storage."}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@tasks_bp.route('/api/tasks/download/docx/<task_id>')
@jwt_required()
def download_docx_file(task_id):
    try:
        # 🟢 RISK MITIGATION: Invalid ID Format
        try:
            oid = ObjectId(task_id)
        except InvalidId:
            return jsonify({'status': 'error', 'error': 'Invalid Task ID format'}), 400

        task = mongo.db.tasks.find_one({'_id': oid})
        
        if not task:
            return jsonify({"error": "Task not found."}), 404

        docx_file_id = task.get('output_docx_id')
        
        # 🟢 RISK MITIGATION: File Not Ready
        if not docx_file_id:
            # Check if it was even requested
            if task.get('analysis_type') == 'score_only':
                return jsonify({"error": "DOCX was not generated (Analysis Type was 'score_only')."}), 400
            return jsonify({"error": "Report is still processing."}), 404
        
        # 🟢 RISK MITIGATION: Ghost File
        try:
            grid_out = current_app.fs.get(ObjectId(docx_file_id))
            
            return send_file(
                grid_out,
                mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                as_attachment=True,
                download_name=grid_out.filename or 'report.docx'
            )
        except gridfs.errors.NoFile:
            return jsonify({"error": "File record exists, but physical file is missing from Storage."}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500    

@tasks_bp.route('/internal/save-results', methods=['POST'])
def save_audit_results():
    """
    Endpoint called by Core Service to save results.
    Unified logic with robust error handling.
    """
    try:
        # 🟢 RISK MITIGATION: Validation Checks at Start
        raw_id = request.form.get('task_id')
        results_str = request.form.get('audit_results')
        analysis_type = request.form.get('analysis_type', 'score_only') 

        if not raw_id: 
            return jsonify({"error": "Missing task_id"}), 400
        
        task_id = raw_id # Default fallback
        if "___" in raw_id:
            try:
                # Split: "bcbsa___64f2..." -> project_code="bcbsa", task_id="64f2..."
                project_code, real_task_id = raw_id.split("___", 1)
                
                # MANUALLY ACTIVATE THE CORRECT DB
                g.current_tenant = project_code 
                task_id = real_task_id # Use the real ObjectId for DB lookups
                
                logging.info(f"🔓 Passport Accepted: Context switched to {project_code}")
            except ValueError:
                logging.error("❌ Invalid Passport ID format")
                return jsonify({"error": "Invalid ID format"}), 400
        else:
            # If we get here, it means we received a result but don't know which DB it belongs to.
            # This is a critical failure state for multi-tenancy.
            logging.error("❌ Received Result without Project Context (No Passport).")
            return jsonify({"error": "Missing Project Context in ID"}), 400

        if not task_id: 
            logging.error("❌ Save Results Failed: Missing task_id")
            return jsonify({"error": "Missing task_id"}), 400
            
        if not results_str: 
            logging.error(f"❌ Save Results Failed: Missing data for task {task_id}")
            return jsonify({"error": "Missing audit_results data"}), 400

        # Determine Category
        if analysis_type == 'incident_report':
            current_category = 'incident'
        else:
            current_category = 'ticket_audit'

        logging.info(f"💾 Receiving results for Task {task_id} ({current_category})...")

        excel_id = None
        docx_id = None

        # 2. Handle legacy file uploads (Optional)
        if 'excel_file' in request.files:
            f = request.files['excel_file']
            if f.filename != '': 
                unique_name = f"ticket_audit_Report_{task_id}.xlsx"
                excel_id = current_app.fs.put(f, filename=unique_name)

        # 3. Process Results (JSON) 
        audit_data = None
        
        # 🟢 RISK MITIGATION: Malformed JSON String
        try:
            audit_data = ast.literal_eval(results_str)
        except (ValueError, SyntaxError) as parse_err:
            logging.error(f"❌ Data Parsing Failed for Task {task_id}: {parse_err}")
            # We explicitly update the task to error so it doesn't hang
            mongo.db.tasks.update_one({'_id': ObjectId(task_id)}, {
                '$set': {'status': 'error', 'error_message': 'Core returned malformed data'}
            })
            return jsonify({"error": "Malformed data format"}), 400
        
        if audit_data:
            # 🟢 RISK MITIGATION: Missing Data Columns
            # We create the DataFrame safely
            task = mongo.db.tasks.find_one({'_id': ObjectId(task_id)})
            user_tz = task.get('user_tz', 'UTC') if task else 'UTC'
            df_results = pd.DataFrame(audit_data)
            
            # Safe drop (your existing check was good, kept it)
            if 'Issues Count' in df_results.columns:
                df_results = df_results.drop(columns=['Issues Count'])

            date_keywords = ['time', 'date', ' at']
            potential_time_cols = [col for col in df_results.columns if any(kw in col.lower() for kw in date_keywords)]
            
            for col in potential_time_cols:
                try:
                    temp_col = pd.to_datetime(df_results[col], errors='coerce')
                    if not temp_col.isna().all():
                        if temp_col.dt.tz is None:
                            temp_col = temp_col.dt.tz_localize('UTC')
                        temp_col = temp_col.dt.tz_convert(user_tz)
                        df_results[col] = temp_col.dt.strftime('%Y-%m-%d') # Strip tag for Excel/Word
                except Exception:
                    pass # Skip if it wasn't actually a date column

            logging.info(f"📊 Columns: {df_results.columns.tolist()}")

            # Save Raw Data
            for row in audit_data:
                row['task_id'] = task_id
                row['audit_category'] = current_category
            
            mongo.db.audit_reports.insert_many(audit_data)

            # Generate Excel
            try:
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_results.to_excel(writer, index=False, sheet_name='Results')
                output.seek(0)
                
                filename = f"ticket_audit_Report_{task_id}.xlsx"
                excel_id = current_app.fs.put(
                    output, 
                    filename=filename,
                    content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                logging.info(f"✅ Excel Report Generated Locally for {current_category}.")
            except Exception as e:
                logging.error(f"❌ Excel Generation Failed: {e}")

            # Generate DOCX
            if analysis_type == 'full_report':
                try:
                    logging.info("📄 Generating DOCX Report locally...")
                    with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as tmp:
                        temp_path = tmp.name
                    
                    generate_docx_report(df_results, temp_path, user_tz)
                    
                    with open(temp_path, 'rb') as f:
                        docx_id = current_app.fs.put(f, filename=f"ticket_audit_Report_{task_id}.docx")
                    os.remove(temp_path)
                except Exception as report_err:
                    logging.exception("❌ DOCX Failed") 

        # 4. Update Task Status
        mongo.db.tasks.update_one({'_id': ObjectId(task_id)}, {
            '$set': {
                'status': 'complete',
                'audit_category': current_category,
                'output_excel_id': excel_id,
                'output_docx_id': docx_id, 
                'completed_at': get_utc_now()
            }
        })

        return jsonify({"message": "Saved successfully"}), 200

    except Exception as e:
        logging.error(f"❌ Error saving results: {e}")
        return jsonify({"error": str(e)}), 500
    
@tasks_bp.route('/api/tasks/incident/upload', methods=['POST'])
@jwt_required()
def upload_incident_report():
    """
    Validates Incident file using robust logic, then triggers Core.
    """
    try:
        # 1. Basic File Check
        if 'file' not in request.files: 
            return api_response(message='No file uploaded', status=400)
        
        file = request.files['file']
        
        if file.filename == '': 
            return api_response(message='No file selected', status=400)
        
        # 🟢 GET CURRENT PROJECT
        claims = get_jwt()
        current_project = claims.get('project')
        user_tz = request.form.get('timezone', 'UTC')

        # 2. Encryption Check
        file_pos = file.tell()
        header = file.read(8)
        file.seek(file_pos)
        OLE_MAGIC = b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'
        if file.filename.endswith('.xlsx') and header == OLE_MAGIC:
            return api_response(message='Cannot read encrypted file.', status=400)

        # 3. Read & Parse File to DataFrame
        try:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)
            
            # Normalize headers (strip spaces)
            df.columns = df.columns.astype(str).str.strip()
            current_headers = list(df.columns)
            
            # 🟢 3.1 UPDATED: Define EXACTLY the 13 columns your Pandas script needs
            INCIDENT_REQUIRED = [
                'Ticket Id', 'Created Time', 'Closed Time', 'Resolved Time', 
                'Priority', 'Status', 'Type', 'Group', 'Agent', 
                'Category', 'Requester Name', 'Item', 'Resolution Time (in Hrs)','Description'
            ]
            
            # 3.2 Check for Missing Columns
            missing = [req for req in INCIDENT_REQUIRED if req not in current_headers]
            
            if missing:
                logging.info(f"⚠️ Incident Upload missing {len(missing)} columns. Asking Core AI to map...")
                
                # 🟢 1. FETCH API KEY FROM DB
                config_doc = mongo.db.api_config.find_one({"name": "openai_api_key"})
                api_key = config_doc.get("key") if config_doc else None
                
                if not api_key:
                    logging.error("❌ API Key missing in DB. Cannot perform AI mapping.")
                    return api_response(message="System configuration error (Missing API Key)", status=500)

                # Call Core Service
                core_url = current_app.config['CORE_SERVICE_URL'].rstrip('/') + "/internal/ai-map-custom"
                
                # 🟢 2. SEND KEY IN PAYLOAD
                payload = {
                    "headers": current_headers,
                    "target_fields": INCIDENT_REQUIRED, # Passed for fallback, though core now hardcodes it
                    "api_key": api_key  
                }
                
                try:
                    response = requests.post(core_url, json=payload, timeout=30)
                    
                    if response.status_code == 200:
                        mapping = response.json().get('mapping', {})
                        
                        # 🟢 ADDED: Print the mapping to the backend terminal for easy debugging
                        print("\n" + "="*50)
                        print("🔍 AI COLUMN MAPPING RESULTS (BACKEND)")
                        print("="*50)
                        for target, found in mapping.items():
                            status = f"✅ [{found}]" if found else "❌ (null)"
                            print(f"{target:25} : {status}")
                        print("="*50 + "\n")

                        # AI returns {Target: Found}, Pandas needs {Found: Target}
                        # We flip the dictionary, ignoring nulls
                        rename_dict = {v: k for k, v in mapping.items() if v}
                        
                        df.rename(columns=rename_dict, inplace=True)
                        logging.info(f"✅ AI Mapping Applied. Cleaned columns ready for DB.")
                    else:
                        logging.error(f"❌ Core AI Mapping failed: {response.text}")
                except Exception as e:
                    logging.error(f"❌ Connection to Core failed: {e}")

        except Exception as e:
            return api_response(message=f"File parsing failed: {str(e)}", status=400)

        # 4. Save Modified Data to GridFS
        output_buffer = io.BytesIO()
        if file.filename.endswith('.csv'):
            df.to_csv(output_buffer, index=False)
        else:
            with pd.ExcelWriter(output_buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
        
        output_buffer.seek(0)
        input_file_id = current_app.fs.put(output_buffer, filename=file.filename)
        
        # 5. Get Active Features
        feats = [f.get('sheet_name_prefix') for f in mongo.db.incident_features.find({"is_active": True}) if f.get('sheet_name_prefix')]
        if not feats: feats = [str(i) for i in range(1, 18)]

        # 6. Create Task & Trigger Core
        task = {
            "filename": file.filename,
            "input_file_id": input_file_id,
            "status": "queued",
            "analysis_type": "incident_report",
            "created_at": get_utc_now(),
            "user_tz": user_tz ,
            "total_tickets": len(df)  
            }
        res = mongo.db.tasks.insert_one(task)
        task_id = str(res.inserted_id)

        # 7. Schedule Job
        try:
            real_app = current_app._get_current_object()
            
            scheduler.add_job(
                id=task_id,
                func=run_scheduled_job,
                trigger='date',
                run_date=get_utc_now(),
                args=[task_id, real_app, current_project], 
                kwargs={'features': feats},
                replace_existing=True,
                misfire_grace_time=60
            )
        except Exception as sched_e:
            logging.error(f"❌ Scheduler Error: {sched_e}")
            mongo.db.tasks.update_one({'_id': res.inserted_id}, {'$set': {'status': 'error', 'error_message': 'Scheduling failed'}})
            return api_response(message="File saved but scheduling failed.", status=500)

        return api_response(
            message="Incident Report Queued", 
            status=201, 
            data={'task_id': task_id, 'status': 'queued'}
        )

    except Exception as e:
        logging.error(f"Incident Upload Error: {e}")
        return api_response(message=f"Internal Server Error: {str(e)}", status=500)
    

# 🟢 NEW ENDPOINT: Receive PII Logs from Core Service
@tasks_bp.route('/internal/save-pii-logs', methods=['POST'])
def save_pii_logs():
    """
    Receives PII logs from Core Service and saves them to MongoDB.
    """
    try:
        # 1. Get JSON Data
        log_entry = request.get_json()
        
        if not log_entry:
            return jsonify({"error": "No data provided"}), 400

        raw_task_id = log_entry.get('task_id')
        
        if not raw_task_id:
            return jsonify({"error": "Missing task_id"}), 400

        # =========================================================
        # 🟢 FIX: PASSPORT LOGIC (CONTEXT SWITCH)
        # =========================================================
        # We must tell mongo.db WHICH database to use based on the Project Code
        if "___" in raw_task_id:
            try:
                # Split: "bcbsa___64f2..." -> project_code="bcbsa", task_id="64f2..."
                project_code, real_task_id = raw_task_id.split("___", 1)
                
                # 🟢 ACTIVATE THE DB CONNECTION
                g.current_tenant = project_code 
                
                # Update the log entry to store the clean ObjectId, not the long passport string
                log_entry['task_id'] = real_task_id
                
            except ValueError:
                logging.error("❌ Invalid PII Task ID format")
                return jsonify({"error": "Invalid ID format"}), 400
        else:
            # Fallback if no passport provided (might fail if your app requires tenancy)
            logging.warning("⚠️ PII Log received without Project Context (No Passport). mongo.db might be None.")
        # =========================================================

        logging.info(f"🛡️ Saving PII Log for Task {log_entry.get('task_id')}")

        # 3. Insert into 'pii_logs' collection
        # Now that g.current_tenant is set, mongo.db is no longer None
        mongo.db.pii_logs.insert_one(log_entry)

        return jsonify({"status": "success", "message": "PII Log Saved"}), 200

    except Exception as e:
        logging.error(f"❌ Failed to save PII Log: {e}")
        return jsonify({"error": str(e)}), 500
# app/modules/task/routes.py

@tasks_bp.route('/api/pii-logs', methods=['GET'])
@jwt_required()
def get_pii_logs():
    """
    Retrieves PII audit logs with optional filtering.
    Query Params:
      - start_date (YYYY-MM-DD): Filter logs on or after this date.
      - end_date (YYYY-MM-DD): Filter logs on or before this date.
      - task_id (str): Filter by specific task.
      - limit (int): Max records to return (default 50).
    """
    try:
        # 1. Get Query Parameters
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        task_id = request.args.get('task_id')
        limit = int(request.args.get('limit', 50)) # Default to 50 to prevent huge payloads

        query = {}

        # 2. Build Date Filter (String Comparison for ISO Dates)
        if start_date_str or end_date_str:
            date_query = {}
            
            if start_date_str:
                # "2026-01-28" becomes "2026-01-28T00:00:00" (Start of day)
                date_query['$gte'] = f"{start_date_str}T00:00:00"
            
            if end_date_str:
                # "2026-01-28" becomes "2026-01-28T23:59:59" (End of day)
                date_query['$lte'] = f"{end_date_str}T23:59:59"
            
            if date_query:
                query['timestamp'] = date_query

        # 3. Optional Task Filter
        if task_id:
            query['task_id'] = task_id

        # 4. Execute Query
        # Sort by timestamp DESC (-1) to show newest logs first
        cursor = mongo.db.pii_logs.find(query).sort("timestamp", -1).limit(limit)

        logs = []
        for doc in cursor:
            logs.append({
                "id": str(doc['_id']),
                "task_id": doc.get('task_id'),
                "timestamp": doc.get('timestamp'),
                "status": doc.get('status'),
                "pii_found": doc.get('pii_found', False),
                "stats": doc.get('detection_stats', {}),
                # We rename 'processed_data_preview' to 'data' for the frontend
                "data": doc.get('processed_data_preview') 
            })

        return jsonify({
            "status": "success",
            "count": len(logs),
            "filters": {"start": start_date_str, "end": end_date_str},
            "data": logs
        }), 200

    except Exception as e:
        logging.error(f"❌ Error fetching PII logs: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500