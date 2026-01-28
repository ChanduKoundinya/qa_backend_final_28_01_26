import os
import logging
import requests
from datetime import datetime
from bson.objectid import ObjectId
from bson.errors import InvalidId
import gridfs
import json
import ast
from flask import request, jsonify, send_file, current_app
import pandas as pd
from app.engine.incident import generate_incident_report
import tempfile  # <--- Needed for temp file generation
from app.engine.reporting import generate_docx_report
# Import extensions (Database & Scheduler)
from app.extensions import mongo, scheduler
from . import tasks_bp
import io
import re

from flask_jwt_extended import jwt_required

def api_response(data=None, message="", status=200):
    return jsonify({
        "status": "success" if status < 400 else "error",
        "message": message,
        "data": data
    }), status
    
# --- BACKGROUND JOB (The Trigger) ---
# --- BACKGROUND JOB (The Trigger) ---
def run_scheduled_job(task_id, app_instance, features=None):
    """
    Hybrid Trigger: 
    - If 'incident_report': Runs LOCALLY on Backend.
    - If 'audit/score': Sends to Core Service (Original Logic).
    """
    with app_instance.app_context():
        # --- Common Setup ---
        try:
            # 1. Status Tracking
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
                    grid_out.seek(0)
                    if task['filename'].endswith('.csv'):
                        df = pd.read_csv(grid_out)
                    else:
                        df = pd.read_excel(grid_out)
                    
                    feat_list = eval(features) if isinstance(features, str) else (features or [])
                    output_bytes = generate_incident_report(df, feat_list)

                    filename = f"Report_{task_id}.xlsx"
                    excel_id = current_app.fs.put(
                        output_bytes, 
                        filename=filename,
                        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )

                    mongo.db.tasks.update_one({'_id': ObjectId(task_id)}, {
                        '$set': {
                            'status': 'complete',
                            'output_excel_id': excel_id,
                            'completed_at': datetime.now()
                        }
                    })

                    mongo.db.INCIDENT_RESULTS.insert_one({
                        "task_id": task_id,
                        "report_file_id": excel_id,
                        "generated_at": datetime.now(),
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

            # 7. Prepare Final Payload
            data = {
                'task_id': task_id,
                'criteria': str(criteria),        # ✅ Correct Filtered Rules
                'analysis_type': analysis_mode,   # ✅ Correct Output Mode
                'audit_category': audit_category, # ✅ Pass Category context
                'api_key': api_key,
                'features': str(features) if features else "[]",
                'scoring_logic': json.dumps(scoring_logic)
            }

            # --- 🔴 END OF NEW LOGIC INSERTION 🔴 ---

            logging.info(f"🚀 Sending Task {task_id} payload to Core...")

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

        # ---------------------------------------------------------
        # 🕒 IMPROVED SCHEDULING LOGIC (From your "Below" Code)
        # ---------------------------------------------------------
        run_date = datetime.now()
        is_scheduled = False

        if schedule_time_str:
            try:
                target_date = datetime.strptime(schedule_time_str, "%Y-%m-%dT%H:%M")
                
                # ✅ ADDED: Validation to prevent past dates
                if target_date < datetime.now():
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
                "created_at": datetime.now(),
                "scheduled_for": run_date
            }
            
            task_result = mongo.db.tasks.insert_one(task)
            task_id = str(task_result.inserted_id)

        except Exception as db_e:
            logging.error(f"❌ Database/GridFS Error: {db_e}")
            return api_response(message="Database write failed.", status=500)

        # 5. Schedule Job
        try:
            real_app_object = current_app._get_current_object()
            
            # 🟢 FIX: Use explicit keyword arguments for 'id' and 'func'
            scheduler.add_job(
                id=task_id,                  # 1. ID comes first (or use keyword like this)
                func=run_scheduled_job,      # 2. The function to run
                trigger='date',              # 3. Trigger type
                run_date=run_date,           # 4. When to run
                args=[task_id, real_app_object, None], # 5. Arguments
                replace_existing=True,       # 6. Safety parameters
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
                "scheduled_at": run_date.isoformat(),
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

            tasks_list.append({
                '_id': str(task['_id']),
                'filename': task.get('filename') or "Batch/Unknown File",
                'status': task.get('status'),
                'audit_category': category,
                'analysis_type': task.get('analysis_type'),
                'created_at': task.get('created_at').strftime('%Y-%m-%d') if task.get('created_at') else None,
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
        task_id = request.form.get('task_id')
        results_str = request.form.get('audit_results')
        analysis_type = request.form.get('analysis_type', 'score_only') 

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
                unique_name = f"results_{task_id}.xlsx"
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
            df_results = pd.DataFrame(audit_data)
            
            # Safe drop (your existing check was good, kept it)
            if 'Issues Count' in df_results.columns:
                df_results = df_results.drop(columns=['Issues Count'])

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
                
                filename = f"results_{task_id}.xlsx"
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
                    
                    generate_docx_report(df_results, temp_path)
                    
                    with open(temp_path, 'rb') as f:
                        docx_id = current_app.fs.put(f, filename=f"report_{task_id}.docx")
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
                'completed_at': datetime.now()
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

        # 2. Encryption Check
        file_pos = file.tell()
        header = file.read(8)
        file.seek(file_pos)
        OLE_MAGIC = b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'
        if file.filename.endswith('.xlsx') and header == OLE_MAGIC:
            return api_response(message='Cannot read encrypted file.', status=400)

        # 3. "Header Hunting" Validation
        REQUIRED_COLUMNS = {'Priority', 'Created Time'}
        is_valid = False
        
        try:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)
            
            df.columns = df.columns.astype(str).str.strip()
            
            # Check row 0
            if REQUIRED_COLUMNS.issubset(set(df.columns)):
                is_valid = True
            else:
                # Scan next 10 rows
                for i in range(1, 11):
                    file.seek(0)
                    if file.filename.endswith('.csv'):
                        df = pd.read_csv(file, header=i)
                    else:
                        df = pd.read_excel(file, header=i)
                    
                    df.columns = df.columns.astype(str).str.strip()
                    if REQUIRED_COLUMNS.issubset(set(df.columns)):
                        is_valid = True
                        break
        except Exception as e:
            return api_response(message=f"File validation failed: {str(e)}", status=400)

        if not is_valid:
            return api_response(message=f"Invalid columns. Required: {REQUIRED_COLUMNS}", status=400)

        # 4. Save to GridFS
        file.seek(0) # Reset pointer before saving
        input_file_id = current_app.fs.put(file, filename=file.filename)

        # 5. Get Active Features (Safe Query)
        # Note: We do NOT use ObjectId here, just a standard find.
        feats = [f.get('sheet_name_prefix') for f in mongo.db.incident_features.find({"is_active": True}) if f.get('sheet_name_prefix')]
        if not feats: feats = [str(i) for i in range(1, 11)] # Default 1-10

        # 6. Create Task & Trigger Core
        task = {
            "filename": file.filename,
            "input_file_id": input_file_id,
            "status": "queued",
            "analysis_type": "incident_report",
            "created_at": datetime.now()
        }
        res = mongo.db.tasks.insert_one(task)
        task_id = str(res.inserted_id)

        # 7. Schedule Job (Robust Config)
        try:
            real_app = current_app._get_current_object()
            
            scheduler.add_job(
                id=task_id,                # 🟢 Bind Job ID to Task ID
                func=run_scheduled_job,    # 🟢 Explicit func arg
                trigger='date',
                run_date=datetime.now(),
                args=[task_id, real_app], 
                kwargs={'features': feats},
                replace_existing=True,
                misfire_grace_time=60      # 🟢 Allow late start
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
