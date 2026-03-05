import os
import logging
import requests
from datetime import datetime, timezone
import json
import ast
import io
import re
import tempfile 
import pytz
from flask import request, jsonify, send_file, current_app, g
import pandas as pd
from flask_jwt_extended import jwt_required, get_jwt
from sqlalchemy import or_
from datetime import timedelta
from app.engine.incident import generate_incident_report
from app.engine.reporting import generate_docx_report
from app.extensions import scheduler
from . import tasks_bp

# 🟢 POSTGRESQL MODELS IMPORT
from app.models import db, Task, StoredFile, AuditReport, IncidentResult, ApiConfig, Criterion, PiiLog, User, CallAuditResult
from app.utils.email_service import send_audit_email, trigger_automated_email

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
    """Converts a datetime object to 'YYYY-MM-DDTHH:MM:SSZ' string."""
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


# --- BACKGROUND JOB (The Trigger) ---
def run_scheduled_job(task_id, app_instance, project_code, features=None):
    with app_instance.app_context():
        g.current_tenant = project_code

        try:
            # 1. Fetch Task using SQLAlchemy
            task = Task.query.get(task_id)
            if not task:
                logging.error(f"Task {task_id} not found in DB.")
                return
            
            # Status Tracking
            logging.info(f"⚙️ Running Task {task_id} for Project: {project_code}")
            task.status = 'processing'
            db.session.commit()
            
            # =========================================================
            # 🟢 LOCAL INCIDENT PROCESSING BRANCH
            # =========================================================
            if task.analysis_type == 'incident_report':
                logging.info(f"⚙️ Incident Report detected. Processing LOCALLY for Task {task_id}")
                
                try:
                    # 🟢 UPDATE PROGRESS: Starting File Read
                    task.progress = 10
                    db.session.commit()

                    input_file = StoredFile.query.get(task.input_file_id)
                    grid_out = io.BytesIO(input_file.file_data)
                    user_tz = task.user_tz or 'UTC'
                    
                    if task.filename.endswith('.csv'):
                        df = pd.read_csv(grid_out)
                    else:
                        df = pd.read_excel(grid_out)
                    
                    # 🟢 UPDATE PROGRESS: Generating charts & calculations
                    task.progress = 50
                    db.session.commit()

                    feat_list = eval(features) if isinstance(features, str) else (features or [])
                    output_bytes = generate_incident_report(df, feat_list, user_tz)

                    # 🟢 UPDATE PROGRESS: Saving final files
                    task.progress = 85
                    db.session.commit()

                    filename = f"Incident_Report_{task_id}.xlsx"
                    output_bytes.seek(0)
                    
                    # Save output to StoredFile
                    new_excel_file = StoredFile(
                        filename=filename,
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        file_data=output_bytes.read(),
                        project_code=project_code
                    )
                    db.session.add(new_excel_file)
                    db.session.flush() # Get ID without committing
                    excel_id = new_excel_file.id

                    # Update Task
                    task.status = 'complete'
                    task.progress = 100
                    task.output_excel_id = excel_id
                    task.completed_at = get_utc_now()
                    
                    # Create Incident Result
                    incident_result = IncidentResult(
                        task_id=task.id,
                        report_file_id=excel_id,
                        file_name=filename,
                        project_code=project_code
                    )
                    db.session.add(incident_result)
                    db.session.commit()

                    logging.info(f"Task {task_id} completed. Checking email triggers...")
                    
                    temp_dir = tempfile.gettempdir()
                    temp_file_path = os.path.join(temp_dir, filename)
                    
                    with open(temp_file_path, 'wb') as f:
                        f.write(new_excel_file.file_data)

                    # 🟢 Call the centralized helper function
                    trigger_automated_email(task, project_code, [temp_file_path])
                    
                    # Cleanup the temp file
                    if os.path.exists(temp_file_path):
                        os.remove(temp_file_path)
                    # ==========================================

                    logging.info(f"✅ Local Incident Processing Complete for {task_id}")
                    return

                except Exception as local_e:
                    db.session.rollback()
                    logging.error(f"❌ Local Engine Failed: {local_e}")
                    task = Task.query.get(task_id)
                    task.status = 'error'
                    task.error_message = str(local_e)
                    db.session.commit()
                    return 

            # =========================================================
            # 🔵 CORE SERVICE TRIGGER (For Audits)
            # =========================================================
            base_url = current_app.config['CORE_SERVICE_URL'].rstrip('/')
            core_url = f"{base_url}/internal/process-task"
        
            if not core_url:
                logging.error("❌ CORE_SERVICE_URL not found in Config.")
                return

            task.status = 'sending_payload'
            db.session.commit()

            audit_category = task.audit_category or 'ticket audit'
            
            # Fetch active criteria for this project
            criteria_records = Criterion.query.filter_by(is_active=True, type=audit_category, project_code=project_code).all()
            criteria = [{'name': c.name, 'weight': c.weight} for c in criteria_records]

            if not criteria:
                logging.warning(f"⚠️ No active criteria found for category: '{audit_category}'. Sending empty list.")

            # Hardcoded scoring logic for now (can map to a new table later if needed)
            scoring_logic = {"Demonstrated": 10, "Needs Training": 5, "Immediate Retrain": 0}

            analysis_mode = task.analysis_type or 'score_only'

            config_doc = ApiConfig.query.filter_by(name="openai_api_key", project_code=project_code).first()
            api_key = config_doc.key if config_doc else None

            if not api_key:
                logging.error(f"❌ Aborting Task {task_id}: OpenAI API Key not found in DB Config.")
                return

            # Fetch input file for Core
            input_file = StoredFile.query.get(task.input_file_id)
            grid_out = io.BytesIO(input_file.file_data)
            files = {'file': (task.filename, grid_out, 'text/csv')}

            passport_task_id = f"{project_code}___{task_id}"

            data = {
                'task_id': passport_task_id,
                'criteria': str(criteria),
                'analysis_type': analysis_mode,
                'audit_category': audit_category,
                'api_key': api_key,
                'features': str(features) if features else "[]",
                'scoring_logic': json.dumps(scoring_logic)
            }

            logging.info(f"🚀 Sending Task {task_id} to Core...")
            response = requests.post(core_url, files=files, data=data, timeout=14400)

            if response.status_code == 200:
                logging.info(f"✅ Core Service accepted payload for Task {task_id}")
            else:
                logging.error(f"⚠️ Core Service returned error: {response.text}")
                task.status = 'error'
                task.error_message = f"Core Refused: {response.status_code}"
                db.session.commit()

        except Exception as e:
            db.session.rollback()
            logging.error(f"❌ Failed to process task: {e}")
            task = Task.query.get(task_id)
            if task:
                task.status = 'error'
                task.error_message = str(e)
                db.session.commit()


# --- ROUTES ---
@tasks_bp.route('/api/tasks/upload', methods=['POST'])
@jwt_required()
def upload_file():
    try:
        if 'file' not in request.files:
            return api_response(message='No file part provided', status=400)
        
        file = request.files['file']
        if file.filename == '':
            return api_response(message='No selected file', status=400)

        filename = file.filename
        if not filename.lower().endswith(('.csv', '.xlsx', '.xls')):
            return api_response(message='Invalid file type. Allowed: .csv, .xlsx, .xls', status=400)

        file.seek(0, os.SEEK_END)
        file_size = file.tell()
        file.seek(0) 
        if file_size == 0:
            return api_response(message='File is empty.', status=400)
        
        try:
            if filename.lower().endswith('.csv'):
                df_temp = pd.read_csv(file)
            else:
                df_temp = pd.read_excel(file)
            total_tickets_count = len(df_temp)
            file.seek(0)
        except Exception as parse_e:
            return api_response(message=f"File parsing failed: {str(parse_e)}", status=400)

        schedule_time_str = request.form.get('schedule_time') 
        analysis_type = request.form.get('reportType', 'score_only')
        audit_category = request.form.get('auditCategory', 'ticket audit') 

        ALLOWED_TYPES = {'score_only', 'full_report', 'incident_report'}
        if analysis_type not in ALLOWED_TYPES:
            return api_response(message=f"Invalid reportType. Allowed: {list(ALLOWED_TYPES)}", status=400)

        if not audit_category or len(audit_category.strip()) == 0:
             return api_response(message="Invalid or missing auditCategory.", status=400)

        claims = get_jwt()
        current_project = claims.get('project')
        username = claims.get("username", "Unknown User") 
        user_tz = request.form.get('timezone', 'UTC')
        
        if not current_project:
            return api_response(message="Project context missing in token", status=400)

        run_date = get_utc_now()
        is_scheduled = False

        if schedule_time_str:
            try:
                naive_target_date = datetime.strptime(schedule_time_str, "%Y-%m-%dT%H:%M")
                target_date = naive_target_date.replace(tzinfo=timezone.utc)
                if target_date < get_utc_now(): 
                    return api_response(message="Scheduled time cannot be in the past.", status=400)
                run_date = target_date
                is_scheduled = True
            except ValueError:
                return api_response(message="Invalid date format. Use YYYY-MM-DDTHH:MM", status=400)

        try:
            # 🟢 Save raw file to Postgres StoredFile
            new_input_file = StoredFile(
                filename=filename,
                mimetype=file.mimetype,
                file_data=file.read(),
                project_code=current_project
            )
            db.session.add(new_input_file)
            db.session.flush()

            # 🟢 Create Task Record
            new_task = Task(
                filename=filename,
                input_file_id=new_input_file.id, 
                status="scheduled" if is_scheduled else "queued",
                analysis_type=analysis_type,   
                audit_category=audit_category, 
                scheduled_for=run_date,
                created_by=username,
                user_tz=user_tz,
                total_files=total_tickets_count, # Reusing column name
                project_code=current_project
            )
            db.session.add(new_task)
            db.session.commit()
            
            task_id = new_task.id

        except Exception as db_e:
            db.session.rollback()
            logging.error(f"❌ Database Error: {db_e}")
            return api_response(message="Database write failed.", status=500)

        # Schedule Job
        try:
            real_app_object = current_app._get_current_object()
            scheduler.add_job(
                id=str(task_id),
                func=run_scheduled_job,
                trigger='date',
                run_date=run_date,
                args=[task_id, real_app_object, current_project, None], 
                replace_existing=True,
                misfire_grace_time=60
            )
            logging.info(f"✅ Job scheduled for Task {task_id} at {run_date}")

        except Exception as sched_e:
            logging.error(f"❌ Scheduler Error task {task_id}: {sched_e}")
            task = Task.query.get(task_id)
            task.status = 'error'
            task.error_message = 'Scheduling failed'
            db.session.commit()
            return api_response(message="File saved but scheduling failed.", status=500)

        return api_response(
            message="Task successfully queued",
            status=201, 
            data={
                "task_id": str(task_id),
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
    try:
        if not task_id.isdigit():
            return jsonify({'status': 'error', 'error': 'Invalid Task ID format'}), 400

        task = Task.query.get(int(task_id))
        if not task:
            return jsonify({'status': 'error', 'error': 'Task not found'}), 404

        status = task.status
        
        # 🟢 ADDED: We now send the uploaded_by username and the live progress percentage!
        response = {
            'status': status, 
            'task_id': str(task.id),
            'uploaded_by': task.created_by or "Unknown User", 
            'progress': task.progress if hasattr(task, 'progress') and task.progress else 0
        }

        if status == "complete":
            response['progress'] = 100  # Force 100% when fully complete
            response['excel_id'] = str(task.output_excel_id) if task.output_excel_id else None
            response['docx_id'] = str(task.output_docx_id) if task.output_docx_id else None
        elif status == "error":
            response['error'] = task.error_message

        return jsonify(response)

    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@tasks_bp.route('/api/tasks/', methods=['GET'])
@jwt_required()
def api_get_tasks():
    try:
        claims = get_jwt()
        project_code = claims.get('project')
        category_filter = request.args.get('category')
        
        query = Task.query.filter_by(project_code=project_code)

        if category_filter:
            search_term = f"%{category_filter}%"
            if 'incident' in category_filter.lower():
                query = query.filter(or_(
                    Task.audit_category.ilike(search_term),
                    Task.analysis_type == 'incident_report'
                ))
            elif 'ticket' in category_filter.lower():
                query = query.filter(or_(
                    Task.audit_category.ilike(search_term),
                    Task.analysis_type.in_(['score_only', 'full_report'])
                ))
            else:
                query = query.filter(Task.audit_category.ilike(search_term))

        all_tasks = query.order_by(Task.created_at.desc()).limit(100).all()
        
        tasks_list = []
        for task in all_tasks:
            category = task.audit_category
            if not category or category == 'unknown':
                a_type = task.analysis_type
                if a_type == 'incident_report':
                    category = 'incident'
                elif a_type in ['score_only', 'full_report']:
                    category = 'ticket_audit'
                else:
                    category = 'unknown'
            
            current_progress = 100 if task.status == 'complete' else (task.progress or 0)

            tasks_list.append({
                '_id': str(task.id),
                'filename': task.filename or "Batch/Unknown File",
                'status': task.status,
                'audit_category': category,
                'analysis_type': task.analysis_type,
                'uploaded_by': task.created_by or 'System/Unknown', 
                
                # 👇 ADD THIS EXACT LINE:
                'progress': current_progress,
                
                'uploaded_at': format_to_iso_z(task.created_at), 
                'created_at': format_to_iso_z(task.created_at),
                'scheduled_for': format_to_iso_z(task.scheduled_for),
                'completed_at': format_to_iso_z(task.completed_at),
                'output_excel_id': str(task.output_excel_id) if task.output_excel_id else None,
                'output_docx_id': str(task.output_docx_id) if task.output_docx_id else None
            })
            
        return jsonify(tasks_list)

    except Exception as e:
        logging.error(f"Error fetching tasks: {e}")
        return jsonify({"error": "Failed to fetch tasks"}), 500
    

@tasks_bp.route('/api/tasks/download/excel/<task_id>')
@jwt_required()
def download_excel_file(task_id):
    try:
        if not task_id.isdigit():
            return jsonify({'status': 'error', 'error': 'Invalid Task ID format'}), 400

        task = Task.query.get(int(task_id))
        if not task:
            return jsonify({"error": "Task not found."}), 404
            
        excel_file_id = task.output_excel_id
        if not excel_file_id:
            return jsonify({"error": f"Report not ready yet. Current status: {task.status}"}), 404
        
        stored_file = StoredFile.query.get(excel_file_id)
        if not stored_file:
             return jsonify({"error": "File record exists, but physical file is missing from DB."}), 404

        file_stream = io.BytesIO(stored_file.file_data)
        return send_file(
            file_stream,
            mimetype=stored_file.mimetype or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=stored_file.filename or 'data.xlsx'
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@tasks_bp.route('/api/tasks/download/docx/<task_id>')
@jwt_required()
def download_docx_file(task_id):
    try:
        if not task_id.isdigit():
            return jsonify({'status': 'error', 'error': 'Invalid Task ID format'}), 400

        task = Task.query.get(int(task_id))
        if not task:
            return jsonify({"error": "Task not found."}), 404

        docx_file_id = task.output_docx_id
        if not docx_file_id:
            if task.analysis_type == 'score_only':
                return jsonify({"error": "DOCX was not generated (Analysis Type was 'score_only')."}), 400
            return jsonify({"error": "Report is still processing."}), 404
        
        stored_file = StoredFile.query.get(docx_file_id)
        if not stored_file:
             return jsonify({"error": "File missing from database."}), 404

        file_stream = io.BytesIO(stored_file.file_data)
        return send_file(
            file_stream,
            mimetype=stored_file.mimetype or 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            as_attachment=True,
            download_name=stored_file.filename or 'report.docx'
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500    


@tasks_bp.route('/internal/save-results', methods=['POST'])
def save_audit_results():
    try:
        raw_id = request.form.get('task_id')
        results_str = request.form.get('audit_results')
        analysis_type = request.form.get('analysis_type', 'score_only') 

        if not raw_id or "___" not in raw_id: 
            return jsonify({"error": "Missing or Invalid task_id"}), 400
        
        project_code, real_task_id = raw_id.split("___", 1)
        task_id = int(real_task_id)

        if not results_str: 
            return jsonify({"error": "Missing audit_results data"}), 400

        current_category = 'incident' if analysis_type == 'incident_report' else 'ticket_audit'
        logging.info(f"💾 Receiving results for Task {task_id} ({current_category})...")

        excel_id = None
        docx_id = None

        # 🟢 RISK MITIGATION: Malformed JSON String
        try:
            audit_data = ast.literal_eval(results_str)
        except (ValueError, SyntaxError) as parse_err:
            logging.error(f"❌ Data Parsing Failed for Task {task_id}: {parse_err}")
            task = Task.query.get(task_id)
            if task:
                task.status = 'error'
                task.error_message = 'Core returned malformed data'
                db.session.commit()
            return jsonify({"error": "Malformed data format"}), 400
        
        if audit_data:
            task = Task.query.get(task_id)
            user_tz = task.user_tz if task else 'UTC'
            df_results = pd.DataFrame(audit_data)
            
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
                        df_results[col] = temp_col.dt.strftime('%Y-%m-%d')
                except Exception:
                    pass 

            logging.info(f"📊 Columns: {df_results.columns.tolist()}")

            # 🟢 Save Raw Data to Postgres JSONB
            reports_to_insert = []
            for row in audit_data:
                reports_to_insert.append(AuditReport(
                    task_id=task_id,
                    audit_category=current_category,
                    full_data=row,
                    project_code=project_code
                ))
            db.session.add_all(reports_to_insert)

            # Generate Excel
            try:
                if 'Audit Date' in df_results.columns:
                    df_results.rename(columns={'Audit Date': 'Processed At'}, inplace=True)

                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_results.to_excel(writer, index=False, sheet_name='Results')
                
                output.seek(0)
                filename = f"ticket_audit_Report_{task_id}.xlsx"
                
                new_excel = StoredFile(
                    filename=filename,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    file_data=output.read(),
                    project_code=project_code
                )
                db.session.add(new_excel)
                db.session.flush()
                excel_id = new_excel.id
                logging.info(f"✅ Excel Report Generated Locally.")
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
                        new_docx = StoredFile(
                            filename=f"ticket_audit_Report_{task_id}.docx",
                            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                            file_data=f.read(),
                            project_code=project_code
                        )
                        db.session.add(new_docx)
                        db.session.flush()
                        docx_id = new_docx.id
                    os.remove(temp_path)
                except Exception as report_err:
                    logging.exception("❌ DOCX Failed") 

        # Update Task Status
        if task:
            task.status = 'complete'
            task.audit_category = current_category
            task.output_excel_id = excel_id
            task.output_docx_id = docx_id
            task.completed_at = get_utc_now()
            db.session.commit()

            if excel_id:
                logging.info(f"Task {task_id} completed. Checking email triggers...")
                
                temp_dir = tempfile.gettempdir()
                files_to_attach = []
                
                # Fetch and write Excel file
                stored_excel = StoredFile.query.get(excel_id)
                if stored_excel:
                    temp_excel_path = os.path.join(temp_dir, stored_excel.filename)
                    with open(temp_excel_path, 'wb') as f:
                        f.write(stored_excel.file_data)
                    files_to_attach.append(temp_excel_path)
                
                # Fetch and write DOCX file (if it exists)
                if docx_id:
                    stored_docx = StoredFile.query.get(docx_id)
                    if stored_docx:
                        temp_docx_path = os.path.join(temp_dir, stored_docx.filename)
                        with open(temp_docx_path, 'wb') as f:
                            f.write(stored_docx.file_data)
                        files_to_attach.append(temp_docx_path)

                # 🟢 Call the centralized helper function
                trigger_automated_email(task, project_code, files_to_attach)
                
                # Cleanup the temp files
                for file_path in files_to_attach:
                    if os.path.exists(file_path):
                        os.remove(file_path)

        return jsonify({"message": "Saved successfully"}), 200

    except Exception as e:
        db.session.rollback()
        logging.error(f"❌ Error saving results: {e}")
        return jsonify({"error": str(e)}), 500
    

@tasks_bp.route('/api/tasks/incident/upload', methods=['POST'])
@jwt_required()
def upload_incident_report():
    try:
        if 'file' not in request.files: 
            return api_response(message='No file uploaded', status=400)
        
        file = request.files['file']
        if file.filename == '': 
            return api_response(message='No file selected', status=400)
        
        claims = get_jwt()
        current_project = claims.get('project')
        user_tz = request.form.get('timezone', 'UTC')
        username = claims.get("username", "Unknown User")

        file_pos = file.tell()
        header = file.read(8)
        file.seek(file_pos)
        OLE_MAGIC = b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'
        if file.filename.endswith('.xlsx') and header == OLE_MAGIC:
            return api_response(message='Cannot read encrypted file.', status=400)

        try:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)
            
            df.columns = df.columns.astype(str).str.strip()
            current_headers = list(df.columns)
            
            INCIDENT_REQUIRED = [
                'Ticket Id', 'Created Time', 'Closed Time', 'Resolved Time', 
                'Priority', 'Status', 'Type', 'Group', 'Agent', 
                'Category', 'Requester Name', 'Item', 'Resolution Time (in Hrs)','Description'
            ]
            
            missing = [req for req in INCIDENT_REQUIRED if req not in current_headers]
            
            if missing:
                logging.info(f"⚠️ Incident Upload missing {len(missing)} columns. Asking Core AI to map...")
                
                config_doc = ApiConfig.query.filter_by(name="openai_api_key", project_code=current_project).first()
                api_key = config_doc.key if config_doc else None
                
                if not api_key:
                    return api_response(message="System configuration error (Missing API Key)", status=500)

                core_url = current_app.config['CORE_SERVICE_URL'].rstrip('/') + "/internal/ai-map-custom"
                
                payload = {
                    "headers": current_headers,
                    "target_fields": INCIDENT_REQUIRED, 
                    "api_key": api_key  
                }
                
                try:
                    response = requests.post(core_url, json=payload, timeout=30)
                    if response.status_code == 200:
                        mapping = response.json().get('mapping', {})
                        rename_dict = {v: k for k, v in mapping.items() if v}
                        df.rename(columns=rename_dict, inplace=True)
                        logging.info(f"✅ AI Mapping Applied.")
                    else:
                        logging.error(f"❌ Core AI Mapping failed: {response.text}")
                except Exception as e:
                    logging.error(f"❌ Connection to Core failed: {e}")

        except Exception as e:
            return api_response(message=f"File parsing failed: {str(e)}", status=400)

        output_buffer = io.BytesIO()
        if file.filename.endswith('.csv'):
            df.to_csv(output_buffer, index=False)
        else:
            with pd.ExcelWriter(output_buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
        
        output_buffer.seek(0)
        
        try:
            new_file = StoredFile(
                filename=file.filename,
                mimetype=file.mimetype,
                file_data=output_buffer.read(),
                project_code=current_project
            )
            db.session.add(new_file)
            db.session.flush()

            # 5. Hardcoded Active Features
            feats = [str(i) for i in range(1, 18)]


            # 6. Create Task
            new_task = Task(
                filename=file.filename,
                input_file_id=new_file.id,
                status="queued",
                analysis_type="incident_report",
                user_tz=user_tz,
                total_files=len(df),
                project_code=current_project,
                created_by=username
            )
            db.session.add(new_task)
            db.session.commit()
            task_id = new_task.id

        except Exception as e:
            db.session.rollback()
            return api_response(message="Database error saving incident.", status=500)

        # 7. Schedule Job
        try:
            real_app = current_app._get_current_object()
            scheduler.add_job(
                id=str(task_id),
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
            task = Task.query.get(task_id)
            task.status = 'error'
            task.error_message = 'Scheduling failed'
            db.session.commit()
            return api_response(message="File saved but scheduling failed.", status=500)

        return api_response(
            message="Incident Report Queued", 
            status=201, 
            data={'task_id': str(task_id), 'status': 'queued'}
        )

    except Exception as e:
        logging.error(f"Incident Upload Error: {e}")
        return api_response(message=f"Internal Server Error: {str(e)}", status=500)
    

# 🟢 NEW ENDPOINT: Receive PII Logs from Core Service
@tasks_bp.route('/internal/save-pii-logs', methods=['POST'])
def save_pii_logs():
    try:
        log_entry = request.get_json()
        if not log_entry:
            return jsonify({"error": "No data provided"}), 400

        raw_task_id = log_entry.get('task_id')
        if not raw_task_id:
            return jsonify({"error": "Missing task_id"}), 400

        project_code = "default"
        real_task_id = raw_task_id
        
        if "___" in raw_task_id:
            try:
                project_code, real_task_id = raw_task_id.split("___", 1)
                log_entry['task_id'] = real_task_id
            except ValueError:
                return jsonify({"error": "Invalid ID format"}), 400

        # 🟢 1. FETCH USER TIMEZONE FROM THE MASTER TASK
        user_tz_str = 'UTC' # Default fallback
        if real_task_id.isdigit():
            task = Task.query.get(int(real_task_id))
            if task and task.user_tz:
                user_tz_str = task.user_tz
                
        # 🟢 2. CONVERT CURRENT TIME TO USER'S LOCAL TIMEZONE
        target_tz = pytz.timezone(user_tz_str)
        # Get UTC time, convert to target timezone, and strip tzinfo so Postgres saves the exact local numbers
        local_time = datetime.now(timezone.utc).astimezone(target_tz).replace(tzinfo=None)

        # 🟢 3. SAVE TO POSTGRES WITH THE LOCAL TIMESTAMP
        new_log = PiiLog(
            task_id=log_entry.get('task_id'),
            timestamp=local_time,  # Inject the local time here
            status=log_entry.get('status'),
            pii_found=log_entry.get('pii_found', False),
            detection_stats=log_entry.get('detection_stats', {}),
            processed_data_preview=log_entry.get('processed_data_preview', {}),
            project_code=project_code
        )
        db.session.add(new_log)
        db.session.commit()

        return jsonify({"status": "success", "message": "PII Log Saved in Local Time"}), 200

    except Exception as e:
        db.session.rollback()
        logging.error(f"❌ Failed to save PII Log: {e}")
        return jsonify({"error": str(e)}), 500

@tasks_bp.route('/api/pii-logs', methods=['GET'])
@jwt_required()
def get_pii_logs():
    try:
        claims = get_jwt()
        project_code = claims.get('project')

        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        task_id = request.args.get('task_id')
        limit = int(request.args.get('limit', 50))

        # Query builder
        query = PiiLog.query.filter_by(project_code=project_code)

        if start_date_str:
            query = query.filter(PiiLog.timestamp >= f"{start_date_str}T00:00:00")
        if end_date_str:
            query = query.filter(PiiLog.timestamp <= f"{end_date_str}T23:59:59")
        if task_id:
            query = query.filter(PiiLog.task_id == task_id)

        # Fetch with limit
        logs_records = query.order_by(PiiLog.timestamp.desc()).limit(limit).all()

        logs = []
        for doc in logs_records:
            logs.append({
                "id": str(doc.id),
                "task_id": doc.task_id,
                "timestamp": format_to_iso_z(doc.timestamp),
                "status": doc.status,
                "pii_found": doc.pii_found,
                "stats": doc.detection_stats or {},
                "data": doc.processed_data_preview or {} 
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
    

def generate_and_send_summary(project_code, frequency="daily", recipient_list=None):
    """Calculates metrics dynamically, separated by Call, Ticket, and Incident."""
    logging.info(f"📊 Generating {frequency.upper()} Summary Report for Project: {project_code}")
    
    if not recipient_list:
        logging.warning(f"⚠️ No recipients provided. Summary skipped.")
        return

    # 🟢 ROLLING WINDOW LOGIC (Exactly preceding 24 hours/7 days/30 days)
    end_time = get_utc_now() # The exact millisecond the scheduled trigger fires
    
    # 🟢 DYNAMIC TIME WINDOW BASED ON FREQUENCY
    if frequency == "weekly":
        start_time = end_time - timedelta(days=7)
        title_prefix = "Weekly"
        time_text = "for the past 7 days"
    elif frequency == "monthly":
        start_time = end_time - timedelta(days=30)
        title_prefix = "Monthly"
        time_text = "for the past 30 days"
    else:
        start_time = end_time - timedelta(days=1)
        title_prefix = "Daily"
        time_text = "for the past 24 hours"

    # Query the database using the dynamic rolling bounds
    tasks = Task.query.filter(
        Task.project_code == project_code,
        Task.status == 'complete',
        Task.completed_at >= start_time,
        Task.completed_at <= end_time
    ).all()

    # 🟢 SEPARATE TRACKERS FOR EACH CATEGORY
    metrics = {
        'call': {'total': 0, 'excellent': 0, 'good': 0, 'fair': 0, 'poor': 0},
        'ticket': {'total': 0, 'excellent': 0, 'good': 0, 'fair': 0, 'poor': 0},
        'incident': {'total': 0, 'excellent': 0, 'good': 0, 'fair': 0, 'poor': 0} # Incidents don't use AI scores, but we track volume
    }

    for t in tasks:
        # Determine Category
        if t.analysis_type == 'incident_report':
            cat = 'incident'
            # Incidents don't have AI rows, so we use the total files/rows processed
            metrics[cat]['total'] += (t.total_files or 0)
            continue # Skip scoring for incidents
            
        elif t.audit_category == 'call audit':
            cat = 'call'
            records = CallAuditResult.query.filter_by(task_id=t.id).all()
        else:
            cat = 'ticket'
            records = AuditReport.query.filter_by(task_id=t.id).all()

        # Tally Scores for Calls and Tickets
        for r in records:
            metrics[cat]['total'] += 1
            data = r.full_data or {}
            raw_score = data.get('Overall Score') or data.get('score') or 0
            
            try:
                score = float(raw_score)
            except (ValueError, TypeError):
                score = 0.0

            if score >= 90: metrics[cat]['excellent'] += 1
            elif score >= 80: metrics[cat]['good'] += 1
            elif score >= 60: metrics[cat]['fair'] += 1
            else: metrics[cat]['poor'] += 1

    # 🟢 HTML TEMPLATE GENERATOR FOR EACH SECTION
    def build_html_section(title, data, show_scores=True):
        if data['total'] == 0:
            return "" # Hide empty sections
            
        html = f"""
        <div style="margin-bottom: 30px;">
            <h3 style="color: #2C3E50; border-bottom: 2px solid #ddd; padding-bottom: 5px;">{title}</h3>
            <p style="font-size: 16px;"><strong>Total Volume Processed:</strong> {data['total']}</p>
        """
        
        if show_scores:
            html += f"""
            <table style="border-collapse: collapse; width: 100%; max-width: 500px; margin-top: 10px;">
                <tr style="background-color: #2980B9; color: white;">
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Performance Tier</th>
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: center;">Count</th>
                </tr>
                <tr><td style="padding: 10px; border: 1px solid #ddd;">🟢 Excellent (90 - 100)</td><td style="padding: 10px; border: 1px solid #ddd; text-align: center;"><b>{data['excellent']}</b></td></tr>
                <tr><td style="padding: 10px; border: 1px solid #ddd;">🟡 Good (80 - 89)</td><td style="padding: 10px; border: 1px solid #ddd; text-align: center;"><b>{data['good']}</b></td></tr>
                <tr><td style="padding: 10px; border: 1px solid #ddd;">🟠 Fair (60 - 79)</td><td style="padding: 10px; border: 1px solid #ddd; text-align: center;"><b>{data['fair']}</b></td></tr>
                <tr><td style="padding: 10px; border: 1px solid #ddd;">🔴 Needs Retrain (< 60)</td><td style="padding: 10px; border: 1px solid #ddd; text-align: center;"><b>{data['poor']}</b></td></tr>
            </table>
            """
        html += "</div>"
        return html

    # Build the full HTML body
    call_html = build_html_section("📞 Call Audit Performance", metrics['call'])
    ticket_html = build_html_section("🎫 Ticket Audit Performance", metrics['ticket'])
    incident_html = build_html_section("⚠️ Incident Analysis Volume", metrics['incident'], show_scores=False) # No AI scores for incident data
    
    # If absolutely nothing was processed
    if not (call_html or ticket_html or incident_html):
        body_content = "<p>No audits or incidents were processed during this time period.</p>"
    else:
        body_content = call_html + ticket_html + incident_html

    final_html = f"""
    <html>
        <body style="font-family: Arial, sans-serif; color: #333; padding: 20px;">
            <h2 style="color: #2C3E50;">{title_prefix} Quality Summary - {project_code.upper()}</h2>
            <p style="color: #555; font-size: 16px;">Performance snapshot <strong>{time_text}</strong>.</p>
            {body_content}
        </body>
    </html>
    """

    # Dispatch via upgraded email service
    success = send_audit_email(
        recipient_email=recipient_list,
        subject=f"{title_prefix} Quality Summary - {project_code.upper()}",
        body_text="Your email client does not support HTML.",
        body_html=final_html
    )
    
    # 🟢 EXPLICIT MAIL SENT LOG
    if success:
        logging.info(f"✅ MAIL SENT LOG: {title_prefix} Summary successfully delivered to {len(recipient_list)} recipients: {recipient_list}")
    else:
        logging.error(f"❌ MAIL FAILED LOG: Could not dispatch {title_prefix} Summary to {recipient_list}")

def evaluate_summary_triggers(app):
    """CRON JOB: Runs every minute and evaluates Daily/Weekly/Monthly triggers."""
    with app.app_context():
        # 🟢 GET ABSOLUTE UTC TIME FIRST
        utc_now = datetime.now(timezone.utc) 

        configs = ApiConfig.query.filter_by(name="summary_notification_settings").all()
        
        for config in configs:
            if config.key == "false" or not config.tools: 
                continue 
                
            settings = config.tools
            if not settings.get("summaryEnabled"): 
                continue

            triggers = settings.get("triggers", [])
            
            for t in triggers:
                if not t.get("status"): 
                    continue 

                # =========================================================
                # 🟢 TIMEZONE MAGIC HAPPENS HERE
                # =========================================================
                tz_str = t.get("timezone", "UTC") # Fallback to UTC if missing
                try:
                    target_tz = pytz.timezone(tz_str)
                    local_time = utc_now.astimezone(target_tz)
                except pytz.UnknownTimeZoneError:
                    local_time = utc_now # Fallback if timezone string is invalid

                # Now get the time, day, and date IN THAT SPECIFIC TIMEZONE
                current_time_str = local_time.strftime("%H:%M")
                current_day = local_time.strftime("%A")
                current_date = str(local_time.day)
                # =========================================================

                # Now compare the trigger's target time to the LOCALIZED time!
                if t.get("time") != current_time_str: 
                    continue 

                freq = str(t.get("frequency")).lower()
                trigger_emails = t.get("emails", [])
                
                if not trigger_emails:
                    continue
                
                # Fire the generator!
                if freq == "daily":
                    generate_and_send_summary(config.project_code, "daily", trigger_emails)
                elif freq == "weekly" and str(t.get("dayOfWeek")).lower() == current_day.lower():
                    generate_and_send_summary(config.project_code, "weekly", trigger_emails)
                elif freq == "monthly" and str(t.get("dateOfMonth")) == current_date:
                    generate_and_send_summary(config.project_code, "monthly", trigger_emails)


# 🟢 NEW ENDPOINT: Core Service sends live progress here (e.g., row 30 of 100 = 30%)
@tasks_bp.route('/internal/update-progress', methods=['POST'])
def update_task_progress():
    try:
        data = request.get_json()
        raw_id = data.get('task_id')
        progress = data.get('progress', 0)

        if not raw_id:
            return jsonify({"error": "Missing task_id"}), 400

        # Handle the passport task ID format (ProjectCode___TaskID)
        if "___" in str(raw_id):
            _, real_task_id = str(raw_id).split("___", 1)
        else:
            real_task_id = raw_id

        task = Task.query.get(int(real_task_id))
        if not task:
            return jsonify({"error": "Task not found"}), 404

        # Update the live progress
        task.progress = int(progress)
        db.session.commit()

        return jsonify({"status": "success", "progress": task.progress}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500