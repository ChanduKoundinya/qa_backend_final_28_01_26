import json
import logging
import re
import requests
import threading
import io
import concurrent.futures
from datetime import datetime, timezone

from flask import request, jsonify, current_app, g
from flask_jwt_extended import jwt_required, get_jwt
from sqlalchemy.orm.attributes import flag_modified
import os
import tempfile
from app.utils.email_service import send_audit_email
from . import call_audit_bp
from app.engine.call_report import CallReportEngine
from app.models import User
# 🟢 POSTGRESQL MODELS IMPORT
from app.models import db, Task, Criterion, ApiConfig, CallAuditResult, StoredFile

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================
def get_utc_now():
    """Returns current time in UTC, timezone-aware."""
    return datetime.now(timezone.utc)

def parse_filename_metadata(filename):
    """
    Extracts Agent Name and Date from format:
    "[Aswin Srinivasan]_101-+12106017188_20251229082336(2830).wav"
    """
    agent_name = "Unknown"
    audit_date = None

    name_match = re.search(r"^\[([^\]]+)\]", filename)
    if name_match:
        agent_name = name_match.group(1)

    date_match = re.search(r"(\d{8})\d{6}", filename)
    if date_match:
        raw_date = date_match.group(1) 
        try:
            dt_obj = datetime.strptime(raw_date, "%Y%m%d")
            audit_date = dt_obj.strftime("%Y-%m-%d")
        except ValueError:
            pass 

    return agent_name, audit_date

# ==========================================
# 2. BACKGROUND WORKER (Parallel Version)
# ==========================================
def background_worker(app, project_code, file_data_list, main_task_id, api_key, core_url, criteria_list):
    """
    Runs in a background thread and dispatches files to the Core Service concurrently.
    """
    with app.app_context():
        logging.info(f"🧵 Parallel Worker started for Project: {project_code} | Task: {main_task_id}")

    def process_single_file(file_item):
        with app.app_context():
            filename = file_item['filename']
            file_bytes = file_item['content'] 
            mimetype = file_item['mimetype']
            
            safe_key = filename.replace('.', '_')
            composite_id = f"{project_code}___{main_task_id}___{filename}"

            try:
                files = {'audio_file': (filename, io.BytesIO(file_bytes), mimetype)}
                data = {
                    'task_id': composite_id,  
                    'api_key': api_key,
                    'criteria': json.dumps(criteria_list),
                    'scoring_text': "" 
                }

                logging.info(f"   📤 Sending {filename} to Core...")
                response = requests.post(core_url, files=files, data=data, timeout=600)

                # 🟢 PostgreSQL JSONB Update
                # We fetch the task, update the dictionary, and flag it as modified
                task = Task.query.get(main_task_id)
                if not task:
                    return

                if response.status_code == 200:
                    logging.info(f"   ✅ Core accepted {filename}")
                    task.files_tracker[safe_key]['status'] = 'processing'
                else:
                    logging.error(f"   ⚠️ Core Rejected {filename}: {response.text}")
                    task.files_tracker[safe_key]['status'] = 'error'

                # Tell SQLAlchemy the JSONB column changed
                flag_modified(task, 'files_tracker')
                db.session.commit()

            except Exception as e:
                logging.error(f"   ❌ Thread Error on {filename}: {e}")
                task = Task.query.get(main_task_id)
                if task:
                    task.files_tracker[safe_key]['status'] = 'error'
                    flag_modified(task, 'files_tracker')
                    db.session.commit()

    # Execute in parallel safely
    max_threads = min(10, len(file_data_list)) 
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        executor.map(process_single_file, file_data_list)

    with app.app_context():
        logging.info(f"🏁 All parallel upload threads finished dispatching.")


# ==========================================
# 3. UPLOAD ROUTE
# ==========================================
@call_audit_bp.route('/api/call/audit', methods=['POST'])
@jwt_required()
def upload_call_audit():
    try:
        # 1. Get Context
        claims = get_jwt()
        project_code = claims.get('project')
        username = claims.get("username", "Unknown User")
        user_tz = request.form.get('timezone', 'UTC')

        if not project_code:
             return jsonify({"error": "Project context missing in token"}), 400

        # 2. Capture Files
        files = []
        if 'audio_files' in request.files:
            files.extend(request.files.getlist('audio_files'))
        if 'audio_file' in request.files:
            files.extend(request.files.getlist('audio_file'))
            
        files = [f for f in files if f.filename]
        
        if not files:
            return jsonify({"error": "No audio files provided"}), 400

        logging.info(f"📥 [Bulk Upload] Received {len(files)} audio files.")

        # 3. Prepare Common Data (SQLAlchemy Fetch)
        criteria_records = Criterion.query.filter_by(is_active=True, type="call audit", project_code=project_code).all()
        criteria_list = [{'name': c.name, 'weight': c.weight, 'description': c.description} for c in criteria_records]
        
        if not criteria_list:
            criteria_list = [{"name": "Opening", "weight": 1}, {"name": "Closing", "weight": 1}]

        config_doc = ApiConfig.query.filter_by(name="openai_api_key", project_code=project_code).first()
        api_key = config_doc.key if config_doc else None
        
        if not api_key:
            return jsonify({"error": "OpenAI Key not configured"}), 500

        core_url = current_app.config.get('CORE_SERVICE_URL', "http://127.0.0.1:6000") + "/internal/process-call"

        # 4. Create Master Task Data
        files_tracker = {}
        file_data_list = []

        for f in files:
            safe_key = f.filename.replace('.', '_')
            files_tracker[safe_key] = {"status": "queued", "error": None}
            
            f.stream.seek(0)
            content = f.read()
            file_data_list.append({
                "filename": f.filename,
                "content": content,
                "mimetype": f.mimetype
            })

        batch_name = f"{files[0].filename} + {len(files)-1} others" if len(files) > 1 else files[0].filename

        # 🟢 CREATE POSTGRES TASK
        new_task = Task(
            filename=batch_name,
            status='processing',
            files_tracker=files_tracker,
            total_files=len(files),
            completed_count=0,
            audit_category='call audit',
            created_by=username,
            user_tz=user_tz,
            project_code=project_code
        )
        db.session.add(new_task)
        db.session.commit()
        
        main_task_id = new_task.id # Integer ID in Postgres!

        logging.info(f"🆔 Created Postgres Master Task: {main_task_id}")

        # 5. Start Background Thread
        app_obj = current_app._get_current_object()
        thread = threading.Thread(
            target=background_worker,
            args=(app_obj, project_code, file_data_list, main_task_id, api_key, core_url, criteria_list)
        )
        thread.start()

        return jsonify({
            "message": "Processing started in background",
            "task_id": str(main_task_id)
        }), 200

    except Exception as e:
        logging.error(f"❌ Critical Upload Error: {e}")
        return jsonify({"error": str(e)}), 500


# ==========================================
# 4. SAVE RESULTS ROUTE
# ==========================================
@call_audit_bp.route('/internal/save-call-results', methods=['POST'])
def save_call_results():
    try:
        composite_id = request.form.get('task_id')
        audit_results_str = request.form.get('audit_results')
        
        if not composite_id or "___" not in composite_id:
            return jsonify({"error": "Invalid Task ID format"}), 400

        project_code, main_task_id_str, filename = composite_id.split("___", 2)
        main_task_id = int(main_task_id_str) # Postgres uses Int

        audit_data = json.loads(audit_results_str)
        result_item = audit_data[0] if isinstance(audit_data, list) else audit_data
        result_item['filename'] = filename 

        agent_name, agent_date = parse_filename_metadata(filename)

        # 🟢 SAVE TO POSTGRES CALL RESULTS TABLE
        new_result = CallAuditResult(
            task_id=main_task_id,
            filename=filename,
            agent_name=agent_name,
            agent_audit_date=agent_date,
            full_data=result_item,
            project_code=project_code
        )
        db.session.add(new_result)

        safe_tracker_key = filename.replace('.', '_')

        # 🟢 LOCK ROW FOR CONCURRENT UPDATE
        # with_for_update() prevents thread race conditions on completed_count
        task = Task.query.with_for_update().get(main_task_id)
        
        if task:
            task.files_tracker[safe_tracker_key]['status'] = 'complete'
            task.completed_count += 1
            flag_modified(task, 'files_tracker')
            db.session.commit()
        else:
            db.session.rollback()
            return jsonify({"error": "Task not found"}), 404

        total = task.total_files or 0
        done = task.completed_count or 0

        # Check completion
        if done >= total:
            logging.info(f"🏁 Batch {main_task_id} Complete. Generating Report...")
            user_tz = task.user_tz or 'UTC'
            
            # Fetch the raw results from Postgres
            raw_db_records = CallAuditResult.query.filter_by(task_id=main_task_id).all()
            
            processed_for_engine = []
            for doc in raw_db_records:
                flat_doc = {
                    "filename": doc.filename,
                    "agent_name": doc.agent_name,
                    "agent_audit_date": doc.agent_audit_date,
                    "created_at": doc.created_at,
                    "full_data": doc.full_data
                }
                
                ai_data = doc.full_data or {}
                breakdown = ai_data.get("Breakdown", [])
                
                if isinstance(breakdown, list):
                    for item in breakdown:
                        param = item.get("Parameter")
                        if param:
                            # 🟢 FIX: Normalize the parameter name to Title Case
                            # This turns "Politeness to customer" -> "Politeness To Customer"
                            normalized_param = param.strip().title()
                            flat_doc[normalized_param] = item
                                        
                processed_for_engine.append(flat_doc)
                
                processed_for_engine.append(flat_doc)

            engine = CallReportEngine()
            excel_output = engine.generate_excel(processed_for_engine, user_tz)
            
            if excel_output:
                filename_report = f"call_audit_Report_{main_task_id}.xlsx"
                
                # 🟢 GRIDFS REPLACEMENT: Save Excel to StoredFile (BYTEA)
                # We read the output buffer into raw bytes
                excel_output.seek(0)
                new_file = StoredFile(
                    filename=filename_report,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    file_data=excel_output.read(),
                    project_code=project_code
                )
                db.session.add(new_file)
                db.session.flush() # Get ID without committing transaction
                
                # Update task with File ID
                task.status = 'complete'
                task.output_excel_id = new_file.id
                task.completed_at = get_utc_now()
                db.session.commit()

            engine = CallReportEngine()
            excel_output = engine.generate_excel(processed_for_engine, user_tz)
            
            if excel_output:
                filename_report = f"call_audit_Report_{main_task_id}.xlsx"
                
                # 🟢 GRIDFS REPLACEMENT: Save Excel to StoredFile (BYTEA)
                excel_output.seek(0)
                file_bytes = excel_output.read() # Read once into a variable
                
                new_file = StoredFile(
                    filename=filename_report,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    file_data=file_bytes,
                    project_code=project_code
                )
                db.session.add(new_file)
                db.session.flush() # Get ID without committing transaction
                
                # Update task with File ID
                task.status = 'complete'
                task.output_excel_id = new_file.id
                task.completed_at = get_utc_now()
                db.session.commit()

                # ==========================================
                # 🟢 NEW EMAIL TRIGGER LOGIC
                # ==========================================
                logging.info(f"Task {main_task_id} saved to DB. Triggering email notification...")

                # Create a temporary file on the VM's disk to attach to the email
                temp_dir = tempfile.gettempdir()
                temp_file_path = os.path.join(temp_dir, filename_report)
                
                with open(temp_file_path, 'wb') as f:
                    f.write(file_bytes)

                uploader = User.query.filter_by(username=task.created_by).first()

                if uploader and uploader.email:
                    recipient = uploader.email
                else:
                    recipient = "subhashini54860@gmail.com" # Fallback just in case

                subject = f"Call Audit Completed - Task #{main_task_id}"
                body = (
                    f"Hello,\n\n"
                    f"The Call Audit for Task #{main_task_id} has been successfully completed.\n"
                    f"Please find the attached audit report.\n\n"
                    f"Best regards,\nQA Bot System"
                )

                # Send the email
                email_sent = send_audit_email(
                    recipient_email=recipient,
                    subject=subject,
                    body_text=body,
                    file_path=temp_file_path 
                )
                
                # Clean up the temporary file from the server so it doesn't waste disk space
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)

                if email_sent:
                    logging.info(f"📧 Email successfully sent to {recipient}")
                else:
                    logging.error(f"❌ Failed to send email for Task {main_task_id}")
                # ==========================================

        return jsonify({"status": "success"}), 200

    except Exception as e:
        db.session.rollback()
        logging.error(f"Error saving results: {e}")
        return jsonify({"error": str(e)}), 500