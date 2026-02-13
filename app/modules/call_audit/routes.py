import json
import logging
import re
import requests
import threading
import io
import os
from datetime import datetime, timezone
from bson.objectid import ObjectId
from flask import request, jsonify, current_app, g
from . import call_audit_bp
from app.extensions import mongo
from app.engine.call_report import CallReportEngine
from flask_jwt_extended import jwt_required, get_jwt

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

    # 1. Extract Name: Content inside [ ] at the start
    name_match = re.search(r"^\[([^\]]+)\]", filename)
    if name_match:
        agent_name = name_match.group(1)

    # 2. Extract Date: Look for YYYYMMDD pattern
    date_match = re.search(r"(\d{8})\d{6}", filename)
    if date_match:
        raw_date = date_match.group(1) # e.g., "20251229"
        try:
            dt_obj = datetime.strptime(raw_date, "%Y%m%d")
            audit_date = dt_obj.strftime("%Y-%m-%d")
        except ValueError:
            pass 

    return agent_name, audit_date

# ==========================================
# 🟢 2. BACKGROUND WORKER (The Fix)
# ==========================================
def background_worker(app, project_code, file_data_list, main_task_id, api_key, core_url, criteria_list):
    """
    Runs in a background thread.
    We MUST pass the 'app' object and 'project_code' explicitly.
    """
    # 1. Manually activate the Flask App Context
    with app.app_context():
        # 2. Manually Set the Tenant Context
        g.current_tenant = project_code
        
        logging.info(f"🧵 Worker started for Project: {project_code} | Task: {main_task_id}")

        for file_item in file_data_list:
            filename = file_item['filename']
            file_bytes = file_item['content'] # Binary data from memory
            mimetype = file_item['mimetype']
            
            # Sanitize filename for DB Key
            safe_key = filename.replace('.', '_')
            
            # 🟢 CONSTRUCT THE PASSPORT ID
            # This ensures the Core Service sends back the correct ID for PII logs
            composite_id = f"{project_code}___{main_task_id}___{filename}"

            try:
                # Prepare payload
                files = {'audio_file': (filename, io.BytesIO(file_bytes), mimetype)}
                data = {
                    'task_id': composite_id,  # <--- Sending the Passport
                    'api_key': api_key,
                    'criteria': json.dumps(criteria_list),
                    'scoring_text': "" 
                }

                # Send to Core
                logging.info(f"   📤 Sending {filename} to Core...")
                response = requests.post(core_url, files=files, data=data, timeout=600)

                if response.status_code == 200:
                    logging.info(f"   ✅ Core accepted {filename}")
                    # Update Tracker to Processing
                    mongo.db.tasks.update_one(
                        {'_id': main_task_id},
                        {'$set': {f'files_tracker.{safe_key}.status': 'processing'}}
                    )
                else:
                    logging.error(f"   ⚠️ Core Rejected {filename}: {response.text}")
                    mongo.db.tasks.update_one(
                        {'_id': main_task_id},
                        {'$set': {f'files_tracker.{safe_key}.status': 'error'}}
                    )

            except Exception as e:
                logging.error(f"   ❌ Thread Error on {filename}: {e}")
                # Update Tracker to Error
                mongo.db.tasks.update_one(
                    {'_id': main_task_id},
                    {'$set': {f'files_tracker.{safe_key}.status': 'error'}}
                )

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

        if not project_code:
             return jsonify({"error": "Project context missing in token"}), 400

        # Lock Context for Main Thread
        g.current_tenant = project_code 

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

        # 3. Prepare Common Data
        criteria_list = list(mongo.db.criteria.find(
            {"is_active": True, "type": "call audit"}, 
            {'_id': 0, 'name': 1, 'weight': 1, 'description': 1}
        ))
        
        if not criteria_list:
            # Fallback defaults if DB is empty
            criteria_list = [{"name": "Opening", "weight": 1}, {"name": "Closing", "weight": 1}]

        config_doc = mongo.db.api_config.find_one({"name": "openai_api_key"})
        api_key = config_doc.get("key") if config_doc else None
        
        if not api_key:
            return jsonify({"error": "OpenAI Key not configured"}), 500

        core_url = current_app.config.get('CORE_SERVICE_URL', "http://127.0.0.1:6000") + "/internal/process-call"

        # 4. Create Master Task
        files_tracker = {}
        # We need to read files into memory to pass to thread (Flask file objects can't pass)
        file_data_list = []

        for f in files:
            safe_key = f.filename.replace('.', '_')
            files_tracker[safe_key] = {"status": "queued", "error": None}
            
            # Read content to memory (Fixes thread context issues)
            f.stream.seek(0)
            content = f.read()
            file_data_list.append({
                "filename": f.filename,
                "content": content,
                "mimetype": f.mimetype
            })

        batch_name = f"{files[0].filename} + {len(files)-1} others" if len(files) > 1 else files[0].filename

        main_task_id = mongo.db.tasks.insert_one({
            'type': 'call_audit_batch',
            'status': 'processing',
            'filename': batch_name, 
            'files_tracker': files_tracker, 
            'total_files': len(files),
            'completed_count': 0,
            'audit_category': 'call audit',
            'created_at': get_utc_now(),
            'created_by': username,
            'output_excel_id': None
        }).inserted_id

        logging.info(f"🆔 Created Master Task: {main_task_id}")

        # 5. Start Background Thread
        # We pass 'current_app._get_current_object()' so the thread can access config
        app_obj = current_app._get_current_object()
        
        thread = threading.Thread(
            target=background_worker,
            args=(
                app_obj, 
                project_code, 
                file_data_list, 
                main_task_id, 
                api_key, 
                core_url, 
                criteria_list
            )
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

        # Split and Set Context
        project_code, main_task_id, filename = composite_id.split("___", 2)
        
        # 🟢 Activate DB (Crucial for Multi-tenancy)
        g.current_tenant = project_code

        audit_data = json.loads(audit_results_str)
        result_item = audit_data[0] if isinstance(audit_data, list) else audit_data
        result_item['filename'] = filename 

        # 🟢 METADATA EXTRACTION (Restored)
        agent_name, agent_date = parse_filename_metadata(filename)

        mongo.db.call_audit_results.insert_one({
             "task_id": main_task_id, 
            "filename": filename,
            "agent_name": agent_name,       # <--- Saved
            "agent_audit_date": agent_date, # <--- Saved
            "full_data": result_item,
            "created_at": get_utc_now()
        })

        safe_tracker_key = filename.replace('.', '_')

        updated_task = mongo.db.tasks.find_one_and_update(
            {'_id': ObjectId(main_task_id)},
            {
                # Use the SAFE key here
                '$set': {f'files_tracker.{safe_tracker_key}.status': 'complete'},
                '$inc': {'completed_count': 1}
            },
            return_document=True
        )

        # Check completion logic (Generate Excel if done)
        total = updated_task.get('total_files', 0)
        done = updated_task.get('completed_count', 0)

        if done >= total:
            logging.info(f"🏁 Batch {main_task_id} Complete. Generating Report...")
            all_results = list(mongo.db.call_audit_results.find({'task_id': main_task_id}))
            engine = CallReportEngine()
            excel_output = engine.generate_excel(all_results)
            
            if excel_output:
                filename_report = f"Master_Report_{main_task_id}.xlsx"
                excel_id = current_app.fs.put(
                    excel_output, 
                    filename=filename_report,
                    content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                mongo.db.tasks.update_one(
                    {'_id': ObjectId(main_task_id)},
                    {'$set': {
                        'status': 'complete', 
                        'output_excel_id': excel_id,
                        'completed_at': get_utc_now()
                    }}
                )

        return jsonify({"status": "success"}), 200

    except Exception as e:
        logging.error(f"Error saving results: {e}")
        return jsonify({"error": str(e)}), 500
