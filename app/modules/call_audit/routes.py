import json
import logging
import requests
import threading
from datetime import datetime
from bson.objectid import ObjectId
from flask import request, jsonify, current_app, send_file
from . import call_audit_bp
from app.extensions import mongo
from app.engine.call_report import CallReportEngine
import io
import pandas as pd
from flask_jwt_extended import jwt_required

# ==========================================
# 1. BACKGROUND WORKER LOGIC
# ==========================================

def run_audit_in_background(app, files_data, main_task_id_str, criteria_list, api_key, core_url):
    """
    Background worker that talks to the Core Service.
    """
    # 🟢 CRITICAL: Use the app context to access mongo and config
    with app.app_context():
        logging.info(f"🚀 Thread started for Task ID: {main_task_id_str}")
        
        for file_name, file_content, mimetype in files_data:
            safe_key = file_name.replace('.', '_')
            composite_id = f"{main_task_id_str}___{file_name}"
            
            try:
                logging.info(f"🔄 Background Processing File: {file_name}")

                # Prepare the file-like object from stored bytes
                files_payload = {
                    'audio_file': (file_name, io.BytesIO(file_content), mimetype)
                }
                data_payload = {
                    'task_id': composite_id,
                    'api_key': api_key,
                    'criteria': json.dumps(criteria_list)
                }

                # 🟢 Call the Core Service with a long timeout
                response = requests.post(core_url, files=files_payload, data=data_payload, timeout=14400)

                if response.status_code == 200:
                    logging.info(f"   ✅ Successfully sent '{file_name}' to Core.")
                    mongo.db.tasks.update_one(
                        {'_id': ObjectId(main_task_id_str)},
                        {'$set': {f'files_tracker.{safe_key}.status': 'processing'}}
                    )
                else:
                    logging.error(f"   ⚠️ Core Rejected '{file_name}' with status {response.status_code}: {response.text}")
                    mongo.db.tasks.update_one(
                        {'_id': ObjectId(main_task_id_str)},
                        {'$set': {f'files_tracker.{safe_key}.status': 'error', f'files_tracker.{safe_key}.error': 'Core rejection'}}
                    )

            except Exception as e:
                logging.error(f"   ❌ Thread Exception on '{file_name}': {str(e)}")
                mongo.db.tasks.update_one(
                    {'_id': ObjectId(main_task_id_str)},
                    {'$set': {f'files_tracker.{safe_key}.status': 'error', f'files_tracker.{safe_key}.error': str(e)}}
                )
        
        logging.info(f"🏁 Thread finished for Task ID: {main_task_id_str}")

# ==========================================
# 2. CORE AUDIT ENDPOINTS
# ==========================================

@call_audit_bp.route('/api/call/audit', methods=['POST'])
@jwt_required()
def upload_call_audit():
    """
    Client Endpoint: Uploads MULTIPLE audio files.
    Refactored to use SINGLE TASK ID architecture but returns FULL detailed response.
    """
    try:
        # 1️⃣ CAPTURE FILES
        files = []
        if 'audio_files' in request.files:
            files.extend(request.files.getlist('audio_files'))
        if 'audio_file' in request.files:
            files.extend(request.files.getlist('audio_file'))
            
        files = [f for f in files if f.filename]
        logging.info(f"📥 [Bulk Upload] Received {len(files)} audio files.")

        if not files:
            return jsonify({"error": "No audio files provided"}), 400

        # 2️⃣ PREPARE COMMON DATA
        criteria_list = list(mongo.db.criteria.find(
            {"is_active": True, "type": "call audit"}, 
            {'_id': 0, 'name': 1, 'weight': 1, 'description': 1}
        ))
        
        if not criteria_list:
            return jsonify({"error": "No audit criteria configured."}), 500

        config_doc = mongo.db.api_config.find_one({"name": "openai_api_key"})
        api_key = config_doc.get("key") if config_doc else None
        
        if not api_key:
            return jsonify({"error": "OpenAI Key not configured"}), 500

        core_url = current_app.config.get('CORE_SERVICE_URL') + "/internal/process-call"

        # 3️⃣ PRE-PROCESS FILES & CREATE TRACKER
        files_tracker = {}
        files_to_thread = []  

        for f in files:
            safe_key = f.filename.replace('.', '_')
            files_tracker[safe_key] = {"status": "queued", "error": None}
            
            # 🟢 CRITICAL: Read content into memory BEFORE returning the response
            f.stream.seek(0)
            content = f.read()
            if len(content) > 0:
                files_to_thread.append((f.filename, content, f.mimetype))

        if not files_to_thread:
            return jsonify({"error": "No valid file data found"}), 400

        batch_name = files[0].filename if len(files) == 1 else f"{files[0].filename} + {len(files)-1} others"

        # Create Task Document
        inserted_id = mongo.db.tasks.insert_one({
            'type': 'call_audit_batch',
            'status': 'processing',
            'filename': batch_name, 
            'files_tracker': files_tracker, 
            'total_files': len(files_to_thread),
            'completed_count': 0,
            'audit_category': 'call audit',
            'created_at': datetime.now(),
            'output_excel_id': None
        }).inserted_id

        main_task_id_str = str(inserted_id)
        logging.info(f"🆔 Created Master Task: {main_task_id_str}")

        # 4️⃣ START BACKGROUND THREAD
        # current_app._get_current_object() is the most reliable way to pass the app to a thread
        flask_app = current_app._get_current_object()
        thread = threading.Thread(
            target=run_audit_in_background,
            args=(flask_app, files_to_thread, main_task_id_str, criteria_list, api_key, core_url)
        )
        thread.daemon = True # Ensure thread doesn't block server exit
        thread.start()

        # 5️⃣ RETURN IMMEDIATELY (Prevent UI Timeout)
        return jsonify({
            "message": f"Processing started for {len(files_to_thread)} files.",
            "task_id": main_task_id_str,
            "status": "accepted"
        }), 202

    except Exception as e:
        logging.error(f"❌ Critical Upload Error: {str(e)}")
        return jsonify({"error": str(e)}), 500
    

@call_audit_bp.route('/internal/save-call-results', methods=['POST'])
def save_call_results():
    try:
        # 1. Get the Composite ID
        composite_id = request.form.get('task_id')
        audit_results_str = request.form.get('audit_results')
        
        # 2. Split it back apart
        if "___" in composite_id:
            main_task_id, filename = composite_id.split("___", 1)
        else:
            return jsonify({"error": "Invalid Composite ID format"}), 400

        # 3. Save Raw Data
        audit_data = json.loads(audit_results_str)
        result_item = audit_data[0] if isinstance(audit_data, list) else audit_data
        
        # Add filename context to the data
        result_item['filename'] = filename 

        mongo.db.call_audit_results.insert_one({
            "task_id": main_task_id, 
            "filename": filename,
            "full_data": result_item,
            "created_at": datetime.now()
        })

        # 🟢 CRITICAL FIX: Sanitize filename for MongoDB Key (Replace . with _)
        # This ensures 'audio.wav' becomes 'audio_wav' for the update path
        safe_tracker_key = filename.replace('.', '_')

        # 4. Update the Tracker in the Main Task
        updated_task = mongo.db.tasks.find_one_and_update(
            {'_id': ObjectId(main_task_id)},
            {
                # Use the SAFE key here
                '$set': {f'files_tracker.{safe_tracker_key}.status': 'complete'},
                '$inc': {'completed_count': 1}
            },
            return_document=True
        )

        # 5. Check if Batch is 100% Done
        total = updated_task.get('total_files', 0)
        done = updated_task.get('completed_count', 0)
        
        logging.info(f"📊 Progress for {main_task_id}: {done}/{total}")

        if done >= total:
            logging.info(f"🏁 Task {main_task_id} Complete! Generating Master Excel...")
            
            # A. Fetch ALL results
            all_results = list(mongo.db.call_audit_results.find({'task_id': main_task_id}))
            
            # B. Generate Excel
            engine = CallReportEngine()
            excel_output = engine.generate_excel(all_results)
            
            # C. Save Excel and Update Main Task
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
                    'completed_at': datetime.now()
                }}
            )

        return jsonify({"status": "success"}), 200

    except Exception as e:
        logging.error(f"Error saving results: {e}")
        return jsonify({"error": str(e)}), 500
# ==========================================
# 2. RESTORED DASHBOARD & UTILITY ENDPOINTS
# ==========================================

@call_audit_bp.route('/dashboard', methods=['GET'])
@jwt_required()
def get_dashboard_data():
    """
    Fetches data and prepares it for the dashboard UI.
    Now reads from the 'call_audit_results' collection.
    """
    try:
        # Fetch audits sorted by newest first
        audits = list(mongo.db.call_audit_results.find().sort("created_at", -1))
        
        # Calculate Average Quality Score
        scores = [a.get('score', 0) for a in audits]
        avg_score = sum(scores) / len(scores) if scores else 0
        
        # Format for Frontend
        for a in audits:
            a["_id"] = str(a["_id"])
            
            # Flatten User Info object into a string for the table
            u = a.get("user_info")
            
            # Logic to handle if user_info is inside full_data (common with new engine)
            if not u and a.get('full_data'):
                u = a['full_data'].get('User Info')

            if isinstance(u, dict):
                # Filter out None values and join with pipe
                parts = [u.get('name') or u.get('user_name'), u.get('email'), u.get('phone') or u.get('phone_number')]
                a["user_info"] = " | ".join(filter(None, parts))
            elif not u:
                a["user_info"] = "N/A"

        return jsonify({
            "audits": audits,
            "summary": {"avg_score": round(avg_score, 1)}
        })
    except Exception as e:
        logging.error(f"❌ Dashboard Error: {e}")
        return jsonify({"error": str(e)}), 500


@call_audit_bp.route('/save-rules', methods=['POST'])
@jwt_required()
def save_rules_only():
    """
    Endpoint to save rules to MongoDB without running an audit.
    """
    try:
        rules = request.json.get('rules', [])
        if not rules:
            return jsonify({"error": "No rules provided"}), 400
            
        # Save to 'audit_rules' collection
        for r in rules:
            mongo.db.audit_rules.update_one(
                {"name": r["name"]}, 
                {"$set": r}, 
                upsert=True
            )
        
        return jsonify({"message": "Rules successfully synced to MongoDB"})
    except Exception as e:
        logging.error(f"❌ Save Rules Error: {e}")
        return jsonify({"error": str(e)}), 500
    

# @call_audit_bp.route('/api/call/report/<task_id>', methods=['GET'])
#@jwt_required()
# def download_task_report(task_id):
#     """
#     Endpoint to download the generated Excel report for a specific task batch.
#     """
#     try:
#         # 1. Look for the Master Task (Parent)
#         task = mongo.db.tasks.find_one({'_id': ObjectId(task_id)})
        
#         if not task:
#             return jsonify({"error": "Task not found"}), 404
        
#         # 2. Check if Excel is ready
#         excel_id = task.get('output_excel_id')
        
#         if not excel_id:
#             # Friendly status message if processing is still underway
#             done = task.get('completed_files', 0)
#             total = task.get('total_files', '?')
#             return jsonify({
#                 "error": "Report is processing", 
#                 "progress": f"{done}/{total} files completed"
#             }), 202

#         # 3. Stream the file from GridFS
#         grid_out = current_app.fs.get(ObjectId(excel_id))
#         return send_file(
#             grid_out,
#             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
#             as_attachment=True,
#             download_name=grid_out.filename
#         )

#     except Exception as e:
#         logging.error(f"❌ Download Error: {e}")
#         return jsonify({"error": str(e)}), 500