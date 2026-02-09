import json
import logging
import requests
from datetime import datetime
from bson.objectid import ObjectId
from flask import request, jsonify, current_app,send_file
from . import call_audit_bp
from app.extensions import mongo
from app.engine.call_report import CallReportEngine
import io
import pandas as pd
import re
from flask_jwt_extended import jwt_required
# ==========================================
# 1. CORE AUDIT ENDPOINTS
# ==========================================

def parse_filename_metadata(filename):
    """
    Extracts Agent Name and Date from format:
    "[Aswin Srinivasan]_101-+12106017188_20251229082336(2830).wav"
    """
    agent_name = "Unknown"
    audit_date = None

    # 1. Extract Name: Content inside [ ] at the start
    # r"^\[([^\]]+)\]" -> Starts with [, capture anything not ], ends with ]
    name_match = re.search(r"^\[([^\]]+)\]", filename)
    if name_match:
        agent_name = name_match.group(1)

    # 2. Extract Date: Look for YYYYMMDD pattern (8 digits) followed by HHMMSS (6 digits)
    # This is specific to your timestamp format "20251229082336"
    date_match = re.search(r"(\d{8})\d{6}", filename)
    if date_match:
        raw_date = date_match.group(1) # e.g., "20251229"
        try:
            # Convert "20251229" -> "2025-12-29"
            dt_obj = datetime.strptime(raw_date, "%Y%m%d")
            audit_date = dt_obj.strftime("%Y-%m-%d")
        except ValueError:
            pass # Use None if date is invalid

    return agent_name, audit_date

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

        # 3️⃣ CREATE ONE MASTER TASK (The "Files Tracker")
        files_tracker = {}
        for f in files:
            # 🟢 FIX: Sanitize key for MongoDB
            safe_key = f.filename.replace('.', '_')
            files_tracker[safe_key] = {"status": "queued", "error": None}

        if len(files) == 1:
            batch_name = files[0].filename
        else:
            # Example: "audio1.wav + 4 others"
            batch_name = f"{files[0].filename} + {len(files)-1} others"

        main_task_id = mongo.db.tasks.insert_one({
            'type': 'call_audit_batch',
            'status': 'processing',
            'filename': batch_name, 
            'files_tracker': files_tracker, 
            'total_files': len(files),
            'completed_count': 0,
            'audit_category': 'call audit',
            'created_at': datetime.now(),
            'output_excel_id': None
        }).inserted_id

        logging.info(f"🆔 Created Single Master Task: {main_task_id}")

        # 4️⃣ LOOP & PROCESS
        created_sub_tasks = []  # 🟢 RESTORED: To hold detailed success response
        errors = []

        for index, file in enumerate(files, start=1):
            safe_key = file.filename.replace('.', '_')
            
            try:
                logging.info(f"🔄 Processing {index}/{len(files)}: '{file.filename}'")

                # --- A. Zero Byte Check ---
                file.stream.seek(0, 2)
                file_size = file.stream.tell()
                file.stream.seek(0)
                
                if file_size == 0:
                    error_msg = "Empty file (0 bytes)"
                    logging.error(f"❌ {file.filename}: {error_msg}")
                    errors.append({"filename": file.filename, "error": error_msg})
                    mongo.db.tasks.update_one(
                        {'_id': main_task_id},
                        {
                            '$set': {f'files_tracker.{safe_key}.status': 'error'},
                            '$inc': {'total_files': -1}
                        }
                    )
                    continue

                # --- B. Composite ID ---
                composite_id = f"{main_task_id}___{file.filename}"

                # --- C. Send to Core ---
                current_files = {'audio_file': (file.filename, file.stream, file.mimetype)}
                data = {
                    'task_id': composite_id,
                    'api_key': api_key,
                    'criteria': json.dumps(criteria_list)
                }

                response = requests.post(core_url, files=current_files, data=data, timeout=14400)

                if response.status_code == 200:
                    logging.info(f"   ✅ Sent '{file.filename}' to Core.")
                    
                    # Update DB status
                    mongo.db.tasks.update_one(
                        {'_id': main_task_id},
                        {'$set': {f'files_tracker.{safe_key}.status': 'processing'}}
                    )
                    
                    # 🟢 RESTORED: Add to response list
                    created_sub_tasks.append({
                        "filename": file.filename,
                        "sub_task_id": composite_id, # Use Composite ID as the unique ref
                        "status": "queued"
                    })
                else:
                    logging.error(f"   ⚠️ Core Error: {response.text}")
                    mongo.db.tasks.update_one(
                        {'_id': main_task_id},
                        {'$set': {f'files_tracker.{safe_key}.status': 'error'}}
                    )
                    errors.append({"filename": file.filename, "error": "Core rejected"})

            except Exception as e:
                logging.error(f"   ❌ Exception on '{file.filename}': {e}")
                mongo.db.tasks.update_one(
                    {'_id': main_task_id},
                    {'$set': {f'files_tracker.{safe_key}.status': 'error'}}
                )
                errors.append({"filename": file.filename, "error": str(e)})

        # 5️⃣ RETURN DETAILED RESPONSE (Restored Format)
        logging.info(f"🏁 Batch Complete. {len(created_sub_tasks)} queued, {len(errors)} failed.")

        response_payload = {
            "message": f"Processing started for {len(created_sub_tasks)} files.",
            "task_id": str(main_task_id),
            "sub_tasks": created_sub_tasks  # 🟢 RESTORED LIST
        }

        if errors:
            response_payload["errors"] = errors

        return jsonify(response_payload), 200 if created_sub_tasks else 500

    except Exception as e:
        logging.error(f"❌ Critical Upload Error: {e}")
        return jsonify({"error": str(e)}), 500
    

@call_audit_bp.route('/internal/save-call-results', methods=['POST'])
def save_call_results():
    try:
        # 1. Get the Composite ID
        composite_id = request.form.get('task_id')
        audit_results_str = request.form.get('audit_results')
        
        # 🟢 REVERTED: Old Logic (Split only into 2 parts)
        # We assume the ID is just "task_id___filename"
        if composite_id and "___" in composite_id:
            try:
                main_task_id, filename = composite_id.split("___", 1)
            except ValueError:
                return jsonify({"error": "Invalid Composite ID format (Expected task___file)"}), 400
        else:
            return jsonify({"error": "Missing or Invalid task_id"}), 400

        # 3. Save Raw Data
        audit_data = json.loads(audit_results_str)
        result_item = audit_data[0] if isinstance(audit_data, list) else audit_data

        # 🟢 KEEPING THE NEW REQUIREMENT: Extract Metadata
        agent_name, agent_date = parse_filename_metadata(filename)
        
        # Add context to the data object
        result_item['filename'] = filename 

        # 🟢 KEEPING THE NEW FIELDS IN DB
        mongo.db.call_audit_results.insert_one({
            "task_id": main_task_id, 
            "filename": filename,
            "agent_name": agent_name,       # <--- Saved
            "agent_audit_date": agent_date, # <--- Saved
            "full_data": result_item,
            "created_at": datetime.now()
        })

        # --- The rest is standard tracking logic ---

        # 🟢 CRITICAL FIX: Sanitize filename for MongoDB Key (Replace . with _)
        safe_tracker_key = filename.replace('.', '_')

        # 4. Update the Tracker in the Main Task
        updated_task = mongo.db.tasks.find_one_and_update(
            {'_id': ObjectId(main_task_id)},
            {
                '$set': {f'files_tracker.{safe_tracker_key}.status': 'complete'},
                '$inc': {'completed_count': 1}
            },
            return_document=True
        )

        # 5. Check if Batch is 100% Done
        if updated_task:
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

