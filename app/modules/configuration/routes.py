import pytz
import requests
from datetime import datetime, timezone
from bson.objectid import ObjectId
from flask import request, jsonify
from app.extensions import mongo
from flask import current_app
from . import config_bp
import logging
from app.decorators import role_required
from flask_jwt_extended import jwt_required, get_jwt
import uuid


# 🟢 Add Helpers
def get_utc_now():
    return datetime.now(timezone.utc)

def format_to_iso_z(dt):
    if not dt: return None
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


# --- API Configuration Routes (e.g. OpenAI Key) ---
def api_response(data=None, message="", status=200):
    return jsonify({
        "status": "success" if status < 400 else "error",
        "message": message,
        "data": data
    }), status

@config_bp.route('/api/integrations/<config_id>/tools', methods=['POST'])
@jwt_required()
def add_new_tool(config_id):
    """
    Adds a NEW tool to an existing category (found by config_id).
    Auto-generates a unique tool_id.
    """
    try:
        # 1. Validate Config ID
        if not ObjectId.is_valid(config_id):
            return jsonify({'error': 'Invalid Configuration ID'}), 400

        data = request.get_json()
        
        # 2. Basic Validation
        if not data.get('tool_name') or not data.get('instance_url'):
            return jsonify({'error': 'Tool name and Instance URL are required'}), 400

        # 3. Create the Tool Object
        # We auto-generate the UUID here so the frontend doesn't have to.
        new_tool_id = str(uuid.uuid4())
        
        new_tool = {
            "tool_id": new_tool_id,
            "tool_name": data.get('tool_name'),
            "instance_url": data.get('instance_url'),
            "credentials": data.get('credentials', {}),
            "sync_scheduler": data.get('sync_scheduler', {}),
            "created_at": get_utc_now() # Track creation time
        }

        # 4. Push to MongoDB
        # $push: Appends the new_tool to the 'tools' array
        res = mongo.db.api_config.update_one(
            {'_id': ObjectId(config_id)},
            {
                '$push': {'tools': new_tool},
                '$set': {'updated_at': datetime.now()}
            }
        )

        if res.matched_count == 0:
            return jsonify({'error': 'Category configuration not found'}), 404

        return jsonify({
            'message': 'Tool added successfully', 
            'tool_id': new_tool_id
        }), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

@config_bp.route('/api/configs', methods=['GET'])
@jwt_required()
def get_all_configs():
    """
    Fetch configs. 
    Can filter by category (e.g., /api/configs?category=ITSM)
    """
    try:
        # 1. Check if user sent a specific category
        category_filter = request.args.get('category')
        
        # 2. Build Query
        # Always look for documents with 'tools' (Integrations)
        query = {'tools': {'$exists': True}}
        
        # If category is provided, add it to the query
        if category_filter:
            query['category'] = category_filter

        configs = []
        for c in mongo.db.api_config.find(query):
            config_entry = {
                '_id': str(c['_id']),
                'category': c.get('category'),
                'updated_at': format_to_iso_z(c.get('updated_at'))
            }

            # 3. Process Tools & Mask Credentials
            if 'tools' in c:
                tools_safe = []
                for tool in c['tools']:
                    safe_tool = tool.copy()
                    
                    if 'credentials' in safe_tool:
                        creds = safe_tool['credentials'].copy()
                        if creds.get('password'):
                            creds['password'] = "********"
                        if creds.get('api_token'):
                            token = creds['api_token']
                            creds['api_token'] = f"****{token[-4:]}" if len(token) > 8 else "****"
                        safe_tool['credentials'] = creds
                    
                    tools_safe.append(safe_tool)
                
                config_entry['tools'] = tools_safe

            configs.append(config_entry)

        return jsonify(configs)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@config_bp.route('/api/configs/<config_id>', methods=['PUT'])
@role_required(['superadmin', 'admin'])
def update_config(config_id):
    try:
        data = request.get_json()
        incoming_tools = data.get('tools')

        if not incoming_tools:
            return jsonify({'error': 'Tools list required'}), 400

        # 1. Fetch the EXISTING document
        existing_doc = mongo.db.api_config.find_one({'_id': ObjectId(config_id)})
        if not existing_doc:
            return jsonify({'error': 'Configuration not found'}), 404

        # 2. Get the current list of tools from DB (or empty list if none)
        # We allow the DB version to be the "Master" list
        db_tools_list = existing_doc.get('tools', [])

        # 3. MERGE LOGIC: Update specific tools, Keep others untouched
        for new_tool in incoming_tools:
            tool_name = new_tool.get('tool_name')
            match_found = False

            for index, db_tool in enumerate(db_tools_list):
                # We use 'tool_name' as the Unique ID to find the match
                if db_tool.get('tool_name') == tool_name:
                    match_found = True
                    
                    # --- PASSWORD RETENTION LOGIC ---
                    # If user sent ********, keep the OLD password from db_tool
                    new_creds = new_tool.get('credentials', {})
                    if new_creds.get('password') == "********":
                         new_tool['credentials']['password'] = db_tool['credentials'].get('password')
                    
                    if new_creds.get('api_token', '').startswith('****'):
                         new_tool['credentials']['api_token'] = db_tool['credentials'].get('api_token')
                    # -------------------------------

                    # Replace the old tool data with the new merged data
                    db_tools_list[index] = new_tool 
                    break
            
            # If this is a brand new tool not in DB, append it
            if not match_found:
                db_tools_list.append(new_tool)

        # 4. Save the FULL updated list back to DB
        mongo.db.api_config.update_one(
            {'_id': ObjectId(config_id)},
            {'$set': {
                'tools': db_tools_list,  # We save the combined list
                'updated_at': get_utc_now()
            }}
        )

        return jsonify({'message': 'Configuration updated successfully'})

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
# --- Evaluation Criteria Routes ---
VALID_CRITERIA_TYPES = {'ticket audit', 'call audit'}

@config_bp.route('/api/criteria', methods=['GET'])
@jwt_required()
def get_criteria():
    """
    List evaluation criteria, optionally filtered by type.
    Query Params: ?type=call audit
    """
    try:
        criteria_list = []
        
        # 1. Build Query
        # Allow filtering by type (e.g. /api/criteria?type=call audit)
        query = {
            'is_active': {'$ne': False} 
        }
        requested_type = request.args.get('type')
        if requested_type:
            query['type'] = requested_type

        # Sort by name for a consistent UI experience
        cursor = mongo.db.criteria.find(query).sort("name", 1) 

        for c in cursor:
            criteria_list.append({
                'id': str(c['_id']),
                'name': c['name'],
                'description': c.get('description', ''),
                'type': c.get('type', 'ticket audit'), # ✅ Default to ticket audit for legacy data
                'weight': c.get('weight', 1.0),
                'is_active': c.get('is_active', True),
                'last_modified_by_role': c.get('last_modified_by_role', 'System'),
                'updated_at': format_to_iso_z(c.get('updated_at')),
                'created_at': format_to_iso_z(c.get('created_at'))
            })
        
        return api_response(
            data=criteria_list, 
            message=f"Successfully retrieved {len(criteria_list)} criteria",
            status=200
        )

    except Exception as e:
        logging.error(f"Error fetching criteria: {e}")
        return api_response(message="Internal Server Error", status=500)

@config_bp.route('/api/criteria', methods=['POST'])
@jwt_required()
def add_criterion():
    """
    Add a new evaluation rule with a specific type.
    """
    try:
        data = request.get_json()
        name = data.get('name', '').strip()
        weight = float(data.get('weight', 1.0))
        criterion_type = data.get('type', '').strip().lower() 
        claims = get_jwt()
        user_role = claims.get('role', 'System')
        description = data.get('description', '').strip() 

        # 1. Validation (Basic)
        if not name: 
            return api_response(message='Name is required', status=400)
        
        if criterion_type not in VALID_CRITERIA_TYPES: 
            return api_response(
                message=f'Invalid type. Must be one of: {", ".join(VALID_CRITERIA_TYPES)}', 
                status=400
            )

        # =========================================================
        # 🟢 NEW: AI GUARDRAIL (Validate Name with Core Service)
        # =========================================================
        try:
            # 1. Get API Key from the Default DB (mongo.db)
            # We don't need 'mongo.cx' or 'db_name' here anymore.
            config_doc = mongo.db.api_config.find_one({"name": "openai_api_key"})
            api_key = config_doc.get('key') if config_doc else None

            # 2. Get Core URL safely
            base_url = current_app.config.get('CORE_SERVICE_URL')
            
            if api_key and base_url:
                core_url = base_url.rstrip('/') + "/internal/validate-criteria"
                
                # Send HTTP Request
                response = requests.post(core_url, json={
                    "term": name,
                    "api_key": api_key
                }, timeout=3) 

                if response.status_code == 200:
                    result = response.json()
                    # If AI says INVALID, block the request
                    if not result.get('is_valid', True):
                        return api_response(
                            message=f"Criteria Rejected by AI: {result.get('reason')}", 
                            status=400
                        )
                else:
                    logging.warning(f"⚠️ Core Validation failed: {response.status_code}")
            else:
                if not base_url: logging.warning("⚠️ CORE_SERVICE_URL missing in Config.")
                if not api_key: logging.warning("⚠️ OpenAI API Key missing in DB.")

        except Exception as ai_error:
            # Fail Open: Log it but allow the save
            logging.error(f"⚠️ Core Validation Unreachable: {ai_error}")
        # =========================================================

        # 2. Check for duplicates (Name + Type combination)
        duplicate_check = {
            "name": {"$regex": f"^{name}$", "$options": "i"}, 
            "type": criterion_type, 
            "is_active": True
        }
        
        if mongo.db.criteria.find_one(duplicate_check):
            return api_response(
                message=f'Active criterion with this name already exists for {criterion_type}', 
                status=409
            )

        # 3. Create Object
        now = get_utc_now()
        new_crit = {
            "name": name, 
            "description": description,
            "type": criterion_type, 
            "weight": weight, 
            "is_active": True, 
            "created_at": now, 
            "updated_at": now,
            "last_modified_by_role": user_role
        }
        
        # 4. Insert & Return
        res = mongo.db.criteria.insert_one(new_crit)
        new_crit['id'] = str(res.inserted_id)
        del new_crit['_id']
        
        return api_response(
            data=new_crit, 
            message='Criterion created successfully', 
            status=201
        )

    except ValueError:
        return api_response(message="Weight must be a valid number", status=400)
    except Exception as e:
        logging.error(f"Error adding criterion: {e}")
        return api_response(message="Internal Server Error", status=500)


@config_bp.route('/api/criteria/<crit_id>', methods=['PUT'])
@role_required(['superadmin', 'admin'])
def update_criterion(crit_id):
    """
    Update an existing rule (including type).
    """
    try:
        data = request.get_json()

        claims = get_jwt()
        current_role = claims.get('role', 'System')
        
        # 1. Fetch current document first to handle partial updates correctly
        current_doc = mongo.db.criteria.find_one({'_id': ObjectId(crit_id)})
        if not current_doc:
             return api_response(message='Criterion not found', status=404)

        update_fields = {
            "updated_at": get_utc_now(),
            "last_modified_by_role": current_role
        }

        if 'description' in data:
            update_fields['description'] = data['description'].strip()

        # 2. Update Role
        if 'role' in data:
            update_fields['last_modified_by_role'] = data['role']

        # 3. Determine proposed Name and Type for Duplicate Check
        # If user sends new data, use it; otherwise use existing data from DB
        new_name = data.get('name', current_doc['name']).strip()
        new_type = data.get('type', current_doc.get('type', 'ticket audit')).strip().lower()

        # Validate Type if it's being changed
        if 'type' in data and new_type not in VALID_CRITERIA_TYPES:
             return api_response(
                message=f'Invalid type. Must be one of: {", ".join(VALID_CRITERIA_TYPES)}', 
                status=400
            )
        
        if 'type' in data:
            update_fields['type'] = new_type

        # 4. Handle Name/Type Duplicate Check
        # Only run check if Name OR Type is changing
        if 'name' in data or 'type' in data:
            if not new_name:
                 return api_response(message='Name cannot be empty', status=400)
            
            update_fields['name'] = new_name

            # Check if (New Name + New Type) is taken by another ID
            duplicate = mongo.db.criteria.find_one({
                "name": {"$regex": f"^{new_name}$", "$options": "i"}, 
                "type": new_type,  # ✅ Check against the specific type
                "_id": {"$ne": ObjectId(crit_id)},
                "is_active": True
            })
            
            if duplicate:
                return api_response(
                    message=f'Name already taken by another active rule in {new_type}', 
                    status=409
                )

        # 5. Handle Weight
        if 'weight' in data: 
            try:
                update_fields['weight'] = float(data['weight'])
            except ValueError:
                return api_response(message='Weight must be a number', status=400)
            
        if 'is_active' in data: 
            update_fields['is_active'] = bool(data['is_active'])

        # 6. ATOMIC UPDATE
        updated_doc = mongo.db.criteria.find_one_and_update(
            {'_id': ObjectId(crit_id)}, 
            {'$set': update_fields},
            return_document=True 
        )
        
        updated_doc['id'] = str(updated_doc['_id'])
        del updated_doc['_id']

        return api_response(
            data=updated_doc,
            message='Criterion updated successfully',
            status=200
        )

    except Exception as e:
        logging.error(f"Error updating criterion: {e}")
        return api_response(message="Internal Server Error", status=500)

@config_bp.route('/api/criteria/<crit_id>', methods=['DELETE'])
@role_required(['superadmin'])
def delete_criterion(crit_id):
    """
    Soft delete (deactivate) a criterion.
    Returns the deactivated object so the UI can update state immediately.
    """
    try:
        # 1. Try to get role from body (if sent) or query params
        # Note: DELETE requests with bodies are valid but sometimes discouraged.
        # We support both here for flexibility.
        claims = get_jwt()
        current_role = claims.get('role', 'System')
        if request.is_json:
            user_role = request.get_json().get('role', 'System')
        else:
            user_role = request.args.get('role', 'System')

        # 2. Prepare Update Logic (Soft Delete)
        update_fields = {
            'is_active': False, 
            'updated_at': get_utc_now(),
            'last_modified_by_role': current_role # ✅ Track who deleted it
        }

        # 3. ATOMIC Soft Delete
        # We find the item, mark it inactive, and return the NEW state
        deactivated_doc = mongo.db.criteria.find_one_and_update(
            {'_id': ObjectId(crit_id)},
            {'$set': update_fields},
            return_document=True # Return the doc AFTER the update
        )

        if not deactivated_doc: 
            return api_response(message='Criterion not found', status=404)
        
        # 4. Format ID for Frontend
        deactivated_doc['id'] = str(deactivated_doc['_id'])
        del deactivated_doc['_id']
        
        return api_response(
            data=deactivated_doc,
            message='Criterion deactivated successfully',
            status=200
        )

    except Exception as e:
        logging.error(f"Error deleting criterion: {e}")
        return api_response(message="Internal Server Error", status=500)