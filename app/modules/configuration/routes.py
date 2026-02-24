import pytz
import requests
from datetime import datetime, timezone
from flask import request, jsonify, current_app
from . import config_bp
import logging
from app.decorators import role_required
from flask_jwt_extended import jwt_required, get_jwt
import uuid

# 🟢 POSTGRESQL MODELS IMPORT
from app.models import db, ApiConfig, Criterion

# 🟢 Add Helpers
def get_utc_now():
    return datetime.now(timezone.utc)

def format_to_iso_z(dt):
    if not dt: return None
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

def api_response(data=None, message="", status=200):
    return jsonify({
        "status": "success" if status < 400 else "error",
        "message": message,
        "data": data
    }), status

# --- Integration / Tools Routes ---

@config_bp.route('/api/integrations/<config_id>/tools', methods=['POST'])
@jwt_required()
def add_new_tool(config_id):
    """Adds a NEW tool to an existing category."""
    try:
        if not config_id.isdigit():
            return jsonify({'error': 'Invalid Configuration ID'}), 400

        data = request.get_json()
        if not data.get('tool_name') or not data.get('instance_url'):
            return jsonify({'error': 'Tool name and Instance URL are required'}), 400

        # Fetch the existing Config row
        config_record = ApiConfig.query.get(int(config_id))
        if not config_record:
            return jsonify({'error': 'Category configuration not found'}), 404

        new_tool_id = str(uuid.uuid4())
        new_tool = {
            "tool_id": new_tool_id,
            "tool_name": data.get('tool_name'),
            "instance_url": data.get('instance_url'),
            "credentials": data.get('credentials', {}),
            "sync_scheduler": data.get('sync_scheduler', {}),
            "created_at": get_utc_now().isoformat()
        }

        # Initialize tools list if None, then append
        current_tools = config_record.tools or []
        current_tools.append(new_tool)
        
        # 🟢 SQL update logic
        config_record.tools = current_tools
        config_record.updated_at = get_utc_now()
        db.session.commit()

        return jsonify({'message': 'Tool added successfully', 'tool_id': new_tool_id}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@config_bp.route('/api/configs', methods=['GET'])
@jwt_required()
def get_all_configs():
    """Fetch configs with credential masking."""
    try:
        claims = get_jwt()
        project_code = claims.get('project')
        category_filter = request.args.get('category')
        
        query = ApiConfig.query.filter_by(project_code=project_code)
        if category_filter:
            query = query.filter_by(category=category_filter)

        configs_records = query.all()
        configs = []

        for c in configs_records:
            config_entry = {
                'id': str(c.id),
                'category': c.category,
                'updated_at': format_to_iso_z(c.updated_at)
            }

            if c.tools:
                tools_safe = []
                for tool in c.tools:
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
@jwt_required()
@role_required(['superadmin', 'admin'])
def update_config(config_id):
    """Merge logic for updating tool configurations."""
    try:
        if not config_id.isdigit():
            return jsonify({'error': 'Invalid Configuration ID'}), 400

        data = request.get_json()
        incoming_tools = data.get('tools')
        if not incoming_tools:
            return jsonify({'error': 'Tools list required'}), 400

        config_record = ApiConfig.query.get(int(config_id))
        if not config_record:
            return jsonify({'error': 'Configuration not found'}), 404

        db_tools_list = config_record.tools or []

        for new_tool in incoming_tools:
            tool_name = new_tool.get('tool_name')
            match_found = False

            for index, db_tool in enumerate(db_tools_list):
                if db_tool.get('tool_name') == tool_name:
                    match_found = True
                    
                    # Password retention logic
                    new_creds = new_tool.get('credentials', {})
                    if new_creds.get('password') == "********":
                         new_tool['credentials']['password'] = db_tool['credentials'].get('password')
                    
                    if new_creds.get('api_token', '').startswith('****'):
                         new_tool['credentials']['api_token'] = db_tool['credentials'].get('api_token')

                    db_tools_list[index] = new_tool 
                    break
            
            if not match_found:
                db_tools_list.append(new_tool)

        config_record.tools = db_tools_list
        config_record.updated_at = get_utc_now()
        db.session.commit()

        return jsonify({'message': 'Configuration updated successfully'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

# --- Evaluation Criteria Routes ---

VALID_CRITERIA_TYPES = {'ticket audit', 'call audit'}

@config_bp.route('/api/criteria', methods=['GET'])
@jwt_required()
def get_criteria():
    """List evaluation criteria."""
    try:
        claims = get_jwt()
        project_code = claims.get('project')
        requested_type = request.args.get('type')

        query = Criterion.query.filter_by(project_code=project_code, is_active=True)
        if requested_type:
            query = query.filter_by(type=requested_type)

        criteria_records = query.order_by(Criterion.name.asc()).all()
        criteria_list = []

        for c in criteria_records:
            criteria_list.append({
                'id': str(c.id),
                'name': c.name,
                'description': c.description or '',
                'type': c.type or 'ticket audit',
                'weight': c.weight or 1.0,
                'is_active': c.is_active,
                'last_modified_by_role': c.last_modified_by_role or 'System',
                'updated_at': format_to_iso_z(c.updated_at),
                'created_at': format_to_iso_z(c.created_at)
            })
        
        return api_response(data=criteria_list, message=f"Retrieved {len(criteria_list)} criteria", status=200)
    except Exception as e:
        logging.error(f"Error fetching criteria: {e}")
        return api_response(message="Internal Server Error", status=500)

@config_bp.route('/api/criteria', methods=['POST'])
@jwt_required()
def add_criterion():
    """Add new criterion with AI Guardrail validation."""
    try:
        claims = get_jwt()
        project_code = claims.get('project')
        data = request.get_json()
        name = data.get('name', '').strip()
        weight = float(data.get('weight', 1.0))
        criterion_type = data.get('type', '').strip().lower() 
        user_role = claims.get('role', 'System')
        description = data.get('description', '').strip() 

        if not name: 
            return api_response(message='Name is required', status=400)
        if criterion_type not in VALID_CRITERIA_TYPES: 
            return api_response(message=f'Invalid type. Use: {", ".join(VALID_CRITERIA_TYPES)}', status=400)

        # 🟢 AI Guardrail Logic
        try:
            config_doc = ApiConfig.query.filter_by(name="openai_api_key", project_code=project_code).first()
            api_key = config_doc.key if config_doc else None
            base_url = current_app.config.get('CORE_SERVICE_URL')
            
            if api_key and base_url:
                core_url = base_url.rstrip('/') + "/internal/validate-criteria"
                response = requests.post(core_url, json={"term": name, "api_key": api_key}, timeout=10) 
                if response.status_code == 200:
                    result = response.json()
                    if not result.get('is_valid', True):
                        return api_response(message=f"Rejected by AI: {result.get('reason')}", status=400)
        except Exception as ai_error:
            logging.error(f"⚠️ Core Validation Unreachable: {ai_error}")

        # 🟢 Duplicate Check
        duplicate = Criterion.query.filter(
            Criterion.name.ilike(name),
            Criterion.type == criterion_type,
            Criterion.is_active == True,
            Criterion.project_code == project_code
        ).first()

        if duplicate:
            return api_response(message='Active criterion with this name already exists', status=409)

        # 🟢 Postgres Insert
        new_crit = Criterion(
            name=name,
            description=description,
            type=criterion_type,
            weight=weight,
            is_active=True,
            last_modified_by_role=user_role,
            project_code=project_code
        )
        db.session.add(new_crit)
        db.session.commit()
        
        return api_response(data={'id': new_crit.id, 'name': name}, message='Created successfully', status=201)

    except Exception as e:
        db.session.rollback()
        logging.error(f"Error adding criterion: {e}")
        return api_response(message="Internal Server Error", status=500)

@config_bp.route('/api/criteria/<crit_id>', methods=['PUT'])
@jwt_required()
@role_required(['superadmin', 'admin'])
def update_criterion(crit_id):
    """Update criterion with duplicate checking."""
    try:
        if not crit_id.isdigit():
            return api_response(message='Invalid ID format', status=400)

        claims = get_jwt()
        project_code = claims.get('project')
        data = request.get_json()

        current_doc = Criterion.query.get(int(crit_id))
        if not current_doc or current_doc.project_code != project_code:
             return api_response(message='Criterion not found', status=404)

        new_name = data.get('name', current_doc.name).strip()
        new_type = data.get('type', current_doc.type).strip().lower()

        if 'type' in data and new_type not in VALID_CRITERIA_TYPES:
             return api_response(message='Invalid type', status=400)

        # Handle Name/Type Duplicate Check
        duplicate = Criterion.query.filter(
            Criterion.name.ilike(new_name),
            Criterion.type == new_type,
            Criterion.id != int(crit_id),
            Criterion.is_active == True,
            Criterion.project_code == project_code
        ).first()
        
        if duplicate:
            return api_response(message='Name already taken in this category', status=409)

        # Map updates
        current_doc.name = new_name
        current_doc.type = new_type
        if 'description' in data: current_doc.description = data['description']
        if 'weight' in data: current_doc.weight = float(data['weight'])
        if 'is_active' in data: current_doc.is_active = bool(data['is_active'])
        
        current_doc.last_modified_by_role = claims.get('role', 'System')
        current_doc.updated_at = get_utc_now()
        
        db.session.commit()
        return api_response(message='Updated successfully', status=200)

    except Exception as e:
        db.session.rollback()
        return api_response(message="Internal Server Error", status=500)

@config_bp.route('/api/criteria/<crit_id>', methods=['DELETE'])
@jwt_required()
@role_required(['superadmin'])
def delete_criterion(crit_id):
    """Soft delete a criterion."""
    try:
        if not crit_id.isdigit():
            return api_response(message='Invalid ID', status=400)

        claims = get_jwt()
        project_code = claims.get('project')
        
        criterion = Criterion.query.filter_by(id=int(crit_id), project_code=project_code).first()
        if not criterion: 
            return api_response(message='Criterion not found', status=404)

        criterion.is_active = False
        criterion.updated_at = get_utc_now()
        criterion.last_modified_by_role = claims.get('role', 'System')
        
        db.session.commit()
        return api_response(message='Criterion deactivated successfully', status=200)
    except Exception as e:
        db.session.rollback()
        return api_response(message="Internal Server Error", status=500)