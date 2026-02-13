from flask import Blueprint, request, jsonify, g
from config import Config
from app.extensions import mongo, jwt  # Assuming you init JWTManager in extensions
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import (
    create_access_token, 
    jwt_required, 
    get_jwt_identity, 
    get_jwt,
    unset_jwt_cookies
)
from datetime import datetime, timedelta, timezone
from app.decorators import role_required
from bson.objectid import ObjectId

auth_bp = Blueprint('auth', __name__)

# 🟢 Add Helpers
def get_utc_now():
    return datetime.now(timezone.utc)

def format_to_iso_z(dt):
    if not dt: return None
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

@auth_bp.route('/register', methods=['POST'])
def register():
    try:
        data = request.get_json()
        
        # 1. Validation
        # 'project' is still required so we know where to assign them!
        required = ['username', 'email', 'password', 'role', 'project']
        if not all(k in data for k in required):
            return jsonify({"error": "Missing fields"}), 400

        project_code = data['project']

        # 🟢 Validate Project Code exists in our System
        # We don't want to assign a user to a database that doesn't exist.
        if project_code not in Config.TENANTS:
             return jsonify({"error": "Invalid Project Code provided"}), 400

        # 2. Check Central DB for duplicates
        # We use mongo.central_db now!
        if mongo.central_db.users.find_one({"email": data['email']}):
            return jsonify({"error": "Email already registered"}), 409

        hashed_password = generate_password_hash(data['password'])

        # 3. Save to CENTRAL DB
        new_user = {
            "username": data['username'],
            "email": data['email'],
            "password": hashed_password,
            "role": data['role'],
            "project": project_code,# Storing the link here
            "is_active": True,
            "created_at": get_utc_now()
        }
        
        mongo.central_db.users.insert_one(new_user)

        return jsonify({"message": "User registered in Central Auth system"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@auth_bp.route('/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        
        # 🟢 NOTICE: We DO NOT need 'project' in the request anymore!
        # The user just provides Email + Password.
        
        # 1. Find User in Central DB
        user = mongo.central_db.users.find_one({"email": data.get('email')})

        if not user:
            return jsonify({"error": "Invalid credentials"}), 401
        
        if not user.get('is_active', True):
            return jsonify({"error": "Account is inactive. Please contact administrator."}), 403

        if not check_password_hash(user['password'], data.get('password')):
            return jsonify({"error": "Invalid credentials"}), 401

        # 2. Retrieve their assigned Project from the DB document
        user_project = user.get('project')
        
        if not user_project:
             return jsonify({"error": "User account corrupted: No project assigned"}), 500

        # 3. Generate Token WITH PROJECT CLAIM
        # The rest of your app (Tasks, Audits) relies on this token claim.
        # Since we put it here, the rest of the app "just works" without changes.
        access_token = create_access_token(
            identity=str(user['_id']), 
            additional_claims={
                "role": user['role'], 
                "project": user_project, # <--- We pulled this from Central DB
                "username": user['username']
            }
        )

        return jsonify({ 
            "message": "Login successful",
            "access_token": access_token,
            "project": user_project # Optional: Let frontend know where they landed
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- 3. PROTECTED ROUTE EXAMPLE (Verify Token) ---
@auth_bp.route('/profile', methods=['GET'])
@jwt_required()
def profile():
    """
    Example of a protected route. Only accessible with a valid Token.
    """
    current_user_id = get_jwt_identity()
    claims = get_jwt() # Get the extra data we saved (role)
    
    return jsonify({
        "id": current_user_id,
        "role": claims['role'],
        "message": f"Hello {claims['username']}, you have {claims['role']} access."
    }), 200


# --- 4. LOGOUT ENDPOINT ---
@auth_bp.route("/logout", methods=["POST"])
def logout():
    """
    Client-side: Delete the token from localStorage.
    Server-side (Optional): You can add the token to a Redis blacklist here if needed.
    """
    response = jsonify({"message": "Logout successful"})
    unset_jwt_cookies(response)
    return response, 200

@auth_bp.route("/users", methods=['GET'])
@jwt_required()
@role_required(['superadmin', 'admin'])
def get_users():
    """
    Fetch all users from the Central DB with their details.
    """
    try:
        # Fetch users but exclude the password field (0 means exclude)
        users_cursor = mongo.central_db.users.find({}, {'password': 0})
        
        users_list = []
        for user in users_cursor:
            users_list.append({
                "id": str(user['_id']),
                "username": user.get('username'),
                "email": user.get('email'),
                "role": user.get('role'),
                "project": user.get('project'),
                # Default to True if field is missing (for old users)
                "is_active": user.get('is_active', True), 
                "created_at": format_to_iso_z(user.get('created_at'))
            })

        return jsonify(users_list), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# 2. UPDATE USER (Role & Active Status)
@auth_bp.route('/users/<user_id>', methods=['PUT'])
@jwt_required()
@role_required(['superadmin', 'admin'])
def update_user(user_id):
    """
    Update a user's role or active status.
    """
    try:
        data = request.get_json()
        update_fields = {}

        # Update Role
        if 'role' in data:
            # Optional: Add validation for valid roles
            if data['role'] not in ['superadmin', 'admin', 'manager', 'user']:
                return jsonify({"error": "Invalid role provided"}), 400
            update_fields['role'] = data['role']

        # Update Active Status
        if 'is_active' in data:
            update_fields['is_active'] = bool(data['is_active'])

        if not update_fields:
            return jsonify({"error": "No valid fields to update"}), 400

        # Perform Update on Central DB
        result = mongo.central_db.users.find_one_and_update(
            {'_id': ObjectId(user_id)},
            {'$set': update_fields},
            return_document=True
        )

        if not result:
            return jsonify({"error": "User not found"}), 404

        return jsonify({
            "message": "User updated successfully",
            "user": {
                "id": str(result['_id']),
                "username": result['username'],
                "role": result['role'],
                "is_active": result.get('is_active', True)
            }
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# 3. SOFT DELETE USER
@auth_bp.route('/users/<user_id>', methods=['DELETE'])
@jwt_required()
@role_required(['superadmin']) # Maybe only superadmin can delete?
def delete_user(user_id):
    """
    Soft delete a user by setting is_active = False.
    """
    try:
        # We do NOT remove the document. We just update the status.
        result = mongo.central_db.users.find_one_and_update(
            {'_id': ObjectId(user_id)},
            {'$set': {'is_active': False}},
            return_document=True
        )

        if not result:
            return jsonify({"error": "User not found"}), 404

        return jsonify({
            "message": "User deactivated successfully (Soft Delete)",
            "user_id": str(result['_id']),
            "is_active": result['is_active']
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500