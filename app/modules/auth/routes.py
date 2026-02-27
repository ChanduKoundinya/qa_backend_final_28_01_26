from flask import Blueprint, request, jsonify, g
from config import Config
import re
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import (
    create_access_token, 
    create_refresh_token, 
    get_jti,              
    jwt_required, 
    get_jwt_identity, 
    get_jwt,
    unset_jwt_cookies
)
from datetime import datetime, timedelta, timezone

# 🟢 NEW POSTGRES IMPORTS
from app.models import db, User, RefreshToken, ApiConfig
from app.decorators import role_required

auth_bp = Blueprint('auth', __name__)

# --- Helpers ---
def get_utc_now():
    return datetime.now(timezone.utc)

def format_to_iso_z(dt):
    if not dt: return None
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

# ==========================================
# 1. REGISTER
# ==========================================
@auth_bp.route('/register', methods=['POST'])
def register():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided in request"}), 400
        
        # 🟢 1. STRICT VALIDATION: Check for missing OR empty fields
        required_fields = ['username', 'email', 'password', 'role', 'project']
        missing_fields = []
        
        for field in required_fields:
            # If the key doesn't exist, OR if it's just an empty string/spaces
            if field not in data or not str(data[field]).strip():
                missing_fields.append(field)
                
        # If any fields are missing, reject the request immediately
        if missing_fields:
            return jsonify({
                "error": "Missing or empty mandatory fields", 
                "missing_fields": missing_fields
            }), 400

        # Now it is 100% safe to extract and clean the variables
        username = data['username'].strip()
        email = data['email'].strip()
        password = data['password']  # We don't strip passwords; spaces might be intentional!
        role = data['role'].strip()
        project_code = data['project'].strip()

        # 🟢 2. Validate Email Format using Regex
        email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if not re.match(email_regex, email):
            return jsonify({"error": "Invalid email format"}), 400

        # 3. Check for duplicates in Postgres
        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            return jsonify({"error": "Email already registered"}), 409

        hashed_password = generate_password_hash(password)

        # 4. Save to Postgres
        new_user = User(
            username=username,
            email=email,
            password=hashed_password,
            role=role,
            project_code=project_code,
            is_active=True,
            created_at=get_utc_now()
        )
        
        db.session.add(new_user)
        db.session.commit()

        return jsonify({"message": "User registered successfully"}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

# ==========================================
# 2. LOGIN
# ==========================================
@auth_bp.route('/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        
        # 1. Find User in Postgres
        user = User.query.filter_by(email=data.get('email')).first()

        if not user:
            return jsonify({"error": "Invalid credentials"}), 401
        
        if not user.is_active:
            return jsonify({"error": "Account is inactive. Please contact administrator."}), 403

        if not check_password_hash(user.password, data.get('password')):
            return jsonify({"error": "Invalid credentials"}), 401

        user_project = user.project_code
        if not user_project:
             return jsonify({"error": "User account corrupted: No project assigned"}), 500
        
        # Get JWT Expiry Time from Config
        try:
            jwt_config = ApiConfig.query.filter_by(name="jwt_settings", project_code=user_project).first()
            access_minutes = 15
            if jwt_config and isinstance(jwt_config.tools, dict):
                 access_minutes = jwt_config.tools.get('access_token_expires_minutes', 15)
        except Exception:
            access_minutes = 15

        # 3. Generate Token WITH PROJECT CLAIM
        access_token = create_access_token(
            identity=str(user.id), 
            additional_claims={
                "role": user.role, 
                "project": user_project, 
                "username": user.username
            },
            expires_delta=timedelta(minutes=access_minutes) 
        )

        # 4. Refresh Token (Long - 7 Days)
        refresh_token = create_refresh_token(
            identity=str(user.id),
            expires_delta=timedelta(days=7) 
        )

        refresh_jti = get_jti(refresh_token)
        
        # 5. Store Refresh Token in Postgres
        new_token_record = RefreshToken(
            jti=refresh_jti,
            user_id=user.id,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=7),
            device=request.headers.get('User-Agent', 'Unknown')
        )
        db.session.add(new_token_record)
        db.session.commit()

        return jsonify({ 
            "message": "Login successful",
            "access_token": access_token,
            "project": user_project,
            "refresh_token": refresh_token,
            "expires_in_minutes": access_minutes
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500
    

# ==========================================
# 3. REFRESH TOKEN
# ==========================================
@auth_bp.route('/refresh', methods=['POST'])
@jwt_required(refresh=True) 
def refresh():
    try:
        current_user_id = int(get_jwt_identity())
        current_jti = get_jwt()["jti"]

        # 1. Revocation Check
        token_record = RefreshToken.query.filter_by(jti=current_jti, user_id=current_user_id).first()
        if not token_record:
            return jsonify({"error": "Refresh token revoked or invalid"}), 401

        # 2. Fetch User
        user = User.query.get(current_user_id)
        if not user:
            return jsonify({"error": "User no longer exists"}), 401

        # 3. Get Expiry Config
        try:
            jwt_config = ApiConfig.query.filter_by(name="jwt_settings", project_code=user.project_code).first()
            access_minutes = 15
            if jwt_config and isinstance(jwt_config.tools, dict):
                 access_minutes = jwt_config.tools.get('access_token_expires_minutes', 15)
        except:
            access_minutes = 15

        # 4. Issue NEW Access Token
        new_access_token = create_access_token(
            identity=str(user.id),
            additional_claims={
                "role": user.role, 
                "project": user.project_code, 
                "username": user.username
            },
            expires_delta=timedelta(minutes=access_minutes)
        )

        return jsonify({
            "access_token": new_access_token,
            "expires_in_minutes": access_minutes
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ==========================================
# 4. PROFILE
# ==========================================
@auth_bp.route('/profile', methods=['GET'])
@jwt_required()
def profile():
    current_user_id = get_jwt_identity()
    claims = get_jwt() 
    
    return jsonify({
        "id": current_user_id,
        "role": claims['role'],
        "message": f"Hello {claims['username']}, you have {claims['role']} access."
    }), 200


# ==========================================
# 5. LOGOUT
# ==========================================
@auth_bp.route("/logout", methods=["POST"])
def logout(): 
    response = jsonify({"message": "Logout successful"})
    unset_jwt_cookies(response)
    return response, 200


# ==========================================
# 6. USER MANAGEMENT (Admin/Superadmin)
# ==========================================
@auth_bp.route("/users", methods=['GET'])
@jwt_required()
@role_required(['superadmin', 'admin'])
def get_users():
    try:
        users_records = User.query.all()
        
        users_list = []
        for user in users_records:
            users_list.append({
                "id": str(user.id),
                "username": user.username,
                "email": user.email,
                "role": user.role,
                "project": user.project_code,
                "is_active": user.is_active, 
                "created_at": format_to_iso_z(user.created_at)
            })

        return jsonify(users_list), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@auth_bp.route('/users/<user_id>', methods=['PUT'])
@jwt_required()
@role_required(['superadmin', 'admin'])
def update_user(user_id):
    try:
        if not user_id.isdigit():
            return jsonify({"error": "Invalid User ID"}), 400

        data = request.get_json()
        user = User.query.get(int(user_id))

        if not user:
            return jsonify({"error": "User not found"}), 404

        has_updates = False

        if 'role' in data:
            if data['role'] not in ['superadmin', 'admin', 'manager', 'user']:
                return jsonify({"error": "Invalid role provided"}), 400
            user.role = data['role']
            has_updates = True

        if 'is_active' in data:
            user.is_active = bool(data['is_active'])
            has_updates = True

        if not has_updates:
            return jsonify({"error": "No valid fields to update"}), 400

        db.session.commit()

        return jsonify({
            "message": "User updated successfully",
            "user": {
                "id": str(user.id),
                "username": user.username,
                "role": user.role,
                "is_active": user.is_active
            }
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@auth_bp.route('/users/<user_id>', methods=['DELETE'])
@jwt_required()
@role_required(['superadmin']) 
def delete_user(user_id):
    try:
        if not user_id.isdigit():
            return jsonify({"error": "Invalid User ID"}), 400

        user = User.query.get(int(user_id))

        if not user:
            return jsonify({"error": "User not found"}), 404

        # Soft Delete
        user.is_active = False
        db.session.commit()

        return jsonify({
            "message": "User deactivated successfully (Soft Delete)",
            "user_id": str(user.id),
            "is_active": user.is_active
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500