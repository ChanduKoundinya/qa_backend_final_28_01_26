from flask import Blueprint, request, jsonify
from app.extensions import mongo, jwt  # Assuming you init JWTManager in extensions
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import (
    create_access_token, 
    jwt_required, 
    get_jwt_identity, 
    get_jwt,
    unset_jwt_cookies
)
from datetime import datetime, timedelta

auth_bp = Blueprint('auth', __name__)

# --- 1. REGISTER ENDPOINT ---
@auth_bp.route('/register', methods=['POST'])
def register():
    """
    Creates a new user with a specific role.
    Payload: { "username": "John", "email": "john@test.com", "password": "123", "role": "admin" }
    """
    try:
        data = request.get_json()
        
        # 1. Validate Input
        required_fields = ['username', 'email', 'password', 'role']
        if not all(field in data for field in required_fields):
            return jsonify({"error": "Missing required fields"}), 400

        # 2. Check if user already exists
        if mongo.db.users.find_one({"email": data['email']}):
            return jsonify({"error": "Email already registered"}), 409

        # 3. Hash Password (NEVER save plain text passwords)
        hashed_password = generate_password_hash(data['password'])

        # 4. Create User Document
        new_user = {
            "username": data['username'],
            "email": data['email'],
            "password": hashed_password,
            "role": data['role'],  # e.g., 'admin', 'auditor', 'viewer'
            "created_at": datetime.utcnow()
        }

        # 5. Save to MongoDB
        mongo.db.users.insert_one(new_user)

        return jsonify({"message": "User created successfully"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- 2. LOGIN ENDPOINT ---
@auth_bp.route('/login', methods=['POST'])
def login():
    """
    Authenticates user and returns a JWT token.
    Payload: { "email": "john@test.com", "password": "123" }
    """
    try:
        data = request.get_json()

        # 1. Find User by Email
        user = mongo.db.users.find_one({"email": data.get('email')})

        if not user:
            return jsonify({"error": "Invalid credentials"}), 401

        # 2. Verify Password
        if not check_password_hash(user['password'], data.get('password')):
            return jsonify({"error": "Invalid credentials"}), 401

        # 3. Generate JWT Token
        # We embed the user ID and ROLE into the token so the frontend knows permissions immediately.
        # Identity identifies *who* they are. Additional claims identify *what* they are.
        access_token = create_access_token(
            identity=str(user['_id']), 
            additional_claims={"role": user['role'], "username": user['username']},
            expires_delta=timedelta(hours=12) # Token valid for 12 hours
        )

        return jsonify({
            "message": "Login successful",
            "access_token": access_token,
            "user": {
                "username": user['username'],
                "email": user['email'],
                "role": user['role']
            }
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