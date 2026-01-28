from functools import wraps
from flask import jsonify
from flask_jwt_extended import verify_jwt_in_request, get_jwt

def role_required(allowed_roles):
    """
    Decorator to restrict access to specific roles.
    Usage: @role_required(['superadmin', 'admin'])
    """
    def wrapper(fn):
        @wraps(fn)
        def decorator(*args, **kwargs):
            # 1. Verify the JWT is present and valid
            try:
                verify_jwt_in_request()
            except Exception as e:
                return jsonify({"error": "Invalid or missing token"}), 401

            # 2. Get claims from the token
            claims = get_jwt()
            user_role = claims.get('role', '').lower() # Normalize to lowercase

            # 3. Check if the user's role is in the allowed list
            if user_role not in [r.lower() for r in allowed_roles]:
                return jsonify({
                    "error": "Access Denied: Insufficient permissions",
                    "required_roles": allowed_roles,
                    "your_role": user_role
                }), 403

            return fn(*args, **kwargs)
        return decorator
    return wrapper