from flask import Blueprint

# Define the Blueprint
call_audit_bp = Blueprint('call_audit', __name__)

# Import routes to register them with the blueprint
from . import routes