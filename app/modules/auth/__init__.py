from flask import Blueprint

# Define the Blueprint
auth_bp = Blueprint('auth', __name__)

# Import routes to ensure they are registered
from . import routes