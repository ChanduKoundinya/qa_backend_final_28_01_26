from flask import Blueprint

# Define the Blueprint
dashboard_bp = Blueprint('dashboard', __name__)

# Import routes to ensure they are registered
from . import routes