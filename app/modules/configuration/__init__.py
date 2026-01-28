from flask import Blueprint

# Define the Blueprint
config_bp = Blueprint('configuration', __name__)

# Import routes to ensure they are registered
from . import routes