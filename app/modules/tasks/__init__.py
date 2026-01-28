from flask import Blueprint

# Define the Blueprint
tasks_bp = Blueprint('tasks', __name__)

# Import routes to ensure they are registered with the Blueprint
from . import routes