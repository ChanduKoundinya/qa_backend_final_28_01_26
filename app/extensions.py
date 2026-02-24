# app/extensions.py
from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler
from flask_jwt_extended import JWTManager
from flask_cors import CORS
from flask_bcrypt import Bcrypt

# Initialize SQLAlchemy (Replaces mongo)
db = SQLAlchemy()

# Initialize other extensions
scheduler = APScheduler()
jwt = JWTManager()
cors = CORS()
bcrypt = Bcrypt()