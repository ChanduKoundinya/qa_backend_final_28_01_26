from flask_pymongo import PyMongo
from flask_cors import CORS
from flask_apscheduler import APScheduler # <--- 🟢 CHANGE THIS
from flask_jwt_extended import JWTManager
import gridfs

# Initialize extensions
mongo = PyMongo()
cors = CORS()
scheduler = APScheduler() # <--- 🟢 This class has .init_app(app)
jwt = JWTManager()

# GridFS placeholder (Correct)
# We will bind this to the database in your app factory (__init__.py)
fs = None