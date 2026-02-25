import os
import urllib.parse
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'), override=True)

class Config:
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL") or \
        "postgresql://alex:AbC123xyz@ep-cool-darkness-123456.us-east-2.aws.neon.tech/neondb?sslmode=require"

    SECRET_KEY = os.getenv("SECRET_KEY", "dev_key")
    CORE_SERVICE_URL = os.getenv("CORE_SERVICE_URL")
    WEB_URL = os.getenv("WEB_URL")

    SQLALCHEMY_ENGINE_OPTIONS = {
        # Tests the connection by running a simple "SELECT 1" before executing your real query
        "pool_pre_ping": True, 
        
        # Recycles connections every 5 minutes (300 seconds) so they never sit idle long enough for Neon to kill them
        "pool_recycle": 300,   
    }
    
    SQLALCHEMY_TRACK_MODIFICATIONS = False