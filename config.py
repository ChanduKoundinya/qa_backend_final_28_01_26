import os
import urllib.parse
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'), override=True)


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev_key")
    CORE_SERVICE_URL = os.getenv("CORE_SERVICE_URL")
    WEB_URL = os.getenv("WEB_URL")

    MONGO_URI_CENTRAL = os.getenv("MONGO_URI_CENTRAL")

    # 🟢 FIX: Match the dictionary keys to your REAL project codes
    TENANTS = {
        "bcbsa": os.getenv("MONGO_URI_BCBSA"),       # Matches .env
        "tuffs": os.getenv("MONGO_URI_TUFFS"),       # Matches .env
        "project3": os.getenv("MONGO_URI_PROJECT3")  # Matches .env
    }

    # Debugging: Print to console on startup to verify (remove in production)
    print(f"Loaded Tenants: {list(TENANTS.keys())}")
    if None in TENANTS.values():
        print("❌ CRITICAL ERROR: One or more Mongo URIs failed to load from .env!")