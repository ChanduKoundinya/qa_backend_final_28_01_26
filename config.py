import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev_key")
    CORE_SERVICE_URL = os.getenv("CORE_SERVICE_URL")

    # --- MongoDB Connection Logic ---
    MONGO_USERNAME = os.getenv("MONGO_USERNAME")
    MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
    MONGO_HOSTNAME = os.getenv("MONGO_HOSTNAME")
    DB_NAME = os.getenv("DB_NAME")

    if all([MONGO_USERNAME, MONGO_PASSWORD, MONGO_HOSTNAME, DB_NAME]):
        encoded_password = urllib.parse.quote_plus(MONGO_PASSWORD)
        MONGO_URI = f"mongodb+srv://{MONGO_USERNAME}:{encoded_password}@{MONGO_HOSTNAME}/{DB_NAME}?retryWrites=true&w=majority"
    else:
        # This will cause PyMongo to fail loudly if variables are missing, which is good
        raise ValueError("Missing MongoDB configuration variables. App cannot start.")
