import os
import urllib.parse
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'), override=True)

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev_key")
    CORE_SERVICE_URL = os.getenv("CORE_SERVICE_URL")
    WEB_URL = os.getenv("WEB_URL")

    # 🟢 POSTGRESQL CONFIGURATION
    DB_USER = os.getenv("DB_USER", "postgres")
    # Wrap password in quote_plus to safely handle special characters like @
    DB_PASSWORD = urllib.parse.quote_plus(os.getenv("DB_PASSWORD", "Aibots@12345"))
    DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "bot_db")

    # The string SQLAlchemy uses to connect
    SQLALCHEMY_DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False