import logging
from flask import Flask
from datetime import datetime, timezone, timedelta
from flask_cors import CORS
from flask_jwt_extended import JWTManager

from config import Config
# 🟢 1. IMPORT 'db' instead of 'mongo' or 'get_fs'
from .extensions import db, cors, scheduler, jwt
from app.models import Task  # Needed for cleanup script

def create_app():
    # 1. Setup Logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    app = Flask(__name__)
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(days=1)
    jwt = JWTManager(app)
    app.config.from_object(Config)

    web_url = app.config['WEB_URL']

    # 2. Initialize Extensions
    CORS(app, resources={r"/api/*": {
    "origins": [ "https://proud-bush-04d9e660f.4.azurestaticapps.net" ]
}},
         supports_credentials=True, 
         allow_headers=["Content-Type", "Authorization", "Access-Control-Allow-Credentials"])

    app.config["JWT_SECRET_KEY"] = "super-secret-key-change-this-in-prod"
    
    # 🟢 2. INITIALIZE SQLALCHEMY
    try:
        db.init_app(app) 
        logging.info("✅ PostgreSQL Database Initialized.")
    except Exception as e:
        logging.error(f"❌ Failed to initialize Database: {e}")

    # 3. Initialize & Start Scheduler
    scheduler.init_app(app)
    if not scheduler.running:
        scheduler.start()
        logging.info("✅ Scheduler Started.")

    jwt.init_app(app)

    # 4. Startup Cleanup (Much simpler in Postgres!)
    with app.app_context():
        try:
            logging.info("🧹 Running Startup Task Cleanup...")
            
            # Find tasks stuck in 'queued' or 'processing'
            active_tasks = Task.query.filter(Task.status.in_(['queued', 'processing'])).all()
            for task in active_tasks:
                task.status = 'error'
                task.error_message = 'System restarted while active.'
            
            # Find missed 'scheduled' tasks
            now_utc = datetime.now(timezone.utc)
            missed_tasks = Task.query.filter(Task.status == 'scheduled', Task.scheduled_for < now_utc).all()
            for task in missed_tasks:
                task.status = 'error'
                task.error_message = 'System restarted and missed schedule.'
            
            # Commit if we changed anything
            if active_tasks or missed_tasks:
                db.session.commit()
                logging.warning(f"   -> Cleaned {len(active_tasks) + len(missed_tasks)} zombie tasks.")
            else:
                logging.info("   -> No zombie tasks found.")
                
        except Exception as e:
            db.session.rollback()
            logging.error(f"❌ Startup cleanup failed: {e}")

    # 5. Register Blueprints 
    # ⚠️ WARNING: See notes below about these!
    from app.modules.auth.routes import auth_bp
    from app.modules.dashboard.routes import dashboard_bp
    from app.modules.tasks.routes import tasks_bp
    from app.modules.configuration.routes import config_bp
    from app.modules.call_audit.routes import call_audit_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(tasks_bp)
    app.register_blueprint(config_bp)
    app.register_blueprint(call_audit_bp)
    app.register_blueprint(auth_bp, url_prefix='/api/auth')

    return app