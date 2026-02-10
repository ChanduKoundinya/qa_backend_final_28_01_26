import logging
#import gridfs
from flask import Flask
from datetime import datetime
from werkzeug.local import LocalProxy  # 🟢 1. ADD THIS IMPORT
from pymongo import MongoClient        # 🟢 2. ADD THIS (for startup cleanup only)
from .extensions import mongo, cors, scheduler, jwt, get_fs # 🟢 3. IMPORT get_fs
from config import Config
from flask_cors import CORS


def create_app():
    # 1. Setup Logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    app = Flask(__name__)
    app.config.from_object(Config)

    web_url = app.config['WEB_URL']

    # 2. Initialize Extensions
    CORS(app, resources={r"/api/*": {
    "origins": [ "https://proud-bush-04d9e660f.4.azurestaticapps.net" ]
}},
         supports_credentials=True, 
         allow_headers=["Content-Type", "Authorization", "Access-Control-Allow-Credentials"])

    app.config["JWT_SECRET_KEY"] = "super-secret-key-change-this-in-prod"
    
    try:
        # This now calls our custom MultiTenantMongo.init_app
        mongo.init_app(app) 
        logging.info("✅ Multi-Tenant MongoDB Manager Initialized.")
    except Exception as e:
        logging.error(f"❌ Failed to initialize MongoDB: {e}")


    # 3. Initialize & Start Scheduler
    scheduler.init_app(app)
    if not scheduler.running:
        scheduler.start()
        logging.info("✅ Scheduler Started.")

    jwt.init_app(app)

    # 4. Initialize GridFS & Cleanup
    with app.app_context():
        # 🟢 CHANGE 1: DYNAMIC GRIDFS
        # We replace the static GridFS with our LocalProxy wrapper.
        # This ensures 'current_app.fs' always points to the correct tenant's storage.
        app.fs = LocalProxy(get_fs)
        
        # =========================================================
        # 🧹 SMART ZOMBIE TASK CLEANUP (MULTI-TENANT VERSION)
        # =========================================================
        # Since 'mongo.db' relies on a logged-in user (which doesn't exist during startup),
        # we must manually loop through all tenants to clean them up.
        
        tenants = app.config.get('TENANTS', {})
        
        if not tenants:
            logging.warning("⚠️ No tenants found in configuration for cleanup.")
        
        for project_code, uri in tenants.items():
            try:
                # Create a temporary connection just for this cleanup task
                # We use a context manager to ensure it closes immediately after
                with MongoClient(uri) as client:
                    db = client.get_database()
                    logging.info(f"🧹 Checking Project: {project_code}...")

                    # 1. Kill 'queued' or 'processing' tasks
                    result_active = db.tasks.update_many(
                        {'status': {'$in': ['queued', 'processing']}},
                        {'$set': {'status': 'error', 'error_message': 'System restarted while active.'}}
                    )

                    # 2. Kill 'scheduled' tasks ONLY if they are in the PAST
                    result_scheduled = db.tasks.update_many(
                        {
                            'status': 'scheduled',
                            'scheduled_for': {'$lt': datetime.now()}
                        },
                        {'$set': {'status': 'error', 'error_message': 'System restarted and missed schedule.'}}
                    )
                    
                    total = result_active.modified_count + result_scheduled.modified_count
                    if total > 0:
                        logging.warning(f"   -> Cleaned {total} tasks in {project_code}.")
            
            except Exception as e:
                logging.error(f"❌ Cleanup failed for {project_code}: {e}")
        # =========================================================
    
    # 5. Register Blueprints (No changes needed here)
    from app.modules.auth.routes import auth_bp
    from app.modules.dashboard.routes import dashboard_bp
    from app.modules.tasks.routes import tasks_bp
    from app.modules.configuration.routes import config_bp
    from app.modules.call_audit import call_audit_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(tasks_bp)
    app.register_blueprint(config_bp)
    app.register_blueprint(call_audit_bp)
    app.register_blueprint(auth_bp, url_prefix='/api/auth')

    return app