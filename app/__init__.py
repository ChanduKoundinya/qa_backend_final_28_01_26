import logging
import gridfs
from flask import Flask
from datetime import datetime
from .extensions import mongo, cors, scheduler, jwt
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
        mongo.init_app(app)
        with app.app_context():
            mongo.cx.server_info()
        logging.info("✅ Connected to MongoDB.")
    except Exception as e:
        logging.error(f"❌ Failed to connect to MongoDB: {e}")

    # 3. Initialize & Start Scheduler
    scheduler.init_app(app)
    if not scheduler.running:
        scheduler.start()
        logging.info("✅ Scheduler Started.")

    jwt.init_app(app)

    # 4. Initialize GridFS & Cleanup
    with app.app_context():
        app.fs = gridfs.GridFS(mongo.db)
        
        # =========================================================
        # 🧹 SMART ZOMBIE TASK CLEANUP (Corrected)
        # =========================================================
        
        # 1. Kill 'queued' or 'processing' tasks
        # These should be running NOW. If the server is restarting, they are dead.
        result_active = mongo.db.tasks.update_many(
            {'status': {'$in': ['queued', 'processing']}},
            {'$set': {'status': 'error', 'error_message': 'System restarted while active.'}}
        )

        # 2. Kill 'scheduled' tasks ONLY if they are in the PAST
        # This preserves valid future tasks.
        result_scheduled = mongo.db.tasks.update_many(
            {
                'status': 'scheduled',
                'scheduled_for': {'$lt': datetime.now()} # Only clean if time < now
            },
            {'$set': {'status': 'error', 'error_message': 'System restarted and missed schedule.'}}
        )
        
        # Logging results
        total_cleaned = result_active.modified_count + result_scheduled.modified_count
        if total_cleaned > 0:
            logging.warning(f"🧹 Cleaned up {total_cleaned} zombie tasks ({result_active.modified_count} active, {result_scheduled.modified_count} missed schedule).")
        else:
            logging.info("✅ No zombie tasks found.")
        # =========================================================
    

    # 5. Register Blueprints
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
