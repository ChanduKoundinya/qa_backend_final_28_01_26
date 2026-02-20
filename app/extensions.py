# app/extensions.py
from flask import current_app, g, request
from werkzeug.local import LocalProxy
from flask_pymongo import PyMongo
from flask_cors import CORS
from flask_apscheduler import APScheduler
from flask_jwt_extended import JWTManager, get_jwt, verify_jwt_in_request
from pymongo import MongoClient
import gridfs

cors = CORS(expose_headers=["Content-Disposition"])
scheduler = APScheduler()
jwt = JWTManager()

class MultiTenantMongo:
    def __init__(self):
        self.clients = {}  # Store active connections { 'project_a': MongoClient(...) }
        self.central_client = None

    def init_app(self, app):
        """
        Initialize connections for ALL tenants defined in Config.
        """
        tenants = app.config.get("TENANTS", {})
        for project_code, uri in tenants.items():
            try:
                # Create a persistent connection for each project
                client = MongoClient(uri)
                # Verify connection
                client.server_info()
                self.clients[project_code] = client
                print(f"✅ Connected to Tenant DB: {project_code}")
            except Exception as e:
                print(f"❌ Failed to connect to {project_code}: {e}")
        # 2. 🟢 Initialize Central Auth DB
        central_uri = app.config.get("MONGO_URI_CENTRAL")
        if central_uri:
            try:
                self.central_client = MongoClient(central_uri)
                print("✅ Central Auth DB connected.")
            except Exception as e:
                print(f"❌ Central DB failed: {e}")

    def get_active_db(self):
        """
        Decides which DB to use based on the current context.
        """
        # 1. Check if we manually set a tenant (e.g., during Login/Register)
        if hasattr(g, 'current_tenant'):
            return self.clients.get(g.current_tenant).get_database()

        # 2. Otherwise, check the JWT Token
        try:
            verify_jwt_in_request(optional=True) # Check if token exists
            claims = get_jwt()
            if claims and 'project' in claims:
                project_code = claims['project']
                if project_code in self.clients:
                    return self.clients[project_code].get_database()
        except:
            pass
        
        # 3. Fallback (Optional: return a default DB or None)
        return None

    def get_central_db(self):
        if self.central_client:
            return self.central_client.get_database()
        return None
    
    def get_active_fs(self):
        """
        Returns the GridFS bucket for the ACTIVE database.
        """
        db = self.get_active_db()
        if db is not None:
            return gridfs.GridFS(db)
        return None

# Instantiate the Manager
tenant_manager = MultiTenantMongo()

# --- THE MAGIC PROXY ---
# Wherever you use 'mongo.db' in your code, it now calls 'tenant_manager.get_active_db()'
# Wherever you use 'current_app.fs', we will swap it to use this proxy logic too.
mongo = type('MongoProxy', (), {})() # Empty object to attach properties
mongo.db = LocalProxy(tenant_manager.get_active_db)
mongo.central_db = LocalProxy(tenant_manager.get_central_db)
mongo.init_app = tenant_manager.init_app

# Helper for GridFS (We will attach this to app later)
def get_fs():
    return tenant_manager.get_active_fs()