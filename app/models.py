# app/models.py
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime, timezone

from app.extensions import db

def get_utc_now():
    return datetime.now(timezone.utc)

# 1. The GridFS Replacement (BYTEA)
class StoredFile(db.Model):
    __tablename__ = 'stored_files'
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    mimetype = db.Column(db.String(100))
    file_data = db.Column(db.LargeBinary, nullable=False) # 🟢 BYTEA column
    project_code = db.Column(db.String(50), nullable=False) # Multi-tenant isolation
    created_at = db.Column(db.DateTime, default=get_utc_now)

# 2. Users & Auth (Previously central_db)
class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(50), nullable=False)
    project_code = db.Column(db.String(50), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=get_utc_now)

class RefreshToken(db.Model):
    __tablename__ = 'refresh_tokens'
    id = db.Column(db.Integer, primary_key=True)
    jti = db.Column(db.String(120), unique=True, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    created_at = db.Column(db.DateTime, default=get_utc_now)
    expires_at = db.Column(db.DateTime, nullable=False)
    device = db.Column(db.String(255))

# 3. Tasks
class Task(db.Model):
    __tablename__ = 'tasks'
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255))
    status = db.Column(db.String(50), default='queued')
    analysis_type = db.Column(db.String(50))
    audit_category = db.Column(db.String(50))
    
    # 🟢 File Relationships (Replacing GridFS ObjectIds)
    input_file_id = db.Column(db.Integer, db.ForeignKey('stored_files.id'))
    output_excel_id = db.Column(db.Integer, db.ForeignKey('stored_files.id'))
    output_docx_id = db.Column(db.Integer, db.ForeignKey('stored_files.id'))
    
    # Batch Call Tracking (Stored dynamically as JSONB)
    files_tracker = db.Column(JSONB) 
    total_files = db.Column(db.Integer, default=1)
    completed_count = db.Column(db.Integer, default=0)
    
    error_message = db.Column(db.Text)
    created_by = db.Column(db.String(80))
    user_tz = db.Column(db.String(50))
    project_code = db.Column(db.String(50), nullable=False)
    
    created_at = db.Column(db.DateTime, default=get_utc_now)
    scheduled_for = db.Column(db.DateTime)
    completed_at = db.Column(db.DateTime)

# 4. Results & Configurations
class AuditReport(db.Model):
    __tablename__ = 'audit_reports'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.Integer, db.ForeignKey('tasks.id'))
    audit_category = db.Column(db.String(50))
    full_data = db.Column(JSONB) # 🟢 Stores your dynamic Pandas row data
    project_code = db.Column(db.String(50), nullable=False)

class CallAuditResult(db.Model):
    __tablename__ = 'call_audit_results'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.Integer, db.ForeignKey('tasks.id'))
    filename = db.Column(db.String(255))
    agent_name = db.Column(db.String(100))
    agent_audit_date = db.Column(db.String(50))
    full_data = db.Column(JSONB) # 🟢 Stores the AI evaluation JSON
    created_at = db.Column(db.DateTime, default=get_utc_now)
    project_code = db.Column(db.String(50), nullable=False)

class Criterion(db.Model):
    __tablename__ = 'criteria'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text)
    type = db.Column(db.String(50))
    weight = db.Column(db.Float, default=1.0)
    is_active = db.Column(db.Boolean, default=True)
    last_modified_by_role = db.Column(db.String(50))
    project_code = db.Column(db.String(50), nullable=False)
    created_at = db.Column(db.DateTime, default=get_utc_now)
    updated_at = db.Column(db.DateTime, default=get_utc_now, onupdate=get_utc_now)

class PiiLog(db.Model):
    __tablename__ = 'pii_logs'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.String(100))
    timestamp = db.Column(db.DateTime, default=get_utc_now)
    status = db.Column(db.String(50))
    pii_found = db.Column(db.Boolean)
    detection_stats = db.Column(JSONB)
    processed_data_preview = db.Column(JSONB)
    project_code = db.Column(db.String(50), nullable=False)

# 5. Configurations & API Integrations
class ApiConfig(db.Model):
    __tablename__ = 'api_configs'
    id = db.Column(db.Integer, primary_key=True)
    
    # E.g., 'openai_api_key' or 'jwt_settings'
    name = db.Column(db.String(100)) 
    
    # E.g., 'ITSM'
    category = db.Column(db.String(100)) 
    
    # For simple string values like the OpenAI key
    key = db.Column(db.String(255)) 
    
    # 🟢 JSONB: Stores your dynamic array of integration tools and credentials
    tools = db.Column(JSONB) 
    
    # Multi-tenant isolation
    project_code = db.Column(db.String(50), nullable=False) 
    
    updated_at = db.Column(db.DateTime, default=get_utc_now, onupdate=get_utc_now)
    created_at = db.Column(db.DateTime, default=get_utc_now)

# 6. Incident Reporting
class IncidentResult(db.Model):
    __tablename__ = 'incident_results'
    id = db.Column(db.Integer, primary_key=True)
    
    # Link back to the Task
    task_id = db.Column(db.Integer, db.ForeignKey('tasks.id'))
    
    # Link to the generated Excel file in the StoredFile table (BYTEA)
    report_file_id = db.Column(db.Integer, db.ForeignKey('stored_files.id'))
    
    file_name = db.Column(db.String(255))
    project_code = db.Column(db.String(50), nullable=False)
    generated_at = db.Column(db.DateTime, default=get_utc_now)
