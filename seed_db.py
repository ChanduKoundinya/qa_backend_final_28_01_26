from app import create_app
from app.models import db, ApiConfig
from datetime import datetime, timezone

app = create_app()

def get_utc_now():
    return datetime.now(timezone.utc)

# The project code to assign these configurations to
PROJECT_CODE = "bcbsa" 

with app.app_context():
    print("🚀 Starting Configuration Migration to PostgreSQL...")

    # --- 1. OPENAI API KEY ---
    openai_cfg = ApiConfig.query.filter_by(name="openai_api_key", project_code=PROJECT_CODE).first()
    if not openai_cfg:
        openai_cfg = ApiConfig(
            name="openai_api_key",
            category="LLM",
            key="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",
            project_code=PROJECT_CODE
        )
        db.session.add(openai_cfg)
        print("✅ Added OpenAI API Key.")

    # --- 2. ITSM TOOLS ---
    itsm_tools = [
        {
            "tool_id": "550e8400-e29b-41d4-a716-446655440000",
            "tool_name": "ServiceNow",
            "instance_url": "https://api.com",
            "credentials": {"password": "super_secret_password_123", "username": "SNS"},
            "sync_scheduler": {"frequency": "monthly", "settings": {"date": "12", "day": None, "time": "22:22"}}
        },
        {
            "tool_id": "771f9500-a33c-45d5-b829-998877660000",
            "tool_name": "Jira Service Management",
            "instance_url": "http://sample.com",
            "credentials": {"password": None, "username": "jira_bot"},
            "sync_scheduler": {"frequency": "daily", "settings": {"date": None, "day": None, "time": "09:30"}},
            "created_at": "2023-10-27T10:05:00Z"
        },
        {
            "tool_id": "882a4400-c55d-46e6-c930-112233440000",
            "tool_name": "Zendesk",
            "instance_url": "https://support.zendesk.com",
            "credentials": {"password": "another_secure_password", "username": "support_admin"},
            "sync_scheduler": {"frequency": "monthly", "settings": {"date": 15, "time": "06:00"}}
        },
        {
            "tool_id": "4c6d7a7e-72ea-4543-a5f1-f7a27f110ba9",
            "tool_name": "PagerDuty",
            "instance_url": "https://api.pagerduty.com",
            "credentials": {"password": None, "username": "Api"},
            "sync_scheduler": {"frequency": "weekly", "settings": {"date": None, "day": "Monday", "time": "22:59"}}
        },
        {
            "tool_id": "dbe00cb5-b66e-4f39-81f4-9d8204fc1ab7",
            "tool_name": "Jira Service Desk",
            "instance_url": "https://company.atlassian.net",
            "credentials": {"username": "atlassian", "password": None},
            "sync_scheduler": {"frequency": "weekly", "settings": {"time": "22:59", "day": "Monday", "date": None}}
        },
        {
            "tool_id": "7dd6006c-b838-46ef-9b22-21ed262cabd5",
            "tool_name": "Twilio",
            "instance_url": "https://api.twilio.com",
            "credentials": {"password": "super_secret_password_123", "username": "SID_123456"},
            "sync_scheduler": {"frequency": "weekly", "settings": {"date": None, "day": "Sunday", "time": "01:00"}}
        }
    ]

    itsm_cfg = ApiConfig.query.filter_by(category="ITSM", project_code=PROJECT_CODE).first()
    if not itsm_cfg:
        itsm_cfg = ApiConfig(
            category="ITSM",
            tools=itsm_tools,
            project_code=PROJECT_CODE,
            updated_at=datetime.fromisoformat("2026-02-18T07:56:41.167").replace(tzinfo=timezone.utc)
        )
        db.session.add(itsm_cfg)
        print("✅ Added ITSM Tools.")

    # --- 3. CALL RECORDING TOOLS ---
    call_tools = [
        {
            "tool_id": "999e8400-e29b-41d4-a716-446655449999",
            "tool_name": "Zoom Phone",
            "instance_url": "https://api.zoom.us",
            "credentials": {"username": "admin", "password": None, "api_token": "zoom_jwt_token_here"},
            "sync_scheduler": {"frequency": "weekly", "settings": {"day": "Tuesday", "time": "10:30"}}
        },
        {
            "tool_id": "888f9500-a33c-45d5-b829-998877668888",
            "tool_name": "Twilio",
            "instance_url": "https://api.twilio.com",
            "credentials": {"username": "SID_12345", "password": "auth_token_secret"},
            "sync_scheduler": {"frequency": "weekly", "settings": {"day": "Sunday", "time": "01:00"}},
            "created_at": "2023-11-01T10:05:00Z"
        }
    ]

    call_cfg = ApiConfig.query.filter_by(category="Call Recording", project_code=PROJECT_CODE).first()
    if not call_cfg:
        call_cfg = ApiConfig(
            category="Call Recording",
            tools=call_tools,
            project_code=PROJECT_CODE,
            updated_at=datetime.fromisoformat("2026-01-22T14:08:20.418").replace(tzinfo=timezone.utc)
        )
        db.session.add(call_cfg)
        print("✅ Added Call Recording Tools.")

    db.session.commit()
    print("🏁 Migration Complete!")