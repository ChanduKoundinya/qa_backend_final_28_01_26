# app/utils/email_service.py

import os
import base64
import mimetypes
from datetime import datetime
import logging
from email.message import EmailMessage
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from app.models import ApiConfig, User

def send_audit_email(recipient_email, subject, body_text, file_paths=None, body_html=None):
    """Sends an email with optional HTML body and optional attachments."""
    token_path = os.path.join(os.path.dirname(__file__), '../../gmail_refresh_token.json')
    
    if not os.path.exists(token_path):
        logging.error("Gmail token file not found.")
        return False

    try:
        creds = Credentials.from_authorized_user_file(token_path, scopes=["https://www.googleapis.com/auth/gmail.modify"])
        service = build('gmail', 'v1', credentials=creds)

        message = EmailMessage()
        message['Subject'] = subject
        message['From'] = 'me'
        
        # Support sending to multiple people (Distribution list)
        if isinstance(recipient_email, list):
            message['To'] = ", ".join(recipient_email)
        else:
            message['To'] = recipient_email

        # Set plain text, and override with HTML if provided
        message.set_content(body_text)
        if body_html:
            message.add_alternative(body_html, subtype='html')

        # Safely handle attachments if they exist
        if file_paths:
            if isinstance(file_paths, str):
                file_paths = [file_paths]
                
            for file_path in file_paths:
                if file_path and os.path.exists(file_path):
                    type_subtype, _ = mimetypes.guess_type(file_path)
                    type_subtype = type_subtype or 'application/octet-stream'
                    maintype, subtype = type_subtype.split('/', 1)
                    
                    with open(file_path, 'rb') as fp:
                        message.add_attachment(fp.read(), maintype=maintype, subtype=subtype, filename=os.path.basename(file_path))

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        service.users().messages().send(userId="me", body={'raw': encoded_message}).execute()
        return True

    except HttpError as error:
        logging.error(f"❌ Failed to send email via Gmail API: {error}")
        return False
    
def trigger_automated_email(task, project_code, file_paths):
    """
    Checks toggle status, validates user, and sends the email.
    """
    try:
        # 1. Check Toggle Status
        config = ApiConfig.query.filter_by(name="email_notifications", project_code=project_code).first()
        if config and config.key == "false":
            logging.info(f"📝 Audit Trail [Task {task.id}]: Email skipped (Toggle is OFF).")
            return

        # 2. Dynamic Recipient Logic: Find the initiator
        uploader = User.query.filter_by(username=task.created_by).first()
        
        # 3. System Validation: Ensure initiator has a valid email
        if not uploader or not getattr(uploader, 'email', None) or "@" not in uploader.email:
            logging.error(f"❌ Audit Trail [Task {task.id}]: Missing/Invalid email for user '{task.created_by}'. Email aborted.")
            return 

        recipient = uploader.email
        subject = f"Audit Completed - Task #{task.id} ({task.analysis_type})"
        body = f"Hello {uploader.username},\n\nYour audit for task #{task.id} has completed successfully. Please find the results attached."

        # 4. Dispatch via Gmail
        email_sent = send_audit_email(
            recipient_email=recipient,
            subject=subject,
            body_text=body,
            file_paths=file_paths
        )

        # 5. Audit Trail: Log the event
        if email_sent:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logging.info(f"✅ Audit Trail [Task {task.id}]: Email sent to {recipient} at {timestamp}.")
        else:
            logging.warning(f"⚠️ Audit Trail [Task {task.id}]: Gmail service failed to send to {recipient}.")

    except Exception as e:
        logging.error(f"❌ Audit Trail [Task {task.id}]: Critical error in trigger_automated_email: {e}")