# app/utils/email_service.py

import os
import base64
import mimetypes
import logging
from email.message import EmailMessage
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def send_audit_email(recipient_email, subject, body_text, file_path):
    """Sends an email with an attachment using Gmail API."""
    
    token_path = os.path.join(os.path.dirname(__file__), '../../gmail_refresh_token.json')
    
    # 1. Load the credentials from your token file
    if not os.path.exists(token_path):
        logging.error("Gmail token file not found.")
        return False

    try:
        # Load credentials using the exact scopes defined in your JSON
        creds = Credentials.from_authorized_user_file(
            token_path, 
            scopes=["https://www.googleapis.com/auth/gmail.modify"]
        )

        # 2. Build the Gmail API service
        service = build('gmail', 'v1', credentials=creds)

        # 3. Construct the Email
        message = EmailMessage()
        message.set_content(body_text)
        message['To'] = recipient_email
        message['From'] = 'me'  # 'me' defaults to the authenticated Gmail account
        message['Subject'] = subject

        # 4. Attach the file (Excel or Word doc)
        if os.path.exists(file_path):
            type_subtype, _ = mimetypes.guess_type(file_path)
            if type_subtype is None:
                type_subtype = 'application/octet-stream'
            maintype, subtype = type_subtype.split('/', 1)
            
            with open(file_path, 'rb') as fp:
                attachment_data = fp.read()
                
            message.add_attachment(
                attachment_data, 
                maintype=maintype, 
                subtype=subtype, 
                filename=os.path.basename(file_path)
            )
        else:
            logging.warning(f"Attachment not found at {file_path}")

        # 5. Encode and Send
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {'raw': encoded_message}

        send_message = service.users().messages().send(userId="me", body=create_message).execute()
        logging.info(f"✅ Audit Email successfully sent! Message ID: {send_message['id']}")
        return True

    except HttpError as error:
        logging.error(f"❌ Failed to send email via Gmail API: {error}")
        return False