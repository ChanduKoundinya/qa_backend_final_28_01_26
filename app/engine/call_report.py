import pandas as pd
import io
import re

class CallReportEngine:
    def __init__(self):
        pass

    def _find_key_recursive(self, data, target_keys):
        """
        Recursively searches for a key (case-insensitive) in a nested dictionary or list.
        Returns the first non-empty value found.
        """
        if isinstance(data, dict):
            # 1. Check current level
            for k, v in data.items():
                if k.lower() in target_keys and v:
                    return v
            # 2. Go deeper
            for v in data.values():
                found = self._find_key_recursive(v, target_keys)
                if found: return found
        elif isinstance(data, list):
            # 3. Check items in list
            for item in data:
                found = self._find_key_recursive(item, target_keys)
                if found: return found
        return None

    def generate_excel(self, db_records, user_tz='UTC'):
        """
        Converts a list of MongoDB documents into an Excel file in memory.
        """
        all_data = []

        # 1. Process each record from MongoDB
        for doc in db_records:
            # Extract the AI analysis data
            ai_data = doc.get("full_data", {})
            
            # Prepare the row with base DB info
            row = {
                "Call ID": doc.get("filename") or doc.get("task_id"),
                "Agent Name": doc.get("agent_name", "Unknown"),       # <--- NEW
                "Call Date": doc.get("agent_audit_date", ""),
                "Overall Score": doc.get("score") or ai_data.get("Overall Score") or ai_data.get("score"),
                "Processed At": doc.get("created_at")
            }

            # 2. User Info
            user_info = (
                ai_data.get("User Info") or 
                ai_data.get("user_info") or 
                ai_data.get("user_data") or 
                {}
            )
            
            # 1. Try finding in user_info (Fastest)
            ticket_id = (
                user_info.get("ticket_id") or
                user_info.get("Ticket ID") or 
                user_info.get("TicketId") or 
                user_info.get("incident_id") or
                user_info.get("id")
            )

            # 2. If not found, try Recursive Search (Deep Search)
            if not ticket_id:
                ticket_id = self._find_key_recursive(ai_data, [
                    "ticket_id", "ticket id", "ticketid", "incident_id", "case_id", "ticket number"
                ])

            # 3. If STILL not found, use Regex Fail-Safe on Transcript
            if not ticket_id:
                transcript_text = ai_data.get("Full Transcript") or ai_data.get("transcript") or ai_data.get("diarized_transcript", "")
                if transcript_text:
                    # Regex for "INC123", "Ticket 123", allowing spaces
                    match = re.search(
                        r'(INC\s*[\d\s]{4,}|Ticket\s*(?:number|ID|#)?\s*(?:is)?\s*[\d\s]{4,})', 
                        transcript_text, 
                        re.IGNORECASE
                    )
                    if match:
                        raw_id = match.group(1)
                        ticket_id = re.sub(r'\s+', '', raw_id)

            phone = (
                user_info.get("phone") or 
                user_info.get("phone_number") or 
                user_info.get("Phone") or
                user_info.get("Contact")
            )

            row.update({
                "User Name": user_info.get("name") or user_info.get("user_name") or user_info.get("Name"),
                "User ID": user_info.get("user_id") or user_info.get("User ID"),
                "Ticket ID": ticket_id,   # <--- Updated Variable
                "Email": user_info.get("email") or user_info.get("Email"),
                "Phone": phone            # <--- Updated Variable
            })
            
            # ---------------------------------------------------------
            # 3. Audit Details Extraction
            # ---------------------------------------------------------
            audit_details = {}

            # 🟢 FIX 1: Added "Breakdown" to the list of keys to look for
            candidate_keys = [
                "Breakdown",          # <--- THIS IS THE KEY YOUR CORE SERVICE SENDS
                "Audit Results", 
                "audit_details", 
                "criteria_results", 
                "analysis", 
                "results"
            ]
            
            found_container = False
            for key in candidate_keys:
                data_node = ai_data.get(key)
                if data_node:
                    audit_details = data_node
                    found_container = True
                    break
            
            # Strategy B: Scan Root if no container found
            if not found_container:
                ignore_keys = [
                    "User Info", "user_info", "user_data", "score", "overall_score", 
                    "summary", "task_id", "filename", "Overall Score", "Transcript Snippet", 
                    "Full Transcript", "diarized_transcript"
                ]
                for k, v in ai_data.items():
                    if k not in ignore_keys and isinstance(v, dict):
                        if any(x in v for x in ["score", "Score", "status", "Status"]):
                            audit_details[k] = v

            # ---------------------------------------------------------
            # 4. List to Dict Conversion
            # ---------------------------------------------------------
            # Your Core service returns a LIST inside "Breakdown". We must convert it.
            if isinstance(audit_details, list):
                converted_details = {}
                for item in audit_details:
                    # 🟢 FIX 2: Added "Parameter" (Capital P) to support your Core Service format
                    name = (
                        item.get("Parameter") or   # <--- MATCHES CORE SERVICE
                        item.get("name") or 
                        item.get("criteria") or 
                        item.get("label")
                    )
                    if name:
                        converted_details[name] = item
                audit_details = converted_details

            # ---------------------------------------------------------
            # 5. Flatten for Excel
            # ---------------------------------------------------------
            if isinstance(audit_details, dict):
                for criteria, details in audit_details.items():
                    if isinstance(details, dict):
                        # Score
                        row[f"{criteria} (Score)"] = details.get("score") if "score" in details else details.get("Score")
                        
                        # Status
                        row[f"{criteria} (Status)"] = details.get("status") if "status" in details else details.get("Status")
                        
                        # Reason
                        reason = (
                            details.get("reason") or 
                            details.get("Reason") or 
                            details.get("justification") or 
                            details.get("observation") or
                            details.get("analysis")
                        )
                        row[f"{criteria} (Reason)"] = reason
                    else:
                        row[f"{criteria}"] = details

            all_data.append(row)

        # 6. Create DataFrame
        if not all_data:
            return None

        df = pd.DataFrame(all_data)

        # 7. Column Sorting
        fixed_cols = ["Call ID", "User Name", "User ID","Agent Name","Call Date", "Ticket ID", "Overall Score", "Processed At", "Email", "Phone"]
        existing_fixed = [c for c in fixed_cols if c in df.columns]
        dynamic_cols = [c for c in df.columns if c not in existing_fixed]
        dynamic_cols.sort()

        df = df[existing_fixed + dynamic_cols]

        # 🟢 ADD THESE TWO LINES HERE:
        df.fillna("N/A", inplace=True)       # Fixes None and NaN
        df.replace("", "N/A", inplace=True)

        date_keywords = ['date', 'time', 'processed at', 'created_at']
        potential_time_cols = [
            col for col in df.columns 
            if any(kw in col.lower() for kw in date_keywords) 
            and "(Score)" not in col # 🟢 Explicitly skip score columns
        ]
        
        for col in potential_time_cols:
            try:
                # Convert string to standard Pandas datetime, coercing errors
                temp_col = pd.to_datetime(df[col], errors='coerce')
                
                # Check if the column actually contains valid dates
                if not temp_col.isna().all():
                    # 1. Anchor to UTC if it doesn't have a timezone yet
                    if temp_col.dt.tz is None:
                        temp_col = temp_col.dt.tz_localize('UTC')
                        
                    # 2. Shift to User's Timezone
                    temp_col = temp_col.dt.tz_convert(user_tz)
                    
                    # 3. Strip timezone for Excel
                    df[col] = temp_col.dt.tz_localize(None)
            except Exception:
                pass

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Audit Report')
        
        output.seek(0)
        return output