import pandas as pd
from datetime import datetime
import io 
import warnings

# Silence warnings as per your original code
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

def generate_incident_report(df: pd.DataFrame, active_features: list):
    """
    Core Logic: Takes a DataFrame, runs math, and returns an Excel file in memory.
    Refactored from your standalone Incident Reporting Tool.
    """
    current_date = datetime.now()
    
    # --- 1. Data Cleaning & Safety Checks (Preserved from Original) ---
    if 'Closed Time' not in df.columns:
        df['Closed Time'] = pd.NaT

    time_cols = ['Closed Time', 'Created Time', 'Due by Time', 'Resolved Time', 'Last Updated Time']
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Convert Numeric Columns (Time in Hrs)
    for col in ['First Response Time (in Hrs)', 'Resolution Time (in Hrs)']:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.extract(r'(\d+\.?\d*)', expand=False), 
                errors='coerce'
            )

    # Global Text Cleaning
    text_cols = ['Priority', 'Status', 'Type', 'Group', 'Agent', 'Description', 'Subject', 'Category']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()
            
            if col == 'Priority' or col == 'Status':
                df[col] = df[col].str.title()
            
    if 'Agent' in df.columns:
        df.loc[(df['Agent'] == '') | (df['Agent'].isnull()), 'Agent'] = 'Unassigned'

    # FCR Calculation
    if 'Agent interactions' in df.columns and 'Status' in df.columns:
        df['FCR'] = ((df['Agent interactions'] == 1) & (df['Status'] == 'Resolved')).astype(int)
    else:
        df['FCR'] = 0

    df_open = df[df['Status'] == 'Open'].copy() if 'Status' in df.columns else pd.DataFrame()
    df_closed = df[df['Closed Time'].notnull()].copy()

    # --- 2. Excel Writer Setup ---
    output = io.BytesIO()
    # Note: Ensure 'xlsxwriter' is installed in your Core Service pip requirements
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    workbook = writer.book

    def add_chart(sheet_name, df_chart, val_col, title, chart_type='bar'):
        if df_chart.empty: return
        
        worksheet = writer.sheets[sheet_name]
        chart = workbook.add_chart({'type': chart_type})
        chart.add_series({
            'categories': [sheet_name, 1, 0, 1 + len(df_chart) - 1, 0],
            'values':     [sheet_name, 1, val_col, 1 + len(df_chart) - 1, val_col],
        })
        chart.set_title({'name': title})
        worksheet.insert_chart('G2', chart)

    # --- 3. Analysis Features (1-10) ---
    
    # 1. Top Talkers
    if '1' in active_features and {'Type', 'Priority', 'Ticket Id'}.issubset(df.columns):
        data = df.groupby(['Type', 'Priority'])['Ticket Id'].count().reset_index(name='Count').sort_values('Count', ascending=False)
        data.to_excel(writer, sheet_name='1_Top_Talkers', index=False)
        add_chart('1_Top_Talkers', data, 2, 'Top Types by Priority')

    # 2. Closed Trend
    if '2' in active_features and not df_closed.empty:
        df_closed['Month'] = df_closed['Closed Time'].dt.to_period('M').astype(str)
        data = df_closed.groupby('Month')['Ticket Id'].count().reset_index(name='Count')
        data.to_excel(writer, sheet_name='2_Closed_Trend', index=False)
        add_chart('2_Closed_Trend', data, 1, 'Closed Ticket Trend', 'line')

    # 3. Agent Rank
    if '3' in active_features and {'Group', 'Agent', 'Ticket Id'}.issubset(df.columns):
        data = df.groupby(['Group', 'Agent'])['Ticket Id'].count().reset_index(name='Count')
        data.to_excel(writer, sheet_name='3_Agent_Rank', index=False)
        add_chart('3_Agent_Rank', data, 2, 'Agent Volume by Group')

    # 4. Avg Response
    if '4' in active_features and 'Group' in df.columns and 'First Response Time (in Hrs)' in df.columns:
        data = df.groupby('Group')['First Response Time (in Hrs)'].mean().reset_index(name='Hours')
        data.to_excel(writer, sheet_name='4_Response_Time', index=False)
        add_chart('4_Response_Time', data, 1, 'Avg Response Time')

    # 5. FCR
    if '5' in active_features and {'Agent', 'Ticket Id'}.issubset(df.columns):
        data = df.groupby('Agent').agg({'FCR': 'mean'}).reset_index()
        data.to_excel(writer, sheet_name='5_FCR_Analysis', index=False)
        add_chart('5_FCR_Analysis', data, 1, 'FCR Rate by Agent')

    # 6. Aging
    if '6' in active_features and not df_open.empty and 'Created Time' in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df_open['Created Time']):
            df_open['Days'] = (current_date - df_open['Created Time']).dt.days
            data = df_open[['Ticket Id', 'Days']].sort_values('Days', ascending=False).head(20)
            data.to_excel(writer, sheet_name='6_Aging', index=False)
            add_chart('6_Aging', data, 1, 'Top 20 Oldest Tickets')

    # 7. MTTR
    mttr_overall = 0
    # FIX: Added "and 'Group' in df.columns" to prevent crash if Group is missing
    if '7' in active_features and 'Resolution Time (in Hrs)' in df.columns and 'Group' in df.columns:
        mean_val = df['Resolution Time (in Hrs)'].mean()
        mttr_overall = 0 if pd.isna(mean_val) else mean_val
        
        data = df.groupby('Group')['Resolution Time (in Hrs)'].mean().reset_index(name='Hours')
        data.to_excel(writer, sheet_name='7_MTTR', index=False)
        add_chart('7_MTTR', data, 1, 'MTTR by Group')

    # 8. Open Trend
    if '8' in active_features and 'Created Time' in df.columns:
        start_date = df['Created Time'].min()
        months = pd.date_range(start=start_date, end=current_date, freq='ME')
        
        if months.empty:
            months = pd.DatetimeIndex([current_date.replace(day=1) + pd.offsets.MonthEnd(0)])

        counts = [((df['Created Time'] <= m) & ((df['Closed Time'] > m) | df['Closed Time'].isnull())).sum() for m in months]
        
        data = pd.DataFrame({'Month': months.strftime('%Y-%m'), 'Open Tickets': counts})
        data.to_excel(writer, sheet_name='8_Open_Trend', index=False)
        add_chart('8_Open_Trend', data, 1, 'Open Backlog Trend', 'line')

    # 9. Automation
    if '9' in active_features and {'Description', 'Category'}.issubset(df.columns):
        keys = ['password', 'reset', 'login', 'unlock']
        # The .str accessor is now safe because we converted 'Description' to str in Step 1
        df['Auto'] = df['Description'].str.contains('|'.join(keys), case=False, na=False)
        data = df[df['Auto']].groupby('Category')['Ticket Id'].count().reset_index(name='Count')
        data.to_excel(writer, sheet_name='9_Automation', index=False)
        add_chart('9_Automation', data, 1, 'Automation Candidates', 'pie')

    # 10. Alerts
    if '10' in active_features and 'Type' in df.columns:
        # The .str accessor is now safe
        data = df[df['Type'].str.contains('alert', case=False, na=False)].groupby('Type')['Ticket Id'].count().reset_index(name='Count')
        if not data.empty:
            data.to_excel(writer, sheet_name='10_Alerts', index=False)
            add_chart('10_Alerts', data, 1, 'Alert Volume')

    # --- 4. Dashboard & Save ---
    dash = pd.DataFrame({'Metric': ['Total', 'Open', 'Closed', 'MTTR'], 
                         'Value': [len(df), len(df_open), len(df_closed), mttr_overall]})
    dash.to_excel(writer, sheet_name='Dashboard', index=False)
    
    writer.close()
    output.seek(0)
    return output