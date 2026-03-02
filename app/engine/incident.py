import pandas as pd
from datetime import datetime, timezone
import io 
import warnings

# Silence warnings
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

def generate_incident_report(df: pd.DataFrame, active_features: list, user_tz: str = 'UTC'):
    """
    Refactored Reporting Tool: Handles missing data with a clean text message area.
    """
    current_date = datetime.now(timezone.utc).replace(tzinfo=None)
    
    # --- 1. Data Cleaning & Safety Checks ---
    if 'Closed Time' not in df.columns:
        df['Closed Time'] = pd.NaT

    time_cols = ['Closed Time', 'Created Time', 'Due by Time', 'Resolved Time', 'Last Updated Time']
    for col in time_cols:
        if col in df.columns:
            # 🟢 NEW: Use format='mixed' to force Pandas to read '12/1/2025 0:47' correctly
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
            except ValueError:
                # Fallback for older versions of Pandas
                df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
            
            if not df[col].isna().all():
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize('UTC') 
                df[col] = df[col].dt.tz_convert(user_tz)   
                df[col] = df[col].dt.tz_localize(None)

    for col in ['First Response Time (in Hrs)', 'Resolution Time (in Hrs)']:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.extract(r'(\d+\.?\d*)', expand=False), 
                errors='coerce'
            )

    text_cols = ['Priority', 'Status', 'Type', 'Group', 'Agent', 'Description', 'Subject', 'Category', 'Requester Name', 'Item']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()
            if col in ['Priority', 'Status']:
                df[col] = df[col].str.title()
            
    if 'Agent' in df.columns:
        df.loc[(df['Agent'] == '') | (df['Agent'].isnull()), 'Agent'] = 'Unassigned'

    if 'Agent interactions' in df.columns and 'Status' in df.columns:
        df['FCR'] = ((df['Agent interactions'] == 1) & (df['Status'].isin(['Resolved', 'Closed']))).astype(int)
    else:
        df['FCR'] = 0

    if 'Status' in df.columns:
        active_statuses = ['Open', 'New', 'In Progress', 'Pending', 'Active', 'Unassigned', 'Waiting On Customer']
        df_open = df[df['Status'].isin(active_statuses)].copy()
    else:
        df_open = pd.DataFrame()
    
    # 🟢 NEW: Global fallback. If Closed Time is completely empty, use Resolved Time.
    if 'Closed Time' in df.columns and not df['Closed Time'].isna().all():
        df_closed = df[df['Closed Time'].notnull()].copy()
        global_time_col = 'Closed Time'
    elif 'Resolved Time' in df.columns and not df['Resolved Time'].isna().all():
        df_closed = df[df['Resolved Time'].notnull()].copy()
        global_time_col = 'Resolved Time'
    else:
        df_closed = pd.DataFrame()
        global_time_col = None

    # --- 2. Excel Writer Setup ---
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    workbook = writer.book

    # 🟢 Format for "No Data" text area
    no_data_format = workbook.add_format({
        'align': 'center',
        'valign': 'vcenter',
        'font_color': '#595959', # Subtle gray text
        'font_size': 12,
        'bold': True,
        'bg_color': 'white',     # Ensure white background
        'border': 1,             # Thin border to frame the area
        'border_color': '#D9D9D9',
        'text_wrap': True        # Allow multiple lines
    })

    # 🟢 Helper: Add Chart (Standard)
    def add_chart(sheet_name, df_chart, val_col, title, chart_type='bar'):
        worksheet = writer.sheets[sheet_name]
        chart = workbook.add_chart({'type': chart_type})
        chart.add_series({
            'categories': [sheet_name, 1, 0, 1 + len(df_chart) - 1, 0],
            'values':     [sheet_name, 1, val_col, 1 + len(df_chart) - 1, val_col],
        })
        chart.set_title({'name': title})
        worksheet.insert_chart('G2', chart)

    # 🟢 Helper: Write "No Data" Message (FIXED)
    def write_dummy_data(sheet_name, reason):
        # 1. Initialize the sheet first by writing an empty DataFrame
        # This registers the sheet in 'writer.sheets' so we don't get a KeyError
        pd.DataFrame().to_excel(writer, sheet_name=sheet_name)
        
        # 2. Now we can safely grab the worksheet object
        worksheet = writer.sheets[sheet_name]
        
        message = f"NO DATA AVAILABLE\n\nReason: {reason}\n\nPlease ensure the uploaded file contains the required columns."
        
        # 3. Merge cells where the chart would normally be (G2:N15)
        worksheet.merge_range('G2:N15', message, no_data_format)

    # --- 3. Original Features (1-10) ---
    
    # 1. Top Talkers
    if '1' in active_features:
        if {'Type', 'Priority', 'Ticket Id'}.issubset(df.columns) and not df.empty:
            data = df.groupby(['Type', 'Priority'])['Ticket Id'].count().reset_index(name='Count').sort_values('Count', ascending=False)
            data.to_excel(writer, sheet_name='1_Top_Talkers', index=False)
            add_chart('1_Top_Talkers', data, 2, 'Top Types by Priority')
        else:
            write_dummy_data('1_Top_Talkers', 'Missing Type/Priority columns or empty data')

    # 2. Closed Trend (UPDATED TO DAILY TREND)
    if '2' in active_features:
        if not df_closed.empty and global_time_col:
            # Extract just the Date (YYYY-MM-DD) for daily grouping
            df_closed['Date'] = df_closed[global_time_col].dt.strftime('%Y-%m-%d')
            data = df_closed.groupby('Date')['Ticket Id'].count().reset_index(name='Count')
            data.to_excel(writer, sheet_name='2_Closed_Trend', index=False)
            add_chart('2_Closed_Trend', data, 1, 'Daily Closed/Resolved Ticket Trend', 'line')
        else:
            write_dummy_data('2_Closed_Trend', 'Missing valid dates in both Closed Time and Resolved Time columns.')

    # 3. Agent Rank (UPDATED TO SORT BY HIGHEST COUNT)
    if '3' in active_features:
        if {'Group', 'Agent', 'Ticket Id'}.issubset(df.columns) and not df.empty:
            # 🟢 Added .sort_values() to rank from highest to lowest
            data = df.groupby(['Group', 'Agent'])['Ticket Id'].count().reset_index(name='Count').sort_values('Count', ascending=False)
            data.to_excel(writer, sheet_name='3_Agent_Rank', index=False)
            add_chart('3_Agent_Rank', data, 2, 'Agent Volume by Group')
        else:
            write_dummy_data('3_Agent_Rank', 'Missing Group/Agent columns')

    # 4. Response Time
    if '4' in active_features:
        if 'Group' in df.columns and 'First Response Time (in Hrs)' in df.columns and not df.empty:
            data = df.groupby('Group')['First Response Time (in Hrs)'].mean().reset_index(name='Hours')
            data.to_excel(writer, sheet_name='4_Response_Time', index=False)
            add_chart('4_Response_Time', data, 1, 'Avg Response Time')
        else:
            write_dummy_data('4_Response_Time', 'Missing Response Time data')

    # 5. FCR Analysis
    if '5' in active_features:
        if {'Agent', 'Ticket Id'}.issubset(df.columns) and not df.empty:
            data = df.groupby('Agent').agg({'FCR': 'mean', 'Ticket Id': 'count'}).rename(columns={'Ticket Id': 'Total Tickets'}).reset_index()
            data.to_excel(writer, sheet_name='5_FCR_Analysis', index=False)
            add_chart('5_FCR_Analysis', data, 1, 'FCR Rate by Agent')
        else:
            write_dummy_data('5_FCR_Analysis', 'Missing Agent data for FCR')

    # 6. Aging (UPDATED TO SHOW ALL OPEN TICKETS)
    if '6' in active_features:
        if not df_open.empty and 'Created Time' in df.columns:
            df_open['Days'] = (current_date - df_open['Created Time']).dt.days
            
            # 🟢 Removed .head(20) so it includes EVERY open ticket
            data = df_open[['Ticket Id', 'Days']].sort_values('Days', ascending=False)
            
            data.to_excel(writer, sheet_name='6_Aging', index=False)
            
            # 🟢 Updated the chart title to reflect all tickets
            add_chart('6_Aging', data, 1, 'Aging of All Open Tickets')
        else:
            write_dummy_data('6_Aging', 'No Open Tickets or missing Created Time')
            
    # 7. MTTR
    mttr_overall = 0
    if '7' in active_features:
        if 'Resolution Time (in Hrs)' in df.columns and 'Group' in df.columns and not df.empty:
            mean_val = df['Resolution Time (in Hrs)'].mean()
            mttr_overall = 0 if pd.isna(mean_val) else mean_val
            data = df.groupby('Group')['Resolution Time (in Hrs)'].mean().reset_index(name='Hours')
            data.to_excel(writer, sheet_name='7_MTTR', index=False)
            add_chart('7_MTTR', data, 1, 'MTTR by Group')
        else:
            write_dummy_data('7_MTTR', 'Missing Resolution Time data')

    # 8. Open Trend (UPDATED TO DAILY TREND)
    if '8' in active_features:
        if 'Created Time' in df.columns and not df.empty:
            start_date = df['Created Time'].min()
            if pd.notna(start_date):
                # 🟢 Changed freq='ME' to freq='D' to generate a list of every single day
                days = pd.date_range(start=start_date, end=current_date, freq='D')
                
                # Check how many tickets were open on each specific day
                counts = [((df['Created Time'] <= d) & ((df['Closed Time'] > d) | df['Closed Time'].isnull())).sum() for d in days]
                
                # 🟢 Changed column name to 'Date' and format to YYYY-MM-DD
                data = pd.DataFrame({'Date': [d.strftime('%Y-%m-%d') for d in days], 'Open Tickets': counts})
                
                data.to_excel(writer, sheet_name='8_Open_Trend', index=False)
                add_chart('8_Open_Trend', data, 1, 'Daily Open Backlog Trend', 'line')
            else:
                write_dummy_data('8_Open_Trend', 'Invalid dates in Created Time')
        else:
            write_dummy_data('8_Open_Trend', 'Missing Created Time column')

    # 9. Automation
    if '9' in active_features:
        if {'Description', 'Category'}.issubset(df.columns) and not df.empty:
            keys = ['password', 'reset', 'login', 'unlock']
            df['Auto'] = df['Description'].str.contains('|'.join(keys), case=False, na=False)
            data = df[df['Auto']].groupby('Category')['Ticket Id'].count().reset_index(name='Count')
            data.to_excel(writer, sheet_name='9_Automation', index=False)
            add_chart('9_Automation', data, 1, 'Automation Candidates', 'pie')
        else:
            write_dummy_data('9_Automation', 'Missing Description/Category columns')

    # 10. Alerts
    if '10' in active_features:
        data = pd.DataFrame()
        if 'Type' in df.columns and not df.empty:
            data = df[df['Type'].str.contains('alert', case=False, na=False)].groupby('Type')['Ticket Id'].count().reset_index(name='Count')
        
        if data.empty:
            # For alerts, "No Data" is a valid positive result, so we show a chart.
            data = pd.DataFrame({'Type': ['No Alerts Found'], 'Count': [0]})
        
        data.to_excel(writer, sheet_name='10_Alerts', index=False)
        add_chart('10_Alerts', data, 1, 'Alert Volume')

    # --- 4. NEW FEATURES (11-17) ---

    # 11. Top Categories
    if '11' in active_features:
        if 'Category' in df.columns and not df.empty:
            data = df['Category'].value_counts().head(10).reset_index()
            data.columns = ['Category', 'Ticket Count']
            data.to_excel(writer, sheet_name='11_Top_Categories', index=False)
            add_chart('11_Top_Categories', data, 1, 'Top 10 Categories')
        else:
            write_dummy_data('11_Top_Categories', 'Missing Category column')

    # 12. Priority Breakdown
    if '12' in active_features:
        if 'Priority' in df.columns and not df.empty:
            data = df['Priority'].value_counts().reset_index()
            data.columns = ['Priority', 'Volume']
            data.to_excel(writer, sheet_name='12_Priority_Breakdown', index=False)
            add_chart('12_Priority_Breakdown', data, 1, 'Volume by Priority', 'pie')
        else:
            write_dummy_data('12_Priority_Breakdown', 'Missing Priority column')

    # 13. Arrival Pattern
    if '13' in active_features:
        if 'Created Time' in df.columns and not df.empty:
            try:
                days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                df['Hour'] = df['Created Time'].dt.hour
                df['Day'] = pd.Categorical(df['Created Time'].dt.day_name(), categories=days_order, ordered=True)
                
                arrival_matrix = df.groupby(['Day', 'Hour'])['Ticket Id'].count().unstack().fillna(0)
                arrival_matrix.to_excel(writer, sheet_name='13_Arrival_Pattern')
                
                hour_summary = df.groupby('Hour')['Ticket Id'].count().reset_index(name='Total Tickets')
                hour_summary.to_excel(writer, sheet_name='13_Arrival_Pattern', startrow=10, index=False)
                
                worksheet = writer.sheets['13_Arrival_Pattern']
                chart = workbook.add_chart({'type': 'line'})
                chart.add_series({
                    'categories': ['13_Arrival_Pattern', 11, 0, 11 + 23, 0],
                    'values':     ['13_Arrival_Pattern', 11, 1, 11 + 23, 1],
                    'name': 'Global Hourly Arrival Pattern'
                })
                chart.set_title({'name': 'Hourly Ticket Volume (Rush Hour)'})
                worksheet.insert_chart('G12', chart)
            except Exception:
                write_dummy_data('13_Arrival_Pattern', 'Error processing date data')
        else:
            write_dummy_data('13_Arrival_Pattern', 'Missing Created Time column')

    # 14. Top Requesters
    if '14' in active_features:
        if 'Requester Name' in df.columns and not df.empty:
            data = df['Requester Name'].value_counts().head(10).reset_index()
            data.columns = ['User Name', 'Ticket Count']
            data.to_excel(writer, sheet_name='14_Top_Requesters', index=False)
            add_chart('14_Top_Requesters', data, 1, 'Top 10 Ticket Creators')
        else:
            write_dummy_data('14_Top_Requesters', 'Missing Requester Name column')

    # 15. Top Assets
    if '15' in active_features:
        if 'Item' in df.columns and not df.empty:
            data = df[df['Item'] != '']['Item'].value_counts().head(10).reset_index()
            data.columns = ['Asset/Item', 'Ticket Count']
            data.to_excel(writer, sheet_name='15_Asset_Tickets', index=False)
            add_chart('15_Asset_Tickets', data, 1, 'Top Impacted Assets')
        else:
            write_dummy_data('15_Asset_Tickets', 'Missing Item column')

    # 16. Type Split
    if '16' in active_features:
        if 'Type' in df.columns and not df.empty:
            data = df['Type'].value_counts().reset_index()
            data.columns = ['Ticket Type', 'Count']
            data.to_excel(writer, sheet_name='16_Type_Split', index=False)
            add_chart('16_Type_Split', data, 1, 'Incident vs Service Request', 'pie')
        else:
            write_dummy_data('16_Type_Split', 'Missing Type column')

    # 17. Closure by Priority
    if '17' in active_features:
        if 'Resolution Time (in Hrs)' in df.columns and 'Priority' in df.columns and not df.empty:
            data = df.groupby('Priority')['Resolution Time (in Hrs)'].mean().reset_index(name='Avg Closure (Hrs)')
            data.to_excel(writer, sheet_name='17_Closure_By_Priority', index=False)
            add_chart('17_Closure_By_Priority', data, 1, 'Avg Closure Time (Hrs)')
        else:
            write_dummy_data('17_Closure_By_Priority', 'Missing Resolution Time or Priority columns')

    # --- 5. Dashboard & Save ---
    dash = pd.DataFrame({'Metric': ['Total Tickets', 'Open', 'Closed', 'MTTR (Hrs)'], 
                         'Value': [len(df), len(df_open), len(df_closed), round(mttr_overall, 2)]})
    dash.to_excel(writer, sheet_name='Dashboard', index=False)
    
    writer.close()
    output.seek(0)
    return output