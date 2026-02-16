import pandas as pd
from datetime import datetime
import io 
import warnings

# Silence warnings
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

def generate_incident_report(df: pd.DataFrame, active_features: list):
    """
    Refactored Reporting Tool with 8 new features.
    """
    current_date = datetime.now()
    
    # --- 1. Data Cleaning & Safety Checks ---
    if 'Closed Time' not in df.columns:
        df['Closed Time'] = pd.NaT

    time_cols = ['Closed Time', 'Created Time', 'Due by Time', 'Resolved Time', 'Last Updated Time']
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

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

    # FCR Calculation Logic
    if 'Agent interactions' in df.columns and 'Status' in df.columns:
        df['FCR'] = ((df['Agent interactions'] == 1) & (df['Status'].isin(['Resolved', 'Closed']))).astype(int)
    else:
        df['FCR'] = 0

    df_open = df[df['Status'] == 'Open'].copy() if 'Status' in df.columns else pd.DataFrame()
    df_closed = df[df['Closed Time'].notnull()].copy()

    # --- 2. Excel Writer Setup ---
    output = io.BytesIO()
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

    # --- 3. Original Features (1-10) ---
    if '1' in active_features and {'Type', 'Priority', 'Ticket Id'}.issubset(df.columns):
        data = df.groupby(['Type', 'Priority'])['Ticket Id'].count().reset_index(name='Count').sort_values('Count', ascending=False)
        data.to_excel(writer, sheet_name='1_Top_Talkers', index=False)
        add_chart('1_Top_Talkers', data, 2, 'Top Types by Priority')

    if '2' in active_features and not df_closed.empty:
        df_closed['Month'] = df_closed['Closed Time'].dt.to_period('M').astype(str)
        data = df_closed.groupby('Month')['Ticket Id'].count().reset_index(name='Count')
        data.to_excel(writer, sheet_name='2_Closed_Trend', index=False)
        add_chart('2_Closed_Trend', data, 1, 'Closed Ticket Trend', 'line')

    if '3' in active_features and {'Group', 'Agent', 'Ticket Id'}.issubset(df.columns):
        data = df.groupby(['Group', 'Agent'])['Ticket Id'].count().reset_index(name='Count')
        data.to_excel(writer, sheet_name='3_Agent_Rank', index=False)
        add_chart('3_Agent_Rank', data, 2, 'Agent Volume by Group')

    if '4' in active_features and 'Group' in df.columns and 'First Response Time (in Hrs)' in df.columns:
        data = df.groupby('Group')['First Response Time (in Hrs)'].mean().reset_index(name='Hours')
        data.to_excel(writer, sheet_name='4_Response_Time', index=False)
        add_chart('4_Response_Time', data, 1, 'Avg Response Time')

    if '5' in active_features and {'Agent', 'Ticket Id'}.issubset(df.columns):
        data = df.groupby('Agent').agg({'FCR': 'mean', 'Ticket Id': 'count'}).rename(columns={'Ticket Id': 'Total Tickets'}).reset_index()
        data.to_excel(writer, sheet_name='5_FCR_Analysis', index=False)
        add_chart('5_FCR_Analysis', data, 1, 'FCR Rate by Agent')

    if '6' in active_features and not df_open.empty and 'Created Time' in df.columns:
        df_open['Days'] = (current_date - df_open['Created Time']).dt.days
        data = df_open[['Ticket Id', 'Days']].sort_values('Days', ascending=False).head(20)
        data.to_excel(writer, sheet_name='6_Aging', index=False)
        add_chart('6_Aging', data, 1, 'Top 20 Oldest Tickets')

    mttr_overall = 0
    if '7' in active_features and 'Resolution Time (in Hrs)' in df.columns and 'Group' in df.columns:
        mean_val = df['Resolution Time (in Hrs)'].mean()
        mttr_overall = 0 if pd.isna(mean_val) else mean_val
        data = df.groupby('Group')['Resolution Time (in Hrs)'].mean().reset_index(name='Hours')
        data.to_excel(writer, sheet_name='7_MTTR', index=False)
        add_chart('7_MTTR', data, 1, 'MTTR by Group')

    if '8' in active_features and 'Created Time' in df.columns:
        start_date = df['Created Time'].min()
        months = pd.date_range(start=start_date, end=current_date, freq='ME')
        counts = [((df['Created Time'] <= m) & ((df['Closed Time'] > m) | df['Closed Time'].isnull())).sum() for m in months]
        data = pd.DataFrame({'Month': [m.strftime('%Y-%m') for m in months], 'Open Tickets': counts})
        data.to_excel(writer, sheet_name='8_Open_Trend', index=False)
        add_chart('8_Open_Trend', data, 1, 'Open Backlog Trend', 'line')

    if '9' in active_features and {'Description', 'Category'}.issubset(df.columns):
        keys = ['password', 'reset', 'login', 'unlock']
        df['Auto'] = df['Description'].str.contains('|'.join(keys), case=False, na=False)
        data = df[df['Auto']].groupby('Category')['Ticket Id'].count().reset_index(name='Count')
        data.to_excel(writer, sheet_name='9_Automation', index=False)
        add_chart('9_Automation', data, 1, 'Automation Candidates', 'pie')

    # 10. Alerts (Modified to always show)
    if '10' in active_features and 'Type' in df.columns:
        data = df[df['Type'].str.contains('alert', case=False, na=False)].groupby('Type')['Ticket Id'].count().reset_index(name='Count')
    
    # Create the sheet regardless of whether data is empty or not
    if data.empty:
        data = pd.DataFrame({'Type': ['No Alerts Found'], 'Count': [0]})
        
    data.to_excel(writer, sheet_name='10_Alerts', index=False)
    add_chart('10_Alerts', data, 1, 'Alert Volume')

    # --- 4. NEW FEATURES (8 Requirements) ---

    # 11. Top 10 Tickets Category-wise
    if '11' in active_features and 'Category' in df.columns:
        data = df['Category'].value_counts().head(10).reset_index()
        data.columns = ['Category', 'Ticket Count']
        data.to_excel(writer, sheet_name='11_Top_Categories', index=False)
        add_chart('11_Top_Categories', data, 1, 'Top 10 Categories')

    # 12. Priority Tickets Separately
    if '12' in active_features and 'Priority' in df.columns:
        data = df['Priority'].value_counts().reset_index()
        data.columns = ['Priority', 'Volume']
        data.to_excel(writer, sheet_name='12_Priority_Breakdown', index=False)
        add_chart('12_Priority_Breakdown', data, 1, 'Volume by Priority', 'pie')

    # 13. Arrival Pattern (Heatmap + Sorted + Chart)
    if '13' in active_features:
        # Categorical sorting for days
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        df['Hour'] = df['Created Time'].dt.hour
        df['Day'] = pd.Categorical(df['Created Time'].dt.day_name(), categories=days_order, ordered=True)
        
        # Matrix for the Heatmap view
        arrival_matrix = df.groupby(['Day', 'Hour'])['Ticket Id'].count().unstack().fillna(0)
        arrival_matrix.to_excel(writer, sheet_name='13_Arrival_Pattern')
        
        # Summary for the Chart (Total by Hour)
        hour_summary = df.groupby('Hour')['Ticket Id'].count().reset_index(name='Total Tickets')
        hour_summary.to_excel(writer, sheet_name='13_Arrival_Pattern', startrow=10, index=False)
        
        # Adding a Line Chart to show global "Rush Hour"
        worksheet = writer.sheets['13_Arrival_Pattern']
        chart = workbook.add_chart({'type': 'line'})
        chart.add_series({
            'categories': ['13_Arrival_Pattern', 11, 0, 11 + 23, 0],
            'values':     ['13_Arrival_Pattern', 11, 1, 11 + 23, 1],
            'name': 'Global Hourly Arrival Pattern'
        })
        chart.set_title({'name': 'Hourly Ticket Volume (Rush Hour)'})
        worksheet.insert_chart('G12', chart)

    # 14. Top User in Creating Tickets
    if '14' in active_features and 'Requester Name' in df.columns:
        data = df['Requester Name'].value_counts().head(10).reset_index()
        data.columns = ['User Name', 'Ticket Count']
        data.to_excel(writer, sheet_name='14_Top_Requesters', index=False)
        add_chart('14_Top_Requesters', data, 1, 'Top 10 Ticket Creators')

    # 15. Top Asset Tickets
    if '15' in active_features and 'Item' in df.columns:
        data = df[df['Item'] != '']['Item'].value_counts().head(10).reset_index()
        data.columns = ['Asset/Item', 'Ticket Count']
        data.to_excel(writer, sheet_name='15_Asset_Tickets', index=False)
        add_chart('15_Asset_Tickets', data, 1, 'Top Impacted Assets')

    # 16. Incident vs Service Request Count
    if '16' in active_features and 'Type' in df.columns:
        data = df['Type'].value_counts().reset_index()
        data.columns = ['Ticket Type', 'Count']
        data.to_excel(writer, sheet_name='16_Type_Split', index=False)
        add_chart('16_Type_Split', data, 1, 'Incident vs Service Request', 'pie')

    # 17. Average Closure Time Priority-wise
    if '17' in active_features and 'Resolution Time (in Hrs)' in df.columns and 'Priority' in df.columns:
        data = df.groupby('Priority')['Resolution Time (in Hrs)'].mean().reset_index(name='Avg Closure (Hrs)')
        data.to_excel(writer, sheet_name='17_Closure_By_Priority', index=False)
        add_chart('17_Closure_By_Priority', data, 1, 'Avg Closure Time (Hrs)')

    # --- 5. Dashboard & Save ---
    dash = pd.DataFrame({'Metric': ['Total Tickets', 'Open', 'Closed', 'MTTR (Hrs)'], 
                         'Value': [len(df), len(df_open), len(df_closed), round(mttr_overall, 2)]})
    dash.to_excel(writer, sheet_name='Dashboard', index=False)
    
    writer.close()
    output.seek(0)
    return output