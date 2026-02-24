from flask import Blueprint, request, jsonify
from datetime import datetime, timezone
from flask_jwt_extended import jwt_required, get_jwt

# 🟢 POSTGRESQL MODELS IMPORT
from app.models import db, AuditReport, CallAuditResult, Criterion

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/api/combined-dashboard-summary')
@jwt_required()
def get_combined_dashboard_summary():
    """
    Unified Dashboard Summary (PostgreSQL Version)
    Params:
      - category: 'qa' (default) OR 'call'
      - start_date: 'YYYY-MM-DD'
      - end_date: 'YYYY-MM-DD'
    """
    try:
        # 1. Get User Context
        claims = get_jwt()
        project_code = claims.get('project')
        
        category = request.args.get('category', 'qa').lower() 
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        stats_output = {}
        tickets_output = {}

        criteria_type = 'ticket audit' if category == 'qa' else 'call audit'
        
        # 2. Fetch Active Criteria for this tenant
        active_criteria_records = Criterion.query.filter_by(
            type=criteria_type, 
            is_active=True, 
            project_code=project_code
        ).all()
        
        active_criteria_names = [c.name for c in active_criteria_records]
        if not active_criteria_names:
            active_criteria_names = []

        # ==========================================
        # PATH A: QA / GENERAL AUDIT (Ticket)
        # ==========================================
        if category == 'qa':
            # 1. Build Query
            query = AuditReport.query.filter_by(project_code=project_code)
            
            # QA uses "Audit Date" inside the JSONB full_data
            if start_date_str and end_date_str:
                query = query.filter(
                    AuditReport.full_data['Audit Date'].astext >= start_date_str,
                    AuditReport.full_data['Audit Date'].astext <= end_date_str
                )
                
            records = query.all()
            
            # 2. Aggregation Variables
            total_count = len(records)
            agent_scores = {}
            pie_counts = {0: 0, 60: 0, 80: 0, 90: 0}
            bar_counts = {name: {} for name in active_criteria_names}
            valid_tickets = []
            
            invalid_agents = {None, "", "No Agent", "Unknown", "Unknown Agent", "Unassigned", "NaN"}

            # 3. Process Records
            for r in records:
                data = r.full_data or {}
                agent = data.get('Agent')
                raw_score = data.get('Overall Score', 0)
                
                try:
                    score = float(raw_score)
                except (ValueError, TypeError):
                    score = 0.0

                # Top/Least Tickets tracking
                valid_tickets.append({
                    "Ticket ID": data.get('Ticket ID'),
                    "Agent": agent,
                    "Overall Score": score
                })

                # Agent Average tracking
                if agent not in invalid_agents:
                    if agent not in agent_scores:
                        agent_scores[agent] = []
                    agent_scores[agent].append(score)

                # Pie Chart buckets
                if score < 60: pie_counts[0] += 1
                elif score < 80: pie_counts[60] += 1
                elif score < 90: pie_counts[80] += 1
                else: pie_counts[90] += 1

                # Bar Chart (Dynamic JSON extraction)
                for k, v in data.items():
                    if k in active_criteria_names:
                        status = str(v) if v is not None else "N/A"
                        bar_counts[k][status] = bar_counts[k].get(status, 0) + 1

            # 4. Format Output
            agent_avgs = []
            for agent, scores in agent_scores.items():
                avg = sum(scores) / len(scores) if scores else 0
                agent_avgs.append({"agent": agent, "score": round(avg, 2)})

            # Sort agents
            top_agents = sorted(agent_avgs, key=lambda x: x['score'], reverse=True)[:5]
            least_agents = sorted(agent_avgs, key=lambda x: x['score'])[:5]

            # Format Pie
            labels = {0: "Poor (<60)", 60: "Fair (60-80)", 80: "Good (80-90)", 90: "Excellent (90+)"}
            pie_chart_data = [{"label": labels[k], "value": v} for k, v in pie_counts.items()]

            # Format Bar
            bar_chart_data = [{"name": k, "counts": v} for k, v in bar_counts.items() if v]
            bar_chart_data.sort(key=lambda x: x['name'])

            # Format Tickets
            valid_tickets.sort(key=lambda x: x['Overall Score'])
            least_tickets_raw = valid_tickets[:5]
            top_tickets_raw = valid_tickets[::-1][:5]

            def format_tickets(t_list):
                return [{
                    "id": str(t.get("Ticket ID")), 
                    "agentName": t.get("Agent"), 
                    "score": t.get("Overall Score")
                } for t in t_list]

            stats_output = {
                'total_audits': total_count,
                'top_agents': top_agents,
                'least_agents': least_agents,
                'pie_chart': pie_chart_data,
                'bar_chart': bar_chart_data
            }
            tickets_output = {
                'top_5_tickets': format_tickets(top_tickets_raw),
                'least_5_tickets': format_tickets(least_tickets_raw)
            }


        # ==========================================
        # PATH B: CALL AUDIT
        # ==========================================
        elif category == 'call':
            # 1. Build Query
            query = CallAuditResult.query.filter_by(project_code=project_code)
            
            # Call DB uses strictly parsed 'created_at' timestamps
            if start_date_str and end_date_str:
                s_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                e_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                query = query.filter(CallAuditResult.created_at >= s_date, CallAuditResult.created_at <= e_date)
            
            records = query.all()
            
            # 2. Aggregation Variables
            total_count = len(records)
            agent_scores = {}
            pie_counts = {0: 0, 60: 0, 80: 0, 90: 0}
            bar_counts = {name: {} for name in active_criteria_names}

            # 3. Process Records
            for r in records:
                agent = r.agent_name or "Unknown"
                data = r.full_data or {}
                raw_score = data.get('Overall Score', 0)
                
                try:
                    score = float(raw_score)
                except (ValueError, TypeError):
                    score = 0.0

                # Agent Average Tracking
                if agent not in agent_scores:
                    agent_scores[agent] = []
                agent_scores[agent].append(score)

                # Pie Chart buckets
                if score < 60: pie_counts[0] += 1
                elif score < 80: pie_counts[60] += 1
                elif score < 90: pie_counts[80] += 1
                else: pie_counts[90] += 1

                # Bar Chart (Iterate Breakdown Array)
                breakdown = data.get('Breakdown', [])
                if isinstance(breakdown, list):
                    for item in breakdown:
                        param = item.get('Parameter')
                        if param in active_criteria_names:
                            status = str(item.get('Status', 'Unknown'))
                            bar_counts[param][status] = bar_counts[param].get(status, 0) + 1

            # 4. Format Output
            agent_avgs = []
            for agent, scores in agent_scores.items():
                avg = sum(scores) / len(scores) if scores else 0
                agent_avgs.append({"agent": agent, "score": round(avg, 2)})

            # Sort agents
            top_agents = sorted(agent_avgs, key=lambda x: x['score'], reverse=True)[:5]
            least_agents = sorted(agent_avgs, key=lambda x: x['score'])[:5]

            # Format Pie
            labels = {0: "Poor (<60)", 60: "Fair (60-80)", 80: "Good (80-90)", 90: "Excellent (90+)"}
            pie_chart_data = [{"label": labels[k], "value": v} for k, v in pie_counts.items()]

            # Format Bar
            bar_chart_data = [{"name": k, "counts": v} for k, v in bar_counts.items() if v]
            bar_chart_data.sort(key=lambda x: x['name'])

            # Format Tickets
            def format_for_ui(agg_list):
                return [{
                    "id": "Agent", 
                    "agentName": a.get("agent") or "Unknown", 
                    "score": a.get("score") or 0
                } for a in agg_list]

            stats_output = {
                'total_audits': total_count,
                'top_agents': top_agents,
                'least_agents': least_agents,
                'pie_chart': pie_chart_data,
                'bar_chart': bar_chart_data
            }
            
            tickets_output = {
                'top_5_tickets': format_for_ui(top_agents),
                'least_5_tickets': format_for_ui(least_agents)
            }

        else:
            return jsonify({'error': 'Invalid category. Use "qa" or "call".'}), 400

        # 5. Final JSON Return
        return jsonify({
            'category': category,
            'stats': stats_output,
            'tickets': tickets_output
        })

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500