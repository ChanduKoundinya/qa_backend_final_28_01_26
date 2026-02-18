from flask import jsonify, request
from datetime import datetime
from app.extensions import mongo
from . import dashboard_bp
import certifi
from flask_pymongo import PyMongo

from flask import Blueprint, request, jsonify
from datetime import datetime
import pymongo
from flask_jwt_extended import jwt_required
dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/api/combined-dashboard-summary')
@jwt_required()
def get_combined_dashboard_summary():
    """
    Unified Dashboard Summary
    Params:
      - category: 'qa' (default) OR 'call'
      - start_date: 'YYYY-MM-DD'
      - end_date: 'YYYY-MM-DD'
    """
    try:
        # 1. Get Parameters and normalize to LOWERCASE
        # This turns "QA" -> "qa"
        category = request.args.get('category', 'qa').lower() 
        
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # 2. Build Date Filter based on Category
        date_query = {}

        if start_date_str and end_date_str:
            # 🟢 FIX: Check for 'qa' (lowercase)
            if category == 'qa':
                # QA DB uses "Audit Date" as STRING
                date_query = {
                    "Audit Date": {
                        "$gte": start_date_str,
                        "$lte": end_date_str
                    }
                }
            elif category == 'call':
                # Call DB uses "created_at" as DATETIME
                s_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                e_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
                date_query = {
                    "created_at": {
                        "$gte": s_date,
                        "$lte": e_date
                    }
                }

        stats_output = {}
        tickets_output = {}


        criteria_type = 'ticket audit' if category == 'qa' else 'call audit'
        
        # Fetch only active criteria names from DB
        active_criteria_cursor = mongo.db.criteria.find(
            {"type": criteria_type, "is_active": True},
            {"name": 1, "_id": 0}
        )
        active_criteria_names = [doc["name"] for doc in active_criteria_cursor]
        
        # If there are no active criteria, ensure we don't crash (pass an empty list)
        if not active_criteria_names:
            active_criteria_names = []

        # ==========================================
        # PATH A: QA / GENERAL AUDIT
        # ==========================================
        # 🟢 FIX: Check for 'qa' (lowercase)
        if category == 'qa':
            # A1. Total Count
            total_count = mongo.db.audit_reports.count_documents(date_query)

            # A2. Agent Aggregation
            agent_pipeline = [
                {"$match": date_query},
                {"$group": {"_id": "$Agent", "average_score": {"$avg": "$Overall Score"}}},
                {"$project": {"agent": "$_id", "score": {"$round": ["$average_score", 2]}, "_id": 0}}
            ]
            top_agents = list(mongo.db.audit_reports.aggregate(agent_pipeline + [{"$sort": {"score": -1}}, {"$limit": 5}]))
            least_agents = list(mongo.db.audit_reports.aggregate(agent_pipeline + [{"$sort": {"score": 1}}, {"$limit": 5}]))

            # A3. Pie Chart
            pie_pipeline = [
                {"$match": date_query},
                {
                    "$bucket": {
                        "groupBy": "$Overall Score",
                        "boundaries": [0, 60, 80, 90, 101],
                        "default": "Other",
                        "output": { "count": { "$sum": 1 } }
                    }
                }
            ]
            raw_pie = list(mongo.db.audit_reports.aggregate(pie_pipeline))
            labels = {0: "Poor (<60)", 60: "Fair (60-80)", 80: "Good (80-90)", 90: "Excellent (90+)"}
            pie_chart_data = [{"label": labels.get(d['_id'], "Other"), "value": d['count']} for d in raw_pie]

            # A4. Bar Chart (FLAT Structure Logic)
            bar_pipeline = [
                {"$match": date_query},
                {"$project": {"data_array": {"$objectToArray": "$$ROOT"}}},
                {"$unwind": "$data_array"},
                {"$match": {
                    "data_array.k": {
                        "$in": active_criteria_names
                    }
                }},
                {"$group": {
                    "_id": {"parameter": "$data_array.k", "status": {"$toString": {"$ifNull": ["$data_array.v", "N/A"]}}},
                    "count": {"$sum": 1}
                }},
                {"$group": {
                    "_id": "$_id.parameter",
                    "statuses": {"$push": {"k": "$_id.status", "v": "$count"}}
                }},
                {"$project": {"_id": 0, "name": "$_id", "counts": {"$arrayToObject": "$statuses"}}},
                {"$sort": {"name": 1}}
            ]
            bar_chart_data = list(mongo.db.audit_reports.aggregate(bar_pipeline))

            # A5. Ticket Details
            projection = {'Ticket ID': 1, 'Agent': 1, 'Overall Score': 1, '_id': 0}
            raw_top = list(mongo.db.audit_reports.find(date_query, projection).sort("Overall Score", -1).limit(5))
            raw_least = list(mongo.db.audit_reports.find(date_query, projection).sort("Overall Score", 1).limit(5))

            def format_tickets(t_list):
                return [{
                    "id": str(t.get("Ticket ID")), # Force string here
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
                'top_5_tickets': format_tickets(raw_top),
                'least_5_tickets': format_tickets(raw_least)
            }

       # ==========================================
        # PATH B: CALL AUDIT
        # ==========================================
        elif category == 'call':
            # 1. Helper Functions
            def format_for_ui(agg_list):
                return [{
                    "id": "Agent", 
                    "agentName": a.get("agent") or "Unknown", 
                    "score": a.get("score") or 0
                } for a in agg_list]
            
            # 2. Total Count
            total_count = mongo.db.call_audit_results.count_documents(date_query)

            # 3. Agent Aggregation (FIXED)
            agent_pipeline = [
                {"$match": date_query},
                {"$group": {
                    # 🟢 FIX: Group by the root-level 'agent_name' field
                    "_id": "$agent_name", 
                    "avg_score": {"$avg": "$full_data.Overall Score"}
                }},
                {"$project": {
                    # If agent_name is missing, default to "Unknown"
                    "agent": {"$ifNull": ["$_id", "Unknown"]}, 
                    "score": {"$round": ["$avg_score", 2]}, 
                    "_id": 0
                }}
            ]
            
            # Get Top 5
            top_agents = list(mongo.db.call_audit_results.aggregate(
                agent_pipeline + [{"$sort": {"score": -1}}, {"$limit": 5}]
            ))
            
            # Get Least 5
            least_agents = list(mongo.db.call_audit_results.aggregate(
                agent_pipeline + [{"$sort": {"score": 1}}, {"$limit": 5}]
            ))
            
            # 4. Pie Chart
            pie_pipeline = [
                {"$match": date_query},
                {"$bucket": {
                    "groupBy": "$full_data.Overall Score",
                    "boundaries": [0, 60, 80, 90, 101],
                    "default": "Other",
                    "output": { "count": { "$sum": 1 } }
                }}
            ]
            raw_pie = list(mongo.db.call_audit_results.aggregate(pie_pipeline))
            labels = {0: "Poor (<60)", 60: "Fair (60-80)", 80: "Good (80-90)", 90: "Excellent (90+)"}
            pie_chart_data = [{"label": labels.get(d['_id'], "Other"), "value": d['count']} for d in raw_pie]

            # 5. Bar Chart
            bar_pipeline = [
                {"$match": date_query}, 
                {"$unwind": "$full_data.Breakdown"}, 
                {"$match": {
                    "full_data.Breakdown.Parameter": {
                        "$in": active_criteria_names
                    }
                }},
                {"$group": {
                    "_id": {
                        "parameter": "$full_data.Breakdown.Parameter",
                        "status": {"$toString": {"$ifNull": ["$full_data.Breakdown.Status", "Unknown"]}}
                    },
                    "count": {"$sum": 1}
                }},
                {"$group": {
                    "_id": "$_id.parameter",
                    "statuses": {"$push": {"k": "$_id.status", "v": "$count"}}
                }},
                {"$project": {"_id": 0, "name": "$_id", "counts": {"$arrayToObject": "$statuses"}}},
                {"$sort": {"name": 1}}
            ]
            raw_bar_data = list(mongo.db.call_audit_results.aggregate(bar_pipeline))
            bar_chart_data = [{"name": item['name'], **item.get('counts', {})} for item in raw_bar_data]

            # 6. Prepare Final Output Objects
            stats_output = {
                'total_audits': total_count,
                'top_agents': top_agents,
                'least_agents': least_agents,
                'pie_chart': pie_chart_data,
                'bar_chart': bar_chart_data
            }
            
            # Map unique aggregated agents to the Top/Least slots
            tickets_output = {
                'top_5_tickets': format_for_ui(top_agents),
                'least_5_tickets': format_for_ui(least_agents)
            }
        # Handle invalid categories
        else:
            return jsonify({'error': 'Invalid category. Use "qa" or "call".'}), 400

        # 3. Final JSON Return (Used by both Path A and Path B)
        return jsonify({
            'category': category,
            'stats': stats_output,
            'tickets': tickets_output
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500