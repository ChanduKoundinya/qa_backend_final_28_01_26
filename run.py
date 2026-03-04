import logging
from app import create_app
from app.extensions import scheduler
from app.modules.tasks.routes import evaluate_summary_triggers
# --- FIX: Enable INFO logging so you can see success messages ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

app = create_app()
 
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # Set limit to 50MB

with app.app_context():
    
    # This runs the trigger evaluator every 1 minute
    scheduler.add_job(
        id='daily_summary_checker',
        func=evaluate_summary_triggers,
        args=[app],
        trigger='cron',
        minute='*',
        replace_existing=True # Prevents duplicate jobs if the file reloads
    )

if __name__ == '__main__':
    # You can also set debug=True, but basicConfig handles the logging
    app.run(debug=True, port=5000, use_reloader=False)