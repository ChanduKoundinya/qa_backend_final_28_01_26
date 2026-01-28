import logging
from app import create_app

# --- FIX: Enable INFO logging so you can see success messages ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

app = create_app()
 
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # Set limit to 50MB

if __name__ == '__main__':
    # You can also set debug=True, but basicConfig handles the logging
    app.run(debug=True, port=5000)