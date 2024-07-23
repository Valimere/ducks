from flask import Flask
from routes import main
import logging
import signal
import sys
from store import DuckDBStore  # Import your DuckDBStore class

app = Flask(__name__, static_folder='/app/app/static', static_url_path='')
app.config.from_object('config.Config')

app.register_blueprint(main, url_prefix='')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Log the static folder path
logger.info(f"Static folder path: {main.static_folder}")

# Initialize the DuckDBStore instance
store = DuckDBStore()

def signal_handler(sig, frame):
    print('Shutting down gracefully...')
    # Add any cleanup tasks here
    try:
        # Close the DuckDB connection
        store.close_connection()
        print('DuckDB connection closed.')
    except Exception as e:
        print(f'Error closing DuckDB connection: {e}')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
