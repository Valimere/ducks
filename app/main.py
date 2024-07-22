from flask import Flask
from routes import main
import logging

app = Flask(__name__, static_folder='/app/app/static', static_url_path='')
app.config.from_object('config.Config')

app.register_blueprint(main)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Log the static folder path
logger.info(f"Static folder path: {app.static_folder}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
