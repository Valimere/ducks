import logging

class Logger:
    def __init__(self):
        self.logger = logging.getLogger('flask_app')
        self.logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)
