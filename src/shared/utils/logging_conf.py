import os
import logging.config

logging.config.fileConfig(os.getenv('LOGGING_CONF', 'logging.conf'))