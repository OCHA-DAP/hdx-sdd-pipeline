import logging.config

# This runs as soon as pytest starts, before imports
logging.config.fileConfig = lambda *a, **k: None
