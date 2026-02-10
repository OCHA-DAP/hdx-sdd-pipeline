import logging
import logging.config
from pathlib import Path

def configure_logging(config_path: str = None):
    """
    Configure logging using the specified configuration file.
    If no path is provided, it looks for 'logging.conf' in the project root.
    """
    if config_path:
        config_file = Path(config_path)
    else:
        # Default to project root/logging.conf
        # We assume the project structure is:
        # project_root/
        #   logging.conf
        #   src/
        #     shared/
        #       utils/
        #         logging_conf.py
        current_file = Path(__file__).resolve()
        project_root = current_file.parent.parent.parent.parent
        config_file = project_root / 'logging.conf'

    if not config_file.exists():
        # Fallback: check if logging.conf is in the current working directory
        cwd_config = Path.cwd() / 'logging.conf'
        if cwd_config.exists():
            config_file = cwd_config
        else:
            logging.basicConfig(level=logging.INFO)
            print(
                f'Logging configuration file not found at {config_file} or {cwd_config}. '
                'Using basicConfig.'
            )
            return

    # Use absolute path to avoid relative path issues
    logging.config.fileConfig(
        str(config_file.absolute()), 
        disable_existing_loggers=False
    )
