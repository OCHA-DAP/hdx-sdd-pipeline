import logging
import gspread
from gspread import client as gs_client
from config.config import get_config

logger = logging.getLogger(__name__)

CONFIG = None


def get_gsheets() -> gs_client.Client:
    """
    Returns an authenticated gspread client using credentials from the configuration.
    Initializes the configuration and client on the first call.
    """
    global CONFIG
    if not CONFIG:
        CONFIG = get_config()

    try:
        # Standard Google Service Account credentials fields
        creds_dict = {
            'type': 'service_account',
            'private_key': (
                CONFIG.GOOGLE_SHEETS_PRIVATE_KEY.replace('\\n', '\n') if CONFIG.GOOGLE_SHEETS_PRIVATE_KEY else ''
            ),
            'client_email': CONFIG.GOOGLE_SHEETS_CLIENT_EMAIL,
            'token_uri': CONFIG.GOOGLE_SHEETS_TOKEN_URI or 'https://oauth2.googleapis.com/token',
        }
        gsheet = gspread.service_account_from_dict(creds_dict)
        return gsheet
    except Exception as exc:
        logger.error(f'Exception of type {type(exc).__name__} while creating google sheets client: {str(exc)}')
        raise
