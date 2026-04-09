import gspread
from google.oauth2.service_account import Credentials


class GoogleSheetsClientProxy:
    """
    A proxy for the gspread client that initializes only when accessed.
    This prevents unintended side effects (like loading service account JSON)
    at module import time.
    """

    def __init__(self):
        self._client = None
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

    def _initialize(self):
        if self._client is None:
            # authenticate and create client
            credentials = Credentials.from_service_account_file('service_account.json', scopes=self.scopes)
            self._client = gspread.authorize(credentials)

    def __getattr__(self, name):
        self._initialize()
        return getattr(self._client, name)


# Create the proxy instance
google_sheets_client = GoogleSheetsClientProxy()
