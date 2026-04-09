import gspread
from google.oauth2.service_account import Credentials

# permissions
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# authenticate
credentials = Credentials.from_service_account_file('service_account.json', scopes=scopes)

# create client
google_sheets_client = gspread.authorize(credentials)
