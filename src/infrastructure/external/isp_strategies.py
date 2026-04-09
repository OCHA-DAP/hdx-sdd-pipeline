import json
from typing import Dict, Any
from src.infrastructure.external.google_sheets_client import google_sheets_client

class GoogleSheetsISPStrategy:
    """
    Strategy to retrieve and transform ISP data directly from Google Sheets.
    """
    def __init__(self, spreadsheet_url: str = 'https://docs.google.com/spreadsheets/d/1Z5wj6H6WV2E8VN9r6y8AfdgOltTNL-z2KIlbnNzzPok/edit?gid=0#gid=0'):
        self.spreadsheet_url = spreadsheet_url
        self.country_mapping_iso_code = {
            'afghanistan': 'AFG',
            'burundi': 'BDI',
            'cameroon (nwsw)': 'CMR',
            'cameroon (extreme nord)': 'CMR',
            'democratic republic of the congo ': 'COD',
            'haiti': 'HTI',
            'iraq': 'IRQ',
            'mozambique': 'MOZ',
            'myanmar': 'MMR',
            'niger': 'NIG',
            'palestin': 'PSE',
            'pakistan': 'PAK',
            'somalia': 'SOM',
            'south sudan': 'SSD',
            'sudan': 'SDN',
            'syria': 'SYR',
            'ukraine': 'UKR',
            'yemen': 'YEM',
        }

    def get_isps(self) -> Dict[str, Any]:
        """
        Retrieves ISP data from Google Sheets and transforms it into a dictionary format.
        """
        spreadsheet = google_sheets_client.open_by_url(self.spreadsheet_url)
        worksheet = spreadsheet.worksheet('Data and Information Types Copy from ISP')
        
        values = worksheet.get_all_values()
        
        isp_dict = {}
        for idx, value in enumerate(values[0]):
            if idx == 0:
                continue
            if values[1][idx] == '' and values[2][idx] == '' and values[3][idx] == '' and values[4][idx] == '':
                continue
            isp_dict[value] = {
                'low_no_sensitivity': values[1][idx],
                'medium_sensitivity': values[2][idx],
                'high_sensitivity': values[3][idx],
                'severe_sensitivity': values[4][idx]
            }
            
        for key in list(isp_dict.keys()):
            if 'draft' in key.lower() or 'old' in key.lower():
                del isp_dict[key]
                
        for key in list(isp_dict.keys()):
            for country, iso_code in self.country_mapping_iso_code.items():
                if country in key.lower():
                    isp_dict[key]['ISO_CODE'] = iso_code
                    break
                    
        return isp_dict

class LocalJSONISPStrategy:
    """
    Strategy to retrieve ISP data from a local JSON file.
    """
    def __init__(self, json_path: str = 'data/isps.json'):
        self.json_path = json_path

    def get_isps(self) -> Dict[str, Any]:
        """
        Loads ISP data from the configured JSON file.
        """
        with open(self.json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
