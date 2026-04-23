import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class GoogleSheetsISPStrategy:
    """
    Strategy to retrieve and transform ISP data directly from Google Sheets.
    """

    def __init__(
        self,
        spreadsheet_url: str = 'https://docs.google.com/spreadsheets/d/1Z5wj6H6WV2E8VN9r6y8AfdgOltTNL-z2KIlbnNzzPok/edit?gid=0#gid=0',
    ):
        self.spreadsheet_url = spreadsheet_url
        self.country_mapping_iso_code = {
            'afghanistan': 'AFG',
            'burundi': 'BDI',
            'cameroon (nwsw)': 'CMR',
            'cameroon (extreme nord)': 'CMR',
            'democratic republic of the congo': 'COD',
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
        from src.infrastructure.external.google_sheets_client import get_gsheets

        spreadsheet = get_gsheets().open_by_url(self.spreadsheet_url)
        worksheet = spreadsheet.worksheet('Data and Information Types Copy from ISP')

        values = worksheet.get_all_values()

        if not values or len(values) < 5:
            logger.error(
                f'ISP Google Sheet at {self.spreadsheet_url} has insufficient rows ({len(values)}), expected at least 5'
            )
            return {}

        isp_dict = {}
        header = values[0]
        for idx, value in enumerate(header):
            if idx == 0 or not value:
                continue

            # Ensure all required rows have this column
            if any(len(values[i]) <= idx for i in range(1, 5)):
                logger.warning(f'Column {idx} ("{value}") in ISP sheet is missing sensitivity cells. Skipping.')
                continue

            if values[1][idx] == '' and values[2][idx] == '' and values[3][idx] == '' and values[4][idx] == '':
                continue

            isp_dict[value] = {
                'low_no_sensitivity': values[1][idx],
                'medium_sensitivity': values[2][idx],
                'high_sensitivity': values[3][idx],
                'severe_sensitivity': values[4][idx],
            }

        for key in list(isp_dict.keys()):
            if 'draft' in key.lower() or 'old' in key.lower():
                del isp_dict[key]

        for key in list(isp_dict.keys()):
            for country, iso_code in self.country_mapping_iso_code.items():
                if country in key.lower():
                    isp_dict[key]['ISO_CODE'] = iso_code
                    break

        isp_dict['default'] = {
            'low_no_sensitivity': (
                'Data that can be shared publicly or within coordination structures without risk of harm to '
                'individuals or operations.\n'
                '- Humanitarian Needs Overview (HNO) and Humanitarian Response Plan (HRP) data\n'
                '- Common Operational Datasets (CODs)\n'
                '- 3W/4W/5W data at administrative levels 1 or 2 (national, regional)\n'
                '- Situation reports and snapshots\n'
                '- Aggregated assessment results at ADM2 or higher\n'
                '- Generic contact details (organization-level)\n'
                '- Facility location data (e.g., schools, health centers) with consent\n'
                '- General population and IDP statistics at state/district level'
            ),
            'medium_sensitivity': (
                'Data that may cause limited risk if disclosed, and may require contextual approval before sharing.\n'
                '- Assessment and survey data aggregated at ADM2 or ADM3\n'
                '- Disaggregated data (e.g., by age, gender) without personal identifiers\n'
                '- Access constraints data aggregated to district or county level\n'
                '- Collective site data without individual-level details\n'
                '- Public facility data with increased geographic specificity\n'
                '- Security incident reports aggregated at ADM2'
            ),
            'high_sensitivity': (
                'Data that could cause significant harm to individuals or agencies if disclosed. Requires prior '
                'approval and strong protection mechanisms.\n'
                '- Aid-worker contact details (without consent)\n'
                '- Survey or monitoring data at the community or household level\n'
                '- Detailed access constraints or operational presence by location\n'
                '- Sensitive programmatic data (e.g., protection activities by location)\n'
                '- Health and education facility data with exact coordinates'
            ),
            'severe_sensitivity': (
                'Highly sensitive data that, if disclosed, could lead to serious harm or legal consequences. Sharing '
                'is highly restricted.\n'
                '- Individual survey responses and household-level datasets\n'
                '- SEA/GBV/PSEA case data or complaint records\n'
                '- Security incident logs with identifiable data\n'
                '- Location and identity of displaced individuals or vulnerable groups\n'
                '- Raw data from feedback or complaints mechanisms'
            ),
            'ISO_CODE': 'default',
        }

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
            isps = json.load(f)

        if 'default' not in isps:
            isps['default'] = {
                'low_no_sensitivity': (
                    'Data that can be shared publicly or within coordination structures without risk of harm to '
                    'individuals or operations.\n'
                    '- Humanitarian Needs Overview (HNO) and Humanitarian Response Plan (HRP) data\n'
                    '- Common Operational Datasets (CODs)\n'
                    '- 3W/4W/5W data at administrative levels 1 or 2 (national, regional)\n'
                    '- Situation reports and snapshots\n'
                    '- Aggregated assessment results at ADM2 or higher\n'
                    '- Generic contact details (organization-level)\n'
                    '- Facility location data (e.g., schools, health centers) with consent\n'
                    '- General population and IDP statistics at state/district level'
                ),
                'medium_sensitivity': (
                    'Data that may cause limited risk if disclosed, '
                    'and may require contextual approval before sharing.\n'
                    '- Assessment and survey data aggregated at ADM2 or ADM3\n'
                    '- Disaggregated data (e.g., by age, gender) without personal identifiers\n'
                    '- Access constraints data aggregated to district or county level\n'
                    '- Collective site data without individual-level details\n'
                    '- Public facility data with increased geographic specificity\n'
                    '- Security incident reports aggregated at ADM2'
                ),
                'high_sensitivity': (
                    'Data that could cause significant harm to individuals or agencies if disclosed. Requires prior '
                    'approval and strong protection mechanisms.\n'
                    '- Aid-worker contact details (without consent)\n'
                    '- Survey or monitoring data at the community or household level\n'
                    '- Detailed access constraints or operational presence by location\n'
                    '- Sensitive programmatic data (e.g., protection activities by location)\n'
                    '- Health and education facility data with exact coordinates'
                ),
                'severe_sensitivity': (
                    'Highly sensitive data that, if disclosed, could lead to serious harm or legal consequences. '
                    'Sharing is highly restricted.\n'
                    '- Individual survey responses and household-level datasets\n'
                    '- SEA/GBV/PSEA case data or complaint records\n'
                    '- Security incident logs with identifiable data\n'
                    '- Location and identity of displaced individuals or vulnerable groups\n'
                    '- Raw data from feedback or complaints mechanisms'
                ),
                'ISO_CODE': 'default',
            }

        return isps
