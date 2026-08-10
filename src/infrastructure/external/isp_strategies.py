import json
import re
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# Ordered from least to most restrictive
SENSITIVITY_ORDER = [
    'low/no sensitivity',
    'medium sensitivity',
    'high sensitivity',
    'severe sensitivity',
]

# Maps country names (as they appear in the Google Sheet) to ISO3 codes
COUNTRY_MAPPING_ISO = {
    'afghanistan': 'AFG',
    'burkina faso': 'BFA',
    'burundi': 'BDI',
    'cameroon (nwsw)': 'CMR',
    'cameroon nwsw': 'CMR',
    'cameroon (extreme nord)': 'CMR',
    'democratic republic of the congo': 'COD',
    'drc': 'COD',
    'ethiopia': 'ETH',
    'haiti': 'HTI',
    'iraq': 'IRQ',
    'mozambique': 'MOZ',
    'myanmar': 'MMR',
    'niger': 'NER',
    'palestin': 'PSE',
    'opt': 'PSE',
    'pakistan': 'PAK',
    'somalia': 'SOM',
    'somalia 2': 'SOM',
    'south sudan': 'SSD',
    'sudan': 'SDN',
    'syria': 'SYR',
    'ukraine': 'UKR',
    'ukraine 2': 'UKR',
    'venezuela': 'VEN',
    'yemen': 'YEM',
    'default': 'default',
}

CATEGORY_EXPANSIONS = {
    '3W': 'Who does What Where (3W)',
    'COD': 'Common Operational Datasets (COD)',
    'HNO, HRP and Appeals': ('Humanitarian Needs Overview (HNO), Humanitarian Response Plan (HRP) and Appeals'),
    'AAP': 'Accountability to Affected Populations (AAP)',
    'CASH Monitoring': 'Cash and Voucher Assistance (CVA) Monitoring',
}

ADMIN_LEVELS = (
    'Administrative Levels:\n'
    '- Admin Level 0 (National): Sovereign borders of a country.\n'
    '- Admin Level 1 (First Subdivision): States, provinces, regions, or governorates.\n'
    '- Admin Level 2 (Second Subdivision): Districts, counties, municipalities, or cities.\n'
    '- Admin Level 3 (Third Subdivision): Townships, wards, sub-districts, municipalities.\n'
    '- Admin Level 4 and Below: Villages, communes, neighborhoods, or similar granular units.\n'
    '\n'
    'Data Granularity:\n'
    '- Community Level Data: Village, camp, settlement, or small population group.\n'
    '- Household or Individual Data: Personally identifiable or household-specific data.\n'
    '- Raw Data: Non-aggregated operational or assessment data.'
)

DEFAULT_SEVERE_EXCEPTIONS = (
    '- Survey/monitoring data at community or household level\n'
    '- Individual survey responses and household-level datasets\n'
    '- SEA/GBV/PSEA case data or complaint records\n'
    '- Security incident logs with identifiable data\n'
    '- Location and identity of displaced individuals or vulnerable groups\n'
    '- Raw data from feedback or complaints mechanisms\n'
    '- Health and education facility data with exact coordinates\n'
    '- Detailed access constraints or operational presence by location (below Admin Level 2)\n'
    '- Sensitive programmatic data (e.g., protection activities by location below Admin Level 0)\n'
    '- Highly sensitive data where disclosure could cause serious harm or legal consequences'
)


def expand_category(category: str) -> str:
    """Expand abbreviated category names to their full descriptive form."""
    return CATEGORY_EXPANSIONS.get(category, category)


def _sensitivity_to_key(sensitivity: str) -> str:
    """Convert a sensitivity label to its dict key form (e.g. 'low/no sensitivity' → 'low_no_sensitivity')."""
    return sensitivity.replace('/', '_').replace(' ', '_')


def _sensitivity_to_mapped_key(sensitivity: str) -> str:
    """Convert spreadsheet sensitivity level to the key used in newer prompt templates."""
    mapping = {
        'low/no sensitivity': 'LOW/NON_SENSITIVE',
        'medium sensitivity': 'MEDIUM_SENSITIVE',
        'high sensitivity': 'HIGH_SENSITIVE',
        'severe sensitivity': 'SEVERE_SENSITIVE',
    }
    return mapping.get(sensitivity.lower().strip(), 'LOW/NON_SENSITIVE')


ACRONYMS = {
    'AAP': 'Accountability to Affected Populations',
    'SEA': 'Sexual Exploitation and Abuse',
    'VBG': 'Violence Base sur le Genre',
    'COD': 'Common operational data set',
    'GBV': 'Gender Based Violence',
    'HH Level': 'household level',
    'HNO': 'Humanitarian Needs Overview',
    'HRP': 'Humanitarian Response Plan',
    'PIN': 'People in Need',
    'PDI': 'Personnes Déplacées Internes',
    'MSNA': 'multisectoral Needs Assessment',
    'RRM': 'Rapid Response Mechanism',
    'SMART': 'Standardized Monitoring and Assessment of Relief and Transitions',
    'IPC': 'Integrated Food Security Phase Classification',
    'Sitrep': 'Situation Reports',
    'SAT': 'Sites d’Accueil Temporaires',
    'ZAD': 'Zones d’Accueil de Déplacés',
    'MAM': 'Malnutrition Aiguë Modérée (moderate acute malnutrition)',
    'MAS': 'Malnutrition Aiguë Sévère (Severe Acute Malnutrition)',
    'MAG': 'Malnutrition Aiguë Globale (Global Acute Malnutrition)',
    'BSFP': 'Blanket Supplementary Feeding Programme',
    'PCIMA': 'Prise en Charge Intégrée de la Malnutrition Aiguë',
    'HERAMS': 'Health resources and services availability mapping system',
    'SSA': 'Surveillance System for Attacks on Health Care',
    'AME': 'Non-Food Items (used for the Shelter and Non-Food Items (NFI) Cluster)',
    'COD AB': 'Common Operational Dataset-Administrative Boundaries',
    'COD PS': 'Common Operational Dataset for Population Statistics',
    'ETT': 'Emergency Trend Tracking',
    'DTM': 'Displacement Tracking Matrix',
    'FGD': 'Focus Group Discussions',
    'KII': 'Key Informant Interviews',
    'IM': 'Information Management',
    'PSEA': 'Protection from Sexual Exploitation and Abuse',
    'UXOs': 'Unexploded Ordnances',
    'ERW': 'Explosive Remnants of War',
    'ICSM': 'Initiative Conjointe de Suivi des Marchés',
    'HSM': 'Humanitarian Situation Monitoring',
    'ENSAN': 'Enquête Nationale sur la Sécurité Alimentaire et Nutritionnelle',
    'AMRF': 'access monitoring and reporting framework',
    'AoI': 'Area of Influence',
    'CwC': 'Communicating with Communities',
    'PEAS': "Protection contre l'Exploitation et les Abus Sexuels",
    'SIDA': "syndrome d'immunodéficience acquise",
    'FOD': 'Fundamental Operational Dataset',
    'MHR': 'Matrice harmonisée des réalisations',
    'MSA': 'multi-sectoral needs assessments',
    'IDP': 'Internally Displaced Person',
    'WASH': 'Water, Sanitation, and Hygiene',
    'MSRNA': 'Multi-Sectoral Rapid Needs Assessment',
    'OPZ': 'Operational Zone',
    'MUAC': 'Mid-Upper Arm Circumference',
    'HNRP': 'Humanitarian Needs and Response Plan',
    'FSNMS': 'Food Security and Nutrition Monitoring System',
    'ISNA': 'Inter-Sector Needs Assessment',
    'IRNA': 'Inter-agency rapid needs assessment',
    'SDG': 'Sustainable Development Goals',
    'IDSR': 'Integrated Disease Surveillance and Response data',
    'HISM': 'Humanitarian Information Sharing Mechanism',
    'CFM': 'Community Feedback Mechanism',
    'EO': 'Explosive ordnance',
    'LOGs': 'Logistics Cluster',
    'SADD': 'Sex- and Age-Disaggregated Data',
    'GIS': 'Geographic Information System',
    'CAAFAG': 'Children Associated with Armed Forces and Armed Groups',
    'AORs': 'Areas of Responsibility',
    'SOPs': 'Standard Operating Procedure',
}


def _find_abbreviations(text: str, acronyms: Dict[str, str] = None) -> Dict[str, str]:
    if acronyms is None:
        acronyms = ACRONYMS
    found = {}
    if not text:
        return found
    sorted_acronyms = sorted(acronyms.keys(), key=len, reverse=True)
    for acronym in sorted_acronyms:
        definition = acronyms[acronym]
        pattern = r'\b' + re.escape(acronym) + r'\b'
        if re.search(pattern, text):
            found[acronym] = definition
            text = re.sub(pattern, '', text)
    return found


def _build_text_blob_for_level(rules: List[Dict[str, str]], sensitivity: str, acronyms: Dict[str, str] = None) -> str:
    """
    Generate a backward-compatible text blob for a sensitivity level from structured rules.

    This allows the existing templates to continue working.
    """
    level_rules = [r for r in rules if r.get('sensitivity', '').lower() == sensitivity]
    if not level_rules:
        return ''

    lines = []
    for rule in level_rules:
        data_type = rule.get('data_type', '').strip()
        parts = [data_type]

        details = []
        category = rule.get('category', '').strip()
        if category:
            details.append(f'Category: {category}')
        disaggregation = rule.get('disaggregation', '').strip()
        if disaggregation:
            details.append(f'Lowest Disaggregation Level: {disaggregation}')

        if details:
            parts.append(f'({", ".join(details)})')

        rule_str = ' '.join(parts)
        instructions = rule.get('instructions', '').strip()
        if instructions:
            rule_str += f'\n-- Extra Rule Interpretation Instructions: {instructions}'

        # Add abbreviations
        combined_text = f'{data_type} {category} {disaggregation} {instructions}'
        abbreviations = _find_abbreviations(combined_text, acronyms)
        if abbreviations:
            abbrev_strs = [f'{k} = {v}' for k, v in abbreviations.items()]
            rule_str += f'\n-- Definitions: {", ".join(abbrev_strs)}'

        lines.append(rule_str)

    return '\n\n- '.join([''] + lines).lstrip('\n') + '\n' if lines else ''


class GoogleSheetsISPStrategy:
    """
    Strategy to retrieve and transform ISP data from Google Sheets.

    Reads the structured "Data & Information Types Dataset" worksheet which contains
    one row per data-type rule with discrete fields: Country, Data/Information Type,
    Category, Lowest Disaggregation, and Sensitivity.
    """

    def __init__(
        self,
        spreadsheet_url: str = 'https://docs.google.com/spreadsheets/d/1Z5wj6H6WV2E8VN9r6y8AfdgOltTNL-z2KIlbnNzzPok/edit?gid=886310371#gid=886310371',
        worksheet_name: str = 'Data & Information Types Dataset',
    ):
        self.spreadsheet_url = spreadsheet_url
        self.worksheet_name = worksheet_name

    def get_isps(self) -> Dict[str, Any]:
        """
        Retrieve ISP data from Google Sheets and transform it into a dictionary
        keyed by country title. Each entry contains structured rules and
        backward-compatible text blobs.
        """
        from src.infrastructure.external.google_sheets_client import get_gsheets

        try:
            spreadsheet = get_gsheets().open_by_url(self.spreadsheet_url)
            worksheet = spreadsheet.worksheet(self.worksheet_name)
            values = worksheet.get_all_values()
            acronyms = self._load_acronyms_from_readme(spreadsheet)
        except Exception as e:
            logger.error(f'Failed to read ISP Google Sheet: {e}')
            return self._build_default_only()

        if not values or len(values) < 2:
            logger.error(
                f'ISP Google Sheet worksheet "{self.worksheet_name}" has insufficient rows '
                f'({len(values) if values else 0}), expected at least 2'
            )
            return self._build_default_only()

        header = [h.strip() for h in values[0]]
        rows = values[1:]

        # Build lookup for column indices
        col_idx = {}
        for idx, col_name in enumerate(header):
            col_idx[col_name] = idx

        # Support both 'Sensitivity' and 'Sensitivity Level'
        sensitivity_col_name = 'Sensitivity Level' if 'Sensitivity Level' in col_idx else 'Sensitivity'

        required_columns = ['Country', sensitivity_col_name]
        for col in required_columns:
            if col not in col_idx:
                logger.error(f'ISP Google Sheet is missing required column "{col}". Available columns: {header}')
                return self._build_default_only()

        # Parse rows into structured rules grouped by ISO3 code
        country_rules: Dict[str, Dict[str, Any]] = {}

        for row_num, row in enumerate(rows, start=2):
            # Safely get cell values
            def cell(name: str) -> str:
                idx = col_idx.get(name)
                if idx is None or idx >= len(row):
                    return ''
                return str(row[idx]).strip()

            country_raw = cell('Country').lower().strip()
            if not country_raw:
                continue

            iso_code = COUNTRY_MAPPING_ISO.get(country_raw)
            if not iso_code:
                logger.debug(f'Row {row_num}: unknown country "{country_raw}", skipping')
                continue

            sensitivity_raw = cell(sensitivity_col_name).lower().strip()
            if sensitivity_raw not in SENSITIVITY_ORDER:
                logger.debug(f'Row {row_num}: unrecognized sensitivity "{sensitivity_raw}", skipping')
                continue

            data_type = cell('Data / Information Type')
            category = expand_category(cell('Category'))
            disaggregation = cell('Lowest Disaggregation')
            instructions = cell('Rule Interpretation Instructions')

            rule = {
                'data_type': data_type,
                'category': category,
                'disaggregation': disaggregation,
                'sensitivity': sensitivity_raw,
                'instructions': instructions,
            }

            # Use the human-readable country name (title case) as key
            country_title = country_raw.title()

            if iso_code not in country_rules:
                country_rules[iso_code] = {
                    'ISO_CODE': iso_code,
                    'country_name': country_title,
                    'country': iso_code,  # Backwards compatibility key checked by event processor
                    'rules': [],
                    'admin_levels': ADMIN_LEVELS,
                }

            country_rules[iso_code]['rules'].append(rule)

        # Build final ISP dict keyed by country_name for readability
        isp_dict: Dict[str, Any] = {}

        for iso_code, data in country_rules.items():
            country_name = data['country_name']
            rules = data['rules']

            # Sort rules by sensitivity order
            rules.sort(key=lambda r: SENSITIVITY_ORDER.index(r.get('sensitivity', '').lower()))

            # Generate backward-compatible text blobs
            for level in SENSITIVITY_ORDER:
                key = _sensitivity_to_key(level)
                data[key] = _build_text_blob_for_level(rules, level, acronyms)

            # Generate the new dictionary structure required by current prompts
            data['sensitivity_rules'] = {}
            for level in SENSITIVITY_ORDER:
                mapped_level = _sensitivity_to_mapped_key(level)
                text_blob = _build_text_blob_for_level(rules, level, acronyms)
                data['sensitivity_rules'][mapped_level] = {'data and information type': [text_blob]}

            isp_dict[country_name] = data

        # Set default key correctly
        # Ensure both 'Default' and 'default' keys are populated with ISO_CODE = 'default' and is_default = True
        if 'default' in country_rules and 'Default' in isp_dict:
            default_entry = isp_dict['Default']
            default_entry['ISO_CODE'] = 'default'
            default_entry['country'] = 'default'
            default_entry['is_default'] = True
            isp_dict['default'] = default_entry
        else:
            default_entry = self._build_default_isp()
            isp_dict['Default'] = default_entry
            isp_dict['default'] = default_entry

        total_unique_rules = sum(
            len(d.get('rules', [])) for k, d in isp_dict.items() if k not in ('default', 'Default')
        )
        logger.info(f'Loaded ISP rules for {len(isp_dict) - 2} countries ({total_unique_rules} total rules)')

        return isp_dict

    def _load_acronyms_from_readme(self, spreadsheet) -> Dict[str, str]:
        """Load acronyms and definitions from the ReadMe worksheet."""
        acronyms = {}
        try:
            worksheet = spreadsheet.worksheet('ReadMe')
            values = worksheet.get_all_values()
        except Exception as e:
            logger.error(f'Failed to read ReadMe worksheet: {e}')
            return {}

        start_parsing = False
        for row in values:
            if not row:
                continue
            first_cell = str(row[0]).strip()
            if 'Acronyms and abbreviations used' in first_cell:
                start_parsing = True
                continue
            if start_parsing:
                if len(row) >= 2:
                    acronym = str(row[0]).strip()
                    definition = str(row[1]).strip()
                    if acronym and definition:
                        acronyms[acronym] = definition
        logger.info(f'Loaded {len(acronyms)} acronyms from ReadMe worksheet')
        return acronyms

    @staticmethod
    def _build_default_isp() -> Dict[str, Any]:
        """Build the default ISP entry using the simplified binary approach."""
        default_data = {
            'ISO_CODE': 'default',
            'country': 'default',
            'is_default': True,
            'admin_levels': ADMIN_LEVELS,
            'default_severe_exceptions': DEFAULT_SEVERE_EXCEPTIONS,
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
            'medium_sensitivity': '',
            'high_sensitivity': '',
            'severe_sensitivity': (
                'Highly sensitive data that, if disclosed, could lead to serious harm or legal consequences. '
                'Sharing is highly restricted.\n'
                '- Individual survey responses and household-level datasets\n'
                '- SEA/GBV/PSEA case data or complaint records\n'
                '- Security incident logs with identifiable data\n'
                '- Location and identity of displaced individuals or vulnerable groups\n'
                '- Raw data from feedback or complaints mechanisms'
            ),
        }

        # Build sensitivity_rules dict structure for default fallback
        default_data['sensitivity_rules'] = {
            'LOW/NON_SENSITIVE': {'data and information type': [default_data['low_no_sensitivity']]},
            'MEDIUM_SENSITIVE': {'data and information type': [default_data['medium_sensitivity']]},
            'HIGH_SENSITIVE': {'data and information type': [default_data['high_sensitivity']]},
            'SEVERE_SENSITIVE': {'data and information type': [default_data['severe_sensitivity']]},
        }
        return default_data

    def _build_default_only(self) -> Dict[str, Any]:
        """Fallback: return only the default ISP when Google Sheets data is unavailable."""
        return {'default': self._build_default_isp()}


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
                'country': 'default',
            }

            isps['default']['sensitivity_rules'] = {
                'LOW/NON_SENSITIVE': {'data and information type': [isps['default']['low_no_sensitivity']]},
                'MEDIUM_SENSITIVE': {'data and information type': [isps['default']['medium_sensitivity']]},
                'HIGH_SENSITIVE': {'data and information type': [isps['default']['high_sensitivity']]},
                'SEVERE_SENSITIVE': {'data and information type': [isps['default']['severe_sensitivity']]},
            }

        return isps
