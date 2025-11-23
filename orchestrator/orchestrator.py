import datetime
import json
import logging

from models.sdd_report import SDDReport
from utils.processing import DataSampler, table_markdown
from utils.ckan import CKANClient
from classifiers.pii_classifier import PIIClassifier
from classifiers.non_pii_classifier import NonPIIClassifier
from classifiers.pii_reflection_classifier import PIIReflectionClassifier
from classifiers.readme_scan import ReadMeScanClassifier

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    High-level pipeline orchestrator for the HDX SDD processing workflow.
    Responsible for:
      - Loading resource metadata
      - Sampling data
      - Running classifiers
      - Producing per-sheet reports
      - Determining sensitivity
      - Updating CKAN
    """

    def __init__(self, config):
        self.config = config
        self.ckan = CKANClient(base_url=config.HDX_URL, api_token=config.HDX_KEY)
        self.pii_classifier = PIIClassifier(model_name=config.PII_DETECT_MODEL)
        self.reflection_classifier = PIIReflectionClassifier(model_name=config.PII_REFLECT_MODEL)
        self.non_pii_classifier = NonPIIClassifier(model_name=config.NON_PII_DETECT_MODEL)
        self.readme_scanner = ReadMeScanClassifier(model_name=config.README_SCAN_MODEL)
        self.sampler = DataSampler()

    def load_isp_info(self, file_name: str) -> dict:
        with open('data/isps.json', 'r') as f:
            isps = json.load(f)
        for isp_name, isp_data in isps.items():
            if isp_data.get('country', '').lower() in file_name.lower():
                return {isp_name: isp_data}
        return {'default': isps.get('default')}

    def process_sheet(self, df, sheet_name, file_name, download_url, resource_id, isp):
        logger.info(f"Processing sheet: {sheet_name}")

        report = SDDReport(
            resource_id=resource_id,
            file_name=file_name,
            file_url=download_url,
            sheet_name=sheet_name,
            processing_timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            processing_success=True,
            n_records=len(df),
            n_columns=len(df.columns),
        )

        report = self.pii_classifier.classify_df(df, report)
        report = self.reflection_classifier.classify_df(table_markdown(report), report)
        report = self.non_pii_classifier.classify(table_markdown(report), report, isp)

        return report.to_dict()

    def determine_sensitivity(self, reports: list) -> str:
        pii_sensitive = [r.get('pii_sensitive') for r in reports]
        non_pii_sensitive = [r.get('non_pii_sensitive') for r in reports]

        if any(pii_sensitive) and any(non_pii_sensitive):
            return 'sensitive-pii-and-non-pii'
        if any(pii_sensitive) and not any(non_pii_sensitive):
            return 'sensitive-pii'
        if not any(pii_sensitive) and any(non_pii_sensitive):
            return 'sensitive-non-pii'
        return 'not-sensitive'

    def _process_sampled_dfs(self, dfs, file_name, download_url, resource_id, isp):
        """Shared logic for processing sampled dataframes for event + local files."""
        reports = []
        for sheet_name, df in dfs.items():
            if any(k in sheet_name.lower() for k in ['readme', 'instrucciones', 'instructions', 'metadata']):
                readme_string = df.to_string()
                report, completion_tokens, prompt_tokens = self.readme_scanner.classify_readme(readme_string)
                reports.append(
                    {
                        'sheet_name': sheet_name,
                        'completion_tokens': completion_tokens,
                        'prompt_tokens': prompt_tokens,
                        'pii_sensitive': report.get('contains_pii', False),
                        'report': report,
                    }
                )
            else:
                reports.append(self.process_sheet(df, sheet_name, file_name, download_url, resource_id, isp))
        return reports

    def process_local_file(self, file_path):
        """
        Process a local CSV/XLSX file (bypassing CKAN + events).
        Returns: (success: bool, reports: list, sensitivity: str)
        """
        logger.info(f"Processing local file: {file_path}")

        file_name = file_path.split('/')[-1]
        isp = self.load_isp_info(file_name)

        # Use DataSampler to load local files
        dfs = self.sampler.sample(file_path)
        reports = self._process_sampled_dfs(dfs, file_name, file_path, "local", isp)

        reports = self._process_sampled_dfs(dfs, file_name, download_url, resource_id, isp)

        sensitivity = self.determine_sensitivity(reports)

        return True, reports, sensitivity

    def process_event(self, event):
        logger.info('Received event: %s', json.dumps(event, ensure_ascii=False, indent=2))
        start = datetime.datetime.now()

        resource_id = event.get('resource_id')
        if not resource_id:
            return False, 'Missing resource_id'

        try:
            resource = self.ckan.resource_show(resource_id)
            if not resource:
                return False, 'Resource not found'

            if resource.get('sdd_report') and not self.config.RERUN:
                return True, 'Already processed'

            download_url = resource.get('download_url')
            file_name = resource.get('name', 'unknown_dataset.csv')
            isp = self.load_isp_info(file_name)

            dfs = self.sampler.sample(download_url)

            reports = []
            for sheet_name, df in dfs.items():
                # README-like sheets
                if any(k in sheet_name.lower() for k in ['readme', 'instrucciones', 'instructions', 'metadata']):
                    readme_string = df.to_string()
                    report, completion_tokens, prompt_tokens = self.readme_scanner.classify_readme(readme_string)
                    reports.append(
                        {
                            'sheet_name': sheet_name,
                            'completion_tokens': completion_tokens,
                            'prompt_tokens': prompt_tokens,
                            'pii_sensitive': report.get('contains_pii', False),
                            'report': report,
                        }
                    )
                else:
                    reports.append(self.process_sheet(df, sheet_name, file_name, download_url, resource_id, isp))

            sensitivity = self.determine_sensitivity(reports)

            # Update CKAN
            self.ckan.update_resource_fields(
                resource_id,
                {
                    'sdd_report': json.dumps(reports, indent=2),
                    'sensitive': sensitivity,
                },
            )

            elapsed = datetime.datetime.now() - start
            logger.info(f"Finished processing {resource_id} ({file_name}) in {elapsed}. Sensitivity: {sensitivity}")

            return True, f'Processed successfully ({sensitivity})'

        except Exception as e:
            logger.exception(f"Error processing resource {resource_id}: {e}")
            return False, str(e)
