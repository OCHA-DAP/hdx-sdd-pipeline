"""Process Dataset Use Case - Main orchestration."""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from src.domain.entities import SheetReport, Column, NonPIIClassification, PersonalDataClassification
from src.domain.value_objects import PIIEntityType, SensitivityLevel
from src.domain.exceptions import DataProcessingError
from src.infrastructure.data_loader import SmartDataLoader
from src.infrastructure.openai_provider import OpenAIProvider
from src.shared.utils.prompt_manager import PromptManager

logger = logging.getLogger(__name__)


class ProcessDatasetUseCase:
    """
    Main use case for processing a complete dataset.

    This orchestrates the entire pipeline:
    1. Load data
    2. Create reports
    3. Classify PII
    4. Reflect on PII sensitivity
    5. Classify non-PII
    6. Update sensitivity flags
    """

    def __init__(
        self,
        data_loader: SmartDataLoader,
        pii_llm_provider: Optional[OpenAIProvider] = None,
        pii_reflection_llm_provider: Optional[OpenAIProvider] = None,
        non_pii_llm_provider: Optional[OpenAIProvider] = None,
        readme_llm_provider: Optional[OpenAIProvider] = None,
        prompt_manager: Optional[PromptManager] = None,
        sample_size: int = 5,
    ):
        """
        Initialize use case with dependencies.

        Args:
            data_loader: Data loading implementation
            pii_llm_provider: Optional LLM for PII detection (None to skip)
            pii_reflection_llm_provider: Optional LLM for PII sensitivity (None to skip)
            non_pii_llm_provider: Optional LLM for non-PII classification (None to skip)
            readme_llm_provider: Optional LLM for README PII scanning (None to skip)
            prompt_manager: Prompt template manager
            sample_size: Number of samples per column
        """
        self.data_loader = data_loader
        self.pii_llm = pii_llm_provider
        self.pii_reflection_llm = pii_reflection_llm_provider
        self.non_pii_llm = non_pii_llm_provider
        self.readme_llm = readme_llm_provider
        self.prompt_manager = prompt_manager or PromptManager()
        self.sample_size = sample_size

    def execute(
        self,
        source: str,
        resource_id: Optional[str] = None,
        is_url: bool = True,
        isp_rules: Optional[Dict[str, Any]] = None,
        http_headers: Optional[Dict[str, str]] = None,
    ) -> List[SheetReport]:
        """
        Process a dataset from URL or file.

        Args:
            source: URL or file path
            resource_id: Optional resource identifier
            is_url: True if source is URL, False if file path
            isp_rules: Information Sensitivity Protocol rules
            http_headers: Optional HTTP headers for URL downloads (e.g. auth tokens)

        Returns:
            List of processed SheetReports

        Raises:
            DataProcessingError: If processing fails
        """
        logger.info(
            f'Starting dataset processing: source={source}, resource_id={resource_id}, '
            f'is_url={is_url}, has_isp_rules={isp_rules is not None}'
        )
        import time

        start_time = time.time()

        try:
            # Step 1: Load data
            logger.debug('Step 1: Loading data...')

            # Fail-safe: Check if source is a URL even if is_url is False
            actual_is_url = is_url
            if not is_url and source and source.startswith(('http://', 'https://')):
                logger.warning(f'Source looks like a URL but is_url=False. Overriding to True: {source}')
                actual_is_url = True

            if actual_is_url:
                sheets = self.data_loader.load_from_url(source, http_headers=http_headers)
            else:
                logger.info(f'Loading from file: {source}')
                sheets = self.data_loader.load_from_file(source)

            logger.info(f'Loaded {len(sheets)} sheet(s): {list(sheets.keys())}')

            # Step 2: Create reports for each sheet
            reports = []
            for idx, (sheet_name, df) in enumerate(sheets.items(), 1):
                logger.info(
                    f"Processing sheet {idx}/{len(sheets)}: '{sheet_name}' ({len(df)} rows, {len(df.columns)} columns)"
                )

                # Check if it's a README sheet
                if self._is_readme_sheet(sheet_name):
                    logger.debug(f"Sheet '{sheet_name}' identified as README/metadata")
                    report = self._create_readme_report(sheet_name, source, resource_id, df)
                else:
                    report = self._create_data_report(sheet_name, source, resource_id, df, isp_rules)

                reports.append(report)
                logger.debug(f"Completed processing sheet '{sheet_name}'")

            elapsed_time = time.time() - start_time
            total_tokens = sum(r.total_tokens() for r in reports)
            logger.info(
                f'Successfully processed {len(reports)} sheet(s) in {elapsed_time:.2f}s, total_tokens={total_tokens}'
            )
            return reports

        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(
                f'Failed to process dataset after {elapsed_time:.2f}s: {e}',
                exc_info=True,
                extra={'source': source, 'resource_id': resource_id},
            )
            raise DataProcessingError(f'Dataset processing failed: {e}')

    def _is_readme_sheet(self, sheet_name: str) -> bool:
        """Check if sheet is a README/metadata sheet."""
        normalized = sheet_name.lower().replace(' ', '')
        return any(keyword in normalized for keyword in ['readme', 'instructions', 'metadata', 'info'])

    def _create_readme_report(self, sheet_name: str, source: str, resource_id: Optional[str], df: Any) -> SheetReport:
        """Create report for README sheet."""
        report = SheetReport(
            resource_id=resource_id,
            file_name=source,
            file_url=source if source.startswith('http') else None,
            sheet_name=sheet_name,
            processing_timestamp=datetime.now(),
            n_records=len(df) if hasattr(df, '__len__') else 0,
            is_readme=True,
        )

        logger.info(f"Sheet '{sheet_name}' identified as README")

        # Process README content for PII if README scanning is enabled
        if self.readme_llm is not None:
            logger.info('Processing README content for PII detection')
            try:
                # Extract README content from dataframe
                readme_content = self._extract_readme_content(df)

                if readme_content:
                    # Process with README PII detection
                    result = self._process_readme_for_pii(readme_content)

                    # Update token counts
                    report.completion_tokens += result.pop('completion_tokens', 0)
                    report.prompt_tokens += result.pop('prompt_tokens', 0)

                    # Store results in report
                    report.readme_content = readme_content
                    report.readme_report = result

                    # Update sensitivity flags based on README PII detection
                    if result.get('personal_data_sensitive', False):
                        report.personal_data_sensitive = True
                        report.personal_data_classification.sensitivity = SensitivityLevel.HIGH_SENSITIVE
                        logger.info(f'PII detected in README: {result.get("personal_data_entities", [])}')
                    else:
                        report.personal_data_classification.sensitivity = SensitivityLevel.NON_SENSITIVE
                        logger.info('No PII detected in README')

                    # Set model name
                    report.readme_model = self.readme_llm.model_name

                else:
                    logger.warning('No readable content found in README sheet')

            except Exception as e:
                logger.error(f'Failed to process README for PII: {e}')
                report.readme_model = f'error: {str(e)}'
        else:
            logger.info('README scanning disabled - skipping PII analysis')

        report.update_risk_levels()
        return report

    def _create_data_report(
        self, sheet_name: str, source: str, resource_id: Optional[str], df: Any, isp_rules: Optional[Dict[str, Any]]
    ) -> SheetReport:
        """Create and process report for data sheet."""
        logger.debug(f"Creating data report for sheet '{sheet_name}' with {len(df)} rows")

        # Sample data
        sample_dict = self.data_loader.sample_dataframe(df, self.sample_size)
        logger.debug(f'Sampled {len(sample_dict)} columns')

        # Create report
        report = SheetReport(
            resource_id=resource_id,
            file_name=source,
            file_url=source if source.startswith('http') else None,
            sheet_name=sheet_name,
            processing_timestamp=datetime.now(),
            n_records=len(df),
            n_columns=len(sample_dict),
        )

        # Create columns
        for col_name, sample_values in sample_dict.items():
            column = Column(name=col_name, sample_values=sample_values)
            report.add_column(column)

        logger.debug(f'Starting classification pipeline for {len(report.columns)} columns')

        # Step 3: Classify PII
        report = self._classify_pii(report)

        # Step 4: Reflect on PII sensitivity
        report = self._classify_pii_sensitivity(report)

        # Step 5: Classify non-PII
        report = self._classify_non_pii(report, isp_rules)

        # Step 6: Update sensitivity flags
        report.update_non_pii_sensitivity()
        report.update_risk_levels()

        logger.debug(
            f"Data report complete for '{sheet_name}': "
            f'personal_data_sensitive={report.personal_data_sensitive}, '
            f'pii_sensitivity={report.personal_data_classification.sensitivity}, '
            f'pii_explanation="{report.personal_data_classification.explanation}", '
            f'non_pii_sensitivity={report.non_pii_classification.sensitivity}, '
            f'tokens={report.total_tokens()}'
        )

        return report

    def _classify_pii(self, report: SheetReport) -> SheetReport:
        """Classify PII for all columns."""
        if self.pii_llm is None:
            logger.info('PII classification disabled - skipping')
            return report

        logger.info(f'Classifying PII for {len(report.columns)} columns')

        for column in report.columns:
            if not column.has_valid_samples():
                column.pii_classification.entity_type = PIIEntityType.NONE
                continue

            # Heuristic: Latitude/Longitude columns are Geo Coordinates
            normalized_name = column.name.lower().strip()
            if normalized_name in ('latitude', 'longitude'):
                column.pii_classification.entity_type = PIIEntityType.GEO_COORDINATES
                logger.info(f"Heuristic: Column '{column.name}' classified as GEO_COORDINATES")
                continue

            try:
                # Render prompt (use latest version)
                prompt = self.prompt_manager.get_prompt(
                    'pii_detection',
                    version=None,  # Auto-detect latest version
                    context={'column_name': column.name, 'sample_values': column.sample_values},
                )

                # Call LLM
                result, comp_tokens, prompt_tokens = self.pii_llm.generate(prompt, max_tokens=8)

                # Parse result
                entity_type = PIIEntityType.from_string(result)
                if entity_type in (PIIEntityType.UNDETERMINED, PIIEntityType.UNKNOWN):
                    logger.warning(
                        f"PII classification returned {entity_type.value} for column '{column.name}'. "
                        f'Raw response: {repr(result)}'
                    )
                    column.pii_classification.entity_type = PIIEntityType.UNKNOWN
                    column.pii_classification.sensitive = True
                else:
                    column.pii_classification.entity_type = entity_type

                # Update token counts
                report.completion_tokens += comp_tokens
                report.prompt_tokens += prompt_tokens

                logger.debug(f"Column '{column.name}': {column.pii_classification.entity_type}")

            except Exception as e:
                logger.error(f"PII classification failed for column '{column.name}': {e}")
                column.pii_classification.entity_type = PIIEntityType.UNKNOWN
                column.pii_classification.sensitive = True

        report.pii_classifier_model = self.pii_llm.model_name
        return report

    def _classify_pii_sensitivity(self, report: SheetReport) -> SheetReport:
        """Classify sensitivity for PII columns."""
        if self.pii_reflection_llm is None:
            logger.info('PII sensitivity classification disabled - skipping')
            return report

        # Check if any PII columns were found
        if not report.has_pii_columns():
            logger.info('No PII columns found - skipping sensitivity classification')
            report.personal_data_classification.sensitivity = SensitivityLevel.NON_SENSITIVE
            report.personal_data_classification.explanation = 'No PII columns detected in the dataset.'
            report.personal_data_sensitive = False
            report.pii_reflection_model = 'skipped - no PII columns'
            return report

        # Check if the only pii entities detected are none or organization_name
        # then set personal data sensitive on false and skip as well
        pii_entity_types = [column.pii_classification.entity_type for column in report.columns]
        if all(
            entity_type in [PIIEntityType.NONE, PIIEntityType.ORGANIZATION_NAME] for entity_type in pii_entity_types
        ):
            logger.info('Only NONE or ORGANIZATION_NAME PII entities detected - skipping sensitivity classification')
            report.personal_data_classification.sensitivity = SensitivityLevel.NON_SENSITIVE
            report.personal_data_classification.explanation = (
                'Only NONE or ORGANIZATION_NAME PII entities detected. '
                'Organization names are not considered personal data.'
            )
            report.personal_data_sensitive = False
            report.pii_reflection_model = 'skipped - only NONE or ORGANIZATION_NAME PII entities detected'
            return report

        # If email, phone number, person names are in detected entities, set to sensitive by default and skip
        sensitive_pii_entities = {
            PIIEntityType.EMAIL_ADDRESS,
            PIIEntityType.PHONE_NUMBER,
            PIIEntityType.PERSON_NAME,
            PIIEntityType.GEO_COORDINATES,
        }

        if any(entity_type in sensitive_pii_entities for entity_type in pii_entity_types):
            logger.info(
                'Sensitive PII entities (email, phone number, or person names) detected'
                ' - setting as sensitive and skipping reflection'
            )
            # Set individual column sensitivity flags
            for column in report.columns:
                if column.pii_classification.entity_type != PIIEntityType.NONE:
                    column.pii_classification.sensitive = True

            # Set PII sensitivity classification to HIGH_SENSITIVE since we detected sensitive entities
            report.personal_data_classification.sensitivity = SensitivityLevel.HIGH_SENSITIVE
            report.personal_data_classification.explanation = (
                'Highly sensitive PII entities detected (email, phone number, or person names). '
                'These are direct identifiers that can be used to identify individuals.'
            )

            report.personal_data_sensitive = True
            report.pii_reflection_model = (
                'skipped - sensitive PII entities detected (email, phone number, or person names)'
            )
            # Note: No reflection tokens added since we skipped the LLM call
            return report

        try:
            # Generate table markdown context for all columns
            table_markdown = self._generate_table_markdown(report)

            # Render prompt with table context (use latest version)
            prompt = self.prompt_manager.get_prompt(
                'pii_reflection',
                version=None,  # Auto-detect latest version
                context={
                    'table_markdown': table_markdown,
                },
            )
            # Call LLM
            result, comp_tokens, prompt_tokens = self.pii_reflection_llm.generate_json(prompt, max_tokens=1024)
            logger.debug(f'PII sensitivity classification result: {result}')

            # Parse JSON result using the new entity
            report.personal_data_classification = PersonalDataClassification.from_dict(result)

            if report.personal_data_classification.sensitivity == SensitivityLevel.UNDETERMINED:
                logger.warning(
                    f"PII sensitivity classification returned UNDETERMINED for sheet '{report.sheet_name}'. "
                    f'Raw response: {repr(result)}'
                )

            # Update the legacy boolean flag for backward compatibility
            # True for both MODERATE_SENSITIVE and HIGH_SENSITIVE
            report.personal_data_sensitive = report.personal_data_classification.sensitivity.is_sensitive()

            # Update column sensitivity flags based on the classification result
            for column in report.columns:
                if report.personal_data_sensitive:
                    # Set sensitive=True only for columns with entity_type != 'None'
                    column.pii_classification.sensitive = column.pii_classification.entity_type != PIIEntityType.NONE
                else:
                    # Set sensitive=False for all columns
                    column.pii_classification.sensitive = False

            # Update token counts
            report.completion_tokens += comp_tokens
            report.prompt_tokens += prompt_tokens

        except Exception as e:
            logger.error(f'PII sensitivity classification failed: {e}')
            report.personal_data_classification.sensitivity = SensitivityLevel.HIGH_SENSITIVE
            report.personal_data_classification.explanation = f'Classification failed due to an error: {e}'
            report.personal_data_sensitive = True
            for column in report.columns:
                if column.pii_classification.entity_type != PIIEntityType.NONE:
                    column.pii_classification.sensitive = True

        report.pii_reflection_model = self.pii_reflection_llm.model_name

        return report

    def _classify_non_pii(self, report: SheetReport, isp_rules: Optional[Dict[str, Any]]) -> SheetReport:
        """Classify non-PII sensitivity for the table."""
        if self.non_pii_llm is None:
            logger.info('Non-PII classification disabled - skipping')
            return report

        logger.info('Classifying non-PII sensitivity')

        try:
            # Prepare table summary
            table_summary = self._generate_table_markdown(report)

            # Determine prompt version based on ISP
            version = None
            if isp_rules and isp_rules.get('country') == 'default':
                version = 'v2'

            # Render prompt
            prompt = self.prompt_manager.get_prompt(
                'non_pii_classification',
                version=version,  # Auto-detect latest version unless default ISP
                context={'table_name': report.sheet_name, 'table_markdown': table_summary, 'isp': isp_rules or {}},
            )
            # Log prompt for debugging
            logger.debug(f"[Non-PII Classification] Prompt for table '{report.sheet_name}':\n{prompt}\n")

            # Call LLM
            result, comp_tokens, prompt_tokens = self.non_pii_llm.generate_json(prompt, max_tokens=1024)

            report.non_pii_classification = NonPIIClassification.from_dict(result)

            # Promoted UNDETERMINED sensitivity to SEVERE_SENSITIVE as a safe default
            if report.non_pii_classification.sensitivity == SensitivityLevel.UNDETERMINED:
                logger.warning(
                    f"Non-PII classification returned UNDETERMINED for sheet '{report.sheet_name}'. "
                    f'Raw response: {repr(result)}'
                )
                report.non_pii_classification.sensitivity = SensitivityLevel.SEVERE_SENSITIVE
                msg = 'Classification returned UNDETERMINED. Promoted to SEVERE_SENSITIVE as a safe default.'
                if report.non_pii_classification.explanation:
                    report.non_pii_classification.explanation = (
                        f'{msg} Original explanation: {report.non_pii_classification.explanation}'
                    )
                else:
                    report.non_pii_classification.explanation = msg

            # Store ISP name if provided
            if isp_rules:
                # Extract ISP name - could be from 'country' field or use a default
                isp_name = isp_rules.get('country', 'unknown')
                report.non_pii_classification.isp_name = isp_name

            # Update token counts
            report.completion_tokens += comp_tokens
            report.prompt_tokens += prompt_tokens

            logger.info(f'Non-PII sensitivity: {report.non_pii_classification.sensitivity}')

        except Exception as e:
            logger.error(f'Non-PII classification failed: {e}')
            report.non_pii_classification.sensitivity = SensitivityLevel.SEVERE_SENSITIVE
            report.non_pii_classification.explanation = f'Classification failed due to an error: {e}'

        report.non_pii_model = self.non_pii_llm.model_name
        return report

    def _generate_table_markdown(self, report: SheetReport) -> str:
        """
        Generate a markdown table from the report columns with PII entity types.

        This creates a table showing column names (with PII entity types if detected)
        alongside their sample values, providing rich context for PII reflection.

        Args:
            report: SheetReport containing columns with classifications

        Returns:
            Markdown table string
        """
        try:
            import pandas as pd
        except ImportError:
            logger.warning('pandas not available - returning simple table context')
            return report.sheet_name

        # Build column samples dict with PII entity types in headers
        column_samples = {}
        for col in report.columns:
            # Add entity type to column name if PII detected
            if col.has_pii():
                key = f'{col.name} - {col.pii_classification.entity_type}'
            else:
                key = col.name
            column_samples[key] = col.sample_values

        # Pad all columns to same length
        if column_samples:
            max_len = max(len(values) for values in column_samples.values())
            for key, values in column_samples.items():
                column_samples[key] = values + [''] * (max_len - len(values))

            # Generate markdown table
            return pd.DataFrame(column_samples).to_markdown(index=False) or ''

        return ''

    def _extract_readme_content(self, df: Any) -> Optional[str]:
        """
        Extract readable content from README dataframe.

        Args:
            df: README sheet dataframe

        Returns:
            Combined text content from all cells, or None if no content found
        """
        try:
            import pandas as pd

            if not isinstance(df, pd.DataFrame):
                logger.warning('README sheet is not a pandas DataFrame')
                return None

            # Combine all non-null values into a single string
            content_parts = []
            for column in df.columns:
                for value in df[column].dropna():
                    # Convert to string and skip if empty or just whitespace
                    str_value = str(value).strip()
                    if str_value and len(str_value) > 1:  # Skip single chars
                        content_parts.append(str_value)

            if content_parts:
                return '\n'.join(content_parts)
            else:
                return None

        except Exception as e:
            logger.error(f'Failed to extract README content: {e}')
            return None

    def _process_readme_for_pii(self, readme_content: str) -> Dict[str, Any]:
        """
        Process README content for PII detection using the readme_scan template.

        Args:
            readme_content: Text content from README sheet

        Returns:
            PII detection result dictionary
        """
        try:
            # Render the README scan prompt
            prompt = self.prompt_manager.get_prompt(
                'readme_scan',
                version=None,  # Auto-detect latest version
                context={'readme_string': readme_content},
            )

            # Call LLM for JSON response
            result, comp_tokens, prompt_tokens = self.readme_llm.generate_json(prompt, max_tokens=512)

            # Validate result structure
            if not isinstance(result, dict):
                logger.error(f'README PII detection returned non-dict result: {result}')
                return {
                    'personal_data_sensitive': False,
                    'personal_data_entities': [],
                    'evidence': [],
                    'error': 'Invalid result format',
                }

            # Ensure required fields exist
            validated_result = {
                'personal_data_sensitive': result.get(
                    'personal_data_sensitive', result.get('contains_personal_data', False)
                ),
                'personal_data_entities': result.get('personal_data_entities', result.get('personal_data_types', [])),
                'evidence': result.get('evidence', []),
                'completion_tokens': comp_tokens,
                'prompt_tokens': prompt_tokens,
            }

            logger.info(
                f'README PII analysis completed: personal_data_sensitive={validated_result["personal_data_sensitive"]}'
            )

            return validated_result

        except Exception as e:
            logger.error(f'Failed to process README for PII: {e}')
            return {'personal_data_sensitive': False, 'personal_data_entities': [], 'evidence': [], 'error': str(e)}
