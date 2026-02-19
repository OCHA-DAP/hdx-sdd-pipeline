"""Process Dataset Use Case - Main orchestration."""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
import json

from ...domain.entities import SheetReport, Column, NonPIIClassification
from ...domain.value_objects import PIIEntityType, SensitivityLevel
from ...domain.exceptions import DataProcessingError
from ..interfaces import ILLMProvider, IDataLoader
from ...shared.utils.prompt_manager import PromptManager


logger = logging.getLogger(__name__)


def parse_llm_json(s: str):
    s = s.strip()

    if s.startswith('```'):
        # remove ```json and closing ```
        s = s.split('```', 2)[1].strip()

    return json.loads(s)


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
        data_loader: IDataLoader,
        pii_llm_provider: Optional[ILLMProvider] = None,
        pii_reflection_llm_provider: Optional[ILLMProvider] = None,
        non_pii_llm_provider: Optional[ILLMProvider] = None,
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
            prompt_manager: Prompt template manager
            sample_size: Number of samples per column
        """
        self.data_loader = data_loader
        self.pii_llm = pii_llm_provider
        self.pii_reflection_llm = pii_reflection_llm_provider
        self.non_pii_llm = non_pii_llm_provider
        self.prompt_manager = prompt_manager or PromptManager()
        self.sample_size = sample_size

    def execute(
        self,
        source: str,
        resource_id: Optional[str] = None,
        is_url: bool = True,
        isp_rules: Optional[Dict[str, Any]] = None,
    ) -> List[SheetReport]:
        """
        Process a dataset from URL or file.

        Args:
            source: URL or file path
            resource_id: Optional resource identifier
            is_url: True if source is URL, False if file path
            isp_rules: Information Sensitivity Protocol rules

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
            if is_url:
                sheets = self.data_loader.load_from_url(source)
            else:
                print(f'Loading from file: {source}')
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

        logger.info(
            f"Data report complete for '{sheet_name}': "
            f'personal_data_sensitive={report.personal_data_sensitive}, '
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
                column.pii_classification.entity_type = PIIEntityType.from_string(result)

                # Update token counts
                report.completion_tokens += comp_tokens
                report.prompt_tokens += prompt_tokens

                logger.debug(f"Column '{column.name}': {column.pii_classification.entity_type}")

            except Exception as e:
                logger.error(f"PII classification failed for column '{column.name}': {e}")
                column.pii_classification.entity_type = PIIEntityType.UNDETERMINED

        report.pii_classifier_model = self.pii_llm.model_name
        return report

    def _classify_pii_sensitivity(self, report: SheetReport) -> SheetReport:
        """Classify sensitivity for PII columns."""
        if self.pii_reflection_llm is None:
            logger.info('PII sensitivity classification disabled - skipping')
            return report

        # Check if any PII columns were found
        if not report.has_pii_columns():
            print('No PII columns found - skipping sensitivity classification')
            report.personal_data_sensitive = False
            report.pii_reflection_model = 'skipped - no PII columns'
            return report

        # Check if the only pii entities detected are none or organization_name
        # then set personal data sensitive on false and skip as well
        pii_entity_types = [column.pii_classification.entity_type for column in report.columns]
        if all(
            entity_type in [PIIEntityType.NONE, PIIEntityType.ORGANIZATION_NAME] for entity_type in pii_entity_types
        ):
            print('Only NONE or ORGANIZATION_NAME PII entities detected - skipping sensitivity classification')
            report.personal_data_sensitive = False
            report.pii_reflection_model = 'skipped - only NONE or ORGANIZATION_NAME PII entities detected'
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
            result, comp_tokens, prompt_tokens = self.pii_reflection_llm.generate(prompt, max_tokens=16)

            # Parse result (expecting "sensitive" or "non_sensitive")
            result_lower = result.lower()
            if 'non_sensitive' in result_lower or 'non-sensitive' in result_lower:
                is_sensitive = False
            elif 'sensitive' in result_lower:
                is_sensitive = True
            else:
                is_sensitive = True  # Default to sensitive if unclear

            report.personal_data_sensitive = is_sensitive

            # Set sensitive flag on each column based on the logic:
            # - If personal_data_sensitive is True: set sensitive=True for columns with entity_type != 'None'
            # - If personal_data_sensitive is False: set sensitive=False for all columns
            for column in report.columns:
                if is_sensitive:
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
            # Default to sensitive on error (fail safe)
            report.personal_data_sensitive = True
            # Set sensitive=True for columns with entity_type != 'None'
            for column in report.columns:
                column.pii_classification.sensitive = column.pii_classification.entity_type != PIIEntityType.NONE

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

            # Render prompt (use latest version)
            prompt = self.prompt_manager.get_prompt(
                'non_pii_classification',
                version=None,  # Auto-detect latest version
                context={'table_name': report.sheet_name, 'table_markdown': table_summary, 'isp': isp_rules or {}},
            )

            # Log prompt for debugging
            logger.debug(f"[Non-PII Classification] Prompt for table '{report.sheet_name}':\n{prompt}\n")

            # Call LLM
            result, comp_tokens, prompt_tokens = self.non_pii_llm.generate_json(prompt, max_tokens=1024)

            report.non_pii_classification = NonPIIClassification.from_dict(result)

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
            report.non_pii_classification.sensitivity = SensitivityLevel.UNDETERMINED

        report.non_pii_model = self.non_pii_llm.model_name
        return report

    def _extract_sensitivity_from_text(self, text: str) -> SensitivityLevel:
        """
        Extract sensitivity level from LLM response text.

        Handles formats like:
        - "Classification: MODERATE_SENSITIVE\n\nExplanation: ..."
        - "MODERATE_SENSITIVE"
        - "The classification is MODERATE_SENSITIVE because..."

        Args:
            text: LLM response text

        Returns:
            Extracted SensitivityLevel
        """
        if not text:
            return SensitivityLevel.UNDETERMINED

        # Try to extract from "Classification: LEVEL" format
        if 'classification:' in text.lower():
            lines = text.split('\n')
            for line in lines:
                if 'classification:' in line.lower():
                    # Extract the part after "Classification:"
                    parts = line.split(':', 1)
                    if len(parts) > 1:
                        level_text = parts[1].strip()
                        # Try to parse this
                        level = SensitivityLevel.from_string(level_text)
                        if level != SensitivityLevel.UNDETERMINED:
                            return level

        # Try to find sensitivity keywords in the text
        text_upper = text.upper()

        # Check for each sensitivity level (most specific first)
        if 'SEVERE_SENSITIVE' in text_upper or 'SEVERE-SENSITIVE' in text_upper:
            return SensitivityLevel.SEVERE_SENSITIVE
        if 'HIGH_SENSITIVE' in text_upper or 'HIGH-SENSITIVE' in text_upper:
            return SensitivityLevel.HIGH_SENSITIVE
        if 'MODERATE_SENSITIVE' in text_upper or 'MODERATE-SENSITIVE' in text_upper:
            return SensitivityLevel.MODERATE_SENSITIVE
        if 'MEDIUM_SENSITIVE' in text_upper or 'MEDIUM-SENSITIVE' in text_upper:
            return SensitivityLevel.MEDIUM_SENSITIVE
        if 'NON_SENSITIVE' in text_upper or 'NON-SENSITIVE' in text_upper:
            return SensitivityLevel.NON_SENSITIVE

        # Fallback to the original from_string method
        return SensitivityLevel.from_string(text)

    def _create_table_summary(self, report: SheetReport) -> str:
        """Create a summary of the table for non-PII classification."""
        summary_parts = [
            f'Table: {report.sheet_name}',
            f'Rows: {report.n_records}',
            f'Columns: {report.n_columns}',
            '\nColumn Overview:',
        ]

        for column in report.columns[:10]:  # First 10 columns
            pii_info = f' (PII: {column.pii_classification.entity_type})' if column.has_pii() else ''
            summary_parts.append(f'- {column.name}{pii_info}')

        if len(report.columns) > 10:
            summary_parts.append(f'... and {len(report.columns) - 10} more columns')

        return '\n'.join(summary_parts)

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
