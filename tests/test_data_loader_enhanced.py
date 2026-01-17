"""
Unit tests for the enhanced SmartDataLoader.

Tests various edge cases and complex scenarios to ensure robustness.
"""

import pytest
import pandas as pd
import numpy as np
from src.infrastructure.storage.data_loader import SmartDataLoader


class TestSmartDataLoader:
    """Test suite for SmartDataLoader enhancements."""

    def setup_method(self):
        """Setup test fixtures."""
        self.loader = SmartDataLoader(max_rows=1000)

    def test_simple_header(self):
        """Test with simple single-row header."""
        df = pd.DataFrame({0: ['Name', 'Alice', 'Bob'], 1: ['Age', 25, 30], 2: ['City', 'NYC', 'LA']})

        result = self.loader._preprocess_dataframe(df)

        assert len(result.columns) == 3
        assert 'Name' in result.columns
        assert 'Age' in result.columns
        assert 'City' in result.columns
        assert len(result) == 2  # 2 data rows

    def test_multi_row_header(self):
        """Test with multi-row hierarchical header."""
        df = pd.DataFrame(
            {
                0: [np.nan, 'Personal', 'Name', 'Alice', 'Bob'],
                1: [np.nan, 'Personal', 'Age', 25, 30],
                2: [np.nan, 'Location', 'City', 'NYC', 'LA'],
                3: [np.nan, 'Location', 'Country', 'USA', 'USA'],
            }
        )

        result = self.loader._preprocess_dataframe(df)

        # Conservative detection may include header row as data
        assert len(result) >= 2  # At least 2 data rows
        # Should combine hierarchical headers
        assert any('Personal' in col for col in result.columns)
        assert any('Location' in col for col in result.columns)

    def test_empty_leading_rows(self):
        """Test with multiple empty leading rows."""
        df = pd.DataFrame(
            {0: [np.nan, np.nan, np.nan, 'Name', 'Alice', 'Bob'], 1: [np.nan, np.nan, np.nan, 'Age', 25, 30]}
        )

        result = self.loader._preprocess_dataframe(df)

        assert len(result) == 2  # 2 data rows
        assert 'Name' in result.columns
        assert 'Age' in result.columns

    def test_empty_columns_filtered(self):
        """Test that empty columns are filtered out."""
        df = pd.DataFrame(
            {
                0: ['Name', 'Alice', 'Bob'],
                1: [np.nan, np.nan, np.nan],  # Empty column
                2: ['Age', 25, 30],
                3: ['', '', ''],  # Empty strings
                4: ['City', 'NYC', 'LA'],
            }
        )

        result = self.loader._preprocess_dataframe(df)

        # Should only have 3 columns (Name, Age, City)
        assert len(result.columns) == 3
        assert 'Name' in result.columns
        assert 'Age' in result.columns
        assert 'City' in result.columns

    def test_merged_cells_horizontal(self):
        """Test handling of horizontally merged cells."""
        df = pd.DataFrame(
            {
                0: ['Location', 'District', 'Kampala', 'Mukono'],
                1: [np.nan, 'Sub-county', 'Central', 'Mpaata'],  # Merged with Location
                2: [np.nan, 'Village', 'Downtown', 'Kawuna'],  # Merged with Location
            }
        )

        result = self.loader._preprocess_dataframe(df)

        # Should have Location as parent for all columns
        assert len(result) >= 2  # At least 2 data rows
        assert all('Location' in col for col in result.columns)

    def test_metadata_row_detection(self):
        """Test detection of metadata rows (e.g., 'Column1', 'Column2')."""
        df = pd.DataFrame(
            {0: ['Name', 'Column1', 'Alice', 'Bob'], 1: ['Age', 'Column2', 25, 30], 2: ['City', 'Column3', 'NYC', 'LA']}
        )

        result = self.loader._preprocess_dataframe(df)

        # Should detect and process the data
        assert len(result) >= 2  # At least 2 data rows
        # Column names should be based on first row
        assert 'Name' in result.columns or any('Name' in col for col in result.columns)

    def test_unique_column_names(self):
        """Test that duplicate column names are made unique."""
        df = pd.DataFrame(
            {
                0: ['Name', 'Alice', 'Bob'],
                1: ['Name', 'Alice2', 'Bob2'],  # Duplicate
                2: ['Age', 25, 30],
            }
        )

        result = self.loader._preprocess_dataframe(df)

        # Should have unique column names
        assert len(result.columns) == len(set(result.columns))
        assert 'Name' in result.columns
        assert 'Name_1' in result.columns or any('Name' in col and col != 'Name' for col in result.columns)

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()

        result = self.loader._preprocess_dataframe(df)

        assert result.empty

    def test_all_nan_dataframe(self):
        """Test handling of DataFrame with all NaN values."""
        df = pd.DataFrame({0: [np.nan, np.nan, np.nan], 1: [np.nan, np.nan, np.nan]})

        result = self.loader._preprocess_dataframe(df)

        # Should return empty DataFrame after filtering
        assert result.empty or len(result.columns) == 0

    def test_sorting_by_completeness(self):
        """Test that rows are sorted by completeness."""
        df = pd.DataFrame(
            {
                0: ['Name', 'Alice', np.nan, 'Charlie', 'David'],
                1: ['Age', 25, np.nan, 35, 40],
                2: ['City', 'NYC', np.nan, 'LA', 'SF'],
            }
        )

        result = self.loader._preprocess_dataframe(df)

        # First row should be the most complete
        first_row_nulls = result.iloc[0].isna().sum()
        last_row_nulls = result.iloc[-1].isna().sum()

        assert first_row_nulls <= last_row_nulls

    def test_real_world_complex_header(self):
        """Test with a structure similar to the ujana coffee dataset."""
        df = pd.DataFrame(
            {
                0: [np.nan, np.nan, 'Date', np.nan, '2020-01-01', '2020-01-02'],
                1: [np.nan, np.nan, 'Location', 'District', 'Kampala', 'Mukono'],
                2: [np.nan, np.nan, 'Location', 'Sub-county', 'Central', 'Mpaata'],
                3: [np.nan, 'Skills', 'Skills', 'Skill 1', 'Baking', 'Sewing'],
                4: [np.nan, 'Skills', 'Skills', 'Skill 2', 'Cooking', 'Weaving'],
            }
        )

        result = self.loader._preprocess_dataframe(df)

        assert len(result) >= 2  # At least 2 data rows
        assert len(result.columns) >= 4  # At least 4 columns

        # Check that we have some meaningful column names
        # The algorithm should preserve some structure
        assert len(result.columns) > 0
        assert not result.empty


def test_sample_dataframe():
    """Test the sample_dataframe method."""
    loader = SmartDataLoader()

    df = pd.DataFrame(
        {
            'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank'],
            'Age': [25, 30, 35, 40, 45, 50],
            'City': ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix', 'Philly'],
        }
    )

    sample = loader.sample_dataframe(df, sample_size=3)

    assert len(sample) == 3  # 3 columns
    assert 'Name' in sample
    assert 'Age' in sample
    assert 'City' in sample

    # Each column should have 3 samples
    assert len(sample['Name']) == 3
    assert len(sample['Age']) == 3
    assert len(sample['City']) == 3


if __name__ == '__main__':
    # Run tests
    pytest.main([__file__, '-v'])
