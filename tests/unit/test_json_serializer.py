"""Tests for JSON serialization utilities."""

import json
from datetime import datetime, date
import numpy as np
import pandas as pd
from src.shared.utils.json_serializer import make_json_serializable


class TestMakeJsonSerializable:
    """Test suite for make_json_serializable function."""

    def test_none_value(self):
        """Test that None is preserved."""
        assert make_json_serializable(None) is None

    def test_basic_types(self):
        """Test that basic JSON-serializable types are preserved."""
        assert make_json_serializable('string') == 'string'
        assert make_json_serializable(42) == 42
        assert make_json_serializable(3.14) == 3.14
        assert make_json_serializable(True) is True
        assert make_json_serializable(False) is False

    def test_datetime_conversion(self):
        """Test datetime objects are converted to ISO format."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        result = make_json_serializable(dt)
        assert isinstance(result, str)
        assert result == '2024-01-15T10:30:45'

    def test_date_conversion(self):
        """Test date objects are converted to ISO format."""
        d = date(2024, 1, 15)
        result = make_json_serializable(d)
        assert isinstance(result, str)
        assert result == '2024-01-15'

    def test_pandas_timestamp_conversion(self):
        """Test pandas Timestamp is converted to ISO format."""
        ts = pd.Timestamp('2024-01-15 10:30:45')
        result = make_json_serializable(ts)
        assert isinstance(result, str)
        assert '2024-01-15' in result

    def test_numpy_integer_conversion(self):
        """Test numpy integer types are converted to Python int."""
        np_int = np.int64(42)
        result = make_json_serializable(np_int)
        assert isinstance(result, int)
        assert result == 42

    def test_numpy_float_conversion(self):
        """Test numpy float types are converted to Python float."""
        np_float = np.float64(3.14)
        result = make_json_serializable(np_float)
        assert isinstance(result, float)
        assert abs(result - 3.14) < 0.001

    def test_numpy_array_conversion(self):
        """Test numpy arrays are converted to lists."""
        np_array = np.array([1, 2, 3, 4, 5])
        result = make_json_serializable(np_array)
        assert isinstance(result, list)
        assert result == [1, 2, 3, 4, 5]

    def test_numpy_2d_array_conversion(self):
        """Test 2D numpy arrays are converted to nested lists."""
        np_array = np.array([[1, 2], [3, 4]])
        result = make_json_serializable(np_array)
        assert isinstance(result, list)
        assert result == [[1, 2], [3, 4]]

    def test_nan_conversion(self):
        """Test NaN values are converted to None."""
        assert make_json_serializable(float('nan')) is None
        assert make_json_serializable(np.nan) is None
        assert make_json_serializable(pd.NA) is None

    def test_list_conversion(self):
        """Test lists are processed recursively."""
        input_list = [1, 'string', datetime(2024, 1, 15), np.int64(42), None]
        result = make_json_serializable(input_list)
        assert isinstance(result, list)
        assert result[0] == 1
        assert result[1] == 'string'
        assert isinstance(result[2], str)
        assert result[3] == 42
        assert result[4] is None

    def test_tuple_conversion(self):
        """Test tuples are converted to lists."""
        input_tuple = (1, 2, 3)
        result = make_json_serializable(input_tuple)
        assert isinstance(result, list)
        assert result == [1, 2, 3]

    def test_nested_tuple_conversion(self):
        """Test nested tuples are converted to nested lists."""
        input_tuple = (1, (2, 3), (4, (5, 6)))
        result = make_json_serializable(input_tuple)
        assert result == [1, [2, 3], [4, [5, 6]]]

    def test_dict_conversion(self):
        """Test dictionaries are processed recursively."""
        input_dict = {
            'string': 'value',
            'number': 42,
            'datetime': datetime(2024, 1, 15),
            'numpy': np.int64(100),
            'none': None,
        }
        result = make_json_serializable(input_dict)
        assert isinstance(result, dict)
        assert result['string'] == 'value'
        assert result['number'] == 42
        assert isinstance(result['datetime'], str)
        assert result['numpy'] == 100
        assert result['none'] is None

    def test_nested_dict_conversion(self):
        """Test nested dictionaries are processed recursively."""
        input_dict = {'level1': {'level2': {'datetime': datetime(2024, 1, 15), 'numpy': np.array([1, 2, 3])}}}
        result = make_json_serializable(input_dict)
        assert isinstance(result['level1']['level2']['datetime'], str)
        assert result['level1']['level2']['numpy'] == [1, 2, 3]

    def test_mixed_nested_structures(self):
        """Test complex nested structures with mixed types."""
        input_data = {
            'list': [1, 2, datetime(2024, 1, 15)],
            'dict': {'nested': np.int64(42)},
            'tuple': (1, 2, 3),
            'numpy_array': np.array([1, 2, 3]),
        }
        result = make_json_serializable(input_data)

        # Verify it's JSON serializable
        json_str = json.dumps(result)
        assert isinstance(json_str, str)

        # Verify structure
        assert isinstance(result['list'], list)
        assert isinstance(result['dict'], dict)
        assert isinstance(result['tuple'], list)
        assert isinstance(result['numpy_array'], list)

    def test_empty_collections(self):
        """Test empty collections are handled correctly."""
        assert make_json_serializable([]) == []
        assert make_json_serializable({}) == {}
        assert make_json_serializable(()) == []

    def test_list_with_nan_values(self):
        """Test list containing NaN values."""
        input_list = [1, 2, float('nan'), 4, np.nan]
        result = make_json_serializable(input_list)
        assert result == [1, 2, None, 4, None]

    def test_dict_with_nan_values(self):
        """Test dictionary containing NaN values."""
        input_dict = {'a': 1, 'b': float('nan'), 'c': np.nan, 'd': pd.NA}
        result = make_json_serializable(input_dict)
        assert result['a'] == 1
        assert result['b'] is None
        assert result['c'] is None
        assert result['d'] is None

    def test_real_world_column_data(self):
        """Test with real-world column data structure."""
        column_data = {
            'column_name': 'Age',
            'sample_values': [25, np.int64(30), float('nan'), 35, pd.NA],
            'timestamp': datetime(2024, 1, 15, 10, 30),
            'metadata': {'type': 'numeric', 'stats': np.array([25, 30, 35])},
        }
        result = make_json_serializable(column_data)

        # Verify it's JSON serializable
        json_str = json.dumps(result)
        assert isinstance(json_str, str)

        # Verify values
        assert result['column_name'] == 'Age'
        assert result['sample_values'] == [25, 30, None, 35, None]
        assert isinstance(result['timestamp'], str)
        assert result['metadata']['stats'] == [25, 30, 35]

    def test_full_json_serialization(self):
        """Test that result can be fully serialized to JSON."""
        complex_data = {
            'dates': [datetime.now(), date.today()],
            'numbers': [np.int32(1), np.float64(2.5)],
            'arrays': np.array([[1, 2], [3, 4]]),
            'mixed': [1, 'text', None, float('nan'), datetime.now()],
        }
        result = make_json_serializable(complex_data)

        # This should not raise an exception
        json_string = json.dumps(result)
        assert isinstance(json_string, str)

        # Verify we can parse it back
        parsed = json.loads(json_string)
        assert isinstance(parsed, dict)
