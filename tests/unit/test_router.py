"""Unit tests for the FastAPI router endpoints."""

from pathlib import Path
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from app.main_fastapi import app

client = TestClient(app)


@patch('app.router.REPORTS_DIR')
@patch('app.router.load_sdd')
def test_get_model_results_error_fallback(mock_load_sdd, mock_reports_dir):
    """Test get_model_results endpoint returns safe defaults when report parsing fails."""
    # Setup mock file structure
    mock_model_dir = MagicMock(spec=Path)
    mock_reports_dir.__truediv__.return_value = mock_model_dir
    mock_model_dir.exists.return_value = True

    mock_result_file = MagicMock(spec=Path)
    mock_result_file.stem = 'failed_dataset'
    mock_result_file.glob.return_value = [mock_result_file]
    mock_model_dir.glob.return_value = [mock_result_file]

    # Mock file stat for modification time
    mock_stat = MagicMock()
    mock_stat.st_mtime = 1715699674.0
    mock_result_file.stat.return_value = mock_stat

    # Make load_sdd raise an error
    mock_load_sdd.side_effect = Exception('Malformed JSON')

    response = client.get('/api/results/gpt-4.1-nano')

    assert response.status_code == 200
    data = response.json()
    assert 'results' in data
    assert len(data['results']) == 1

    result = data['results'][0]
    assert result['model'] == 'gpt-4.1-nano'
    assert result['dataset'] == 'failed_dataset'
    assert result['status'] == 'error'
    assert result['error'] == 'Malformed JSON'
    assert 'processed_at' in result
    assert result['sensitive'] == 'error'
    assert result['sheet_count'] == 0
    assert result['total_rows'] == 0
