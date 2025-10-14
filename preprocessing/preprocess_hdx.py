import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def write_temp_csv(download_url, temp_path='temp.csv'):
    """
    Write a temp CSV file from a download URL.
    """
    import requests
    response = requests.get(download_url)
    response.raise_for_status()
    with open(temp_path, 'wb') as f:
        f.write(response.content)
    return temp_path

def process_csv(temp_path='temp.csv'):
    """
    Process a CSV file.
    """
    logger.info('Processing csv file: %s', temp_path)
    assert os.path.exists(temp_path), 'File not found: %s' % temp_path
    assert temp_path.endswith('.csv'), 'File is not a CSV: %s' % temp_path

    df = pd.read_csv(temp_path)

    if len(df) == 0:
        raise ValueError(f'File is empty: {temp_path}')

    if len(df.columns) == 0:
        raise ValueError(f'File has no columns: {temp_path}')

    if len(df.columns) > 20:
        logger.warning('File has too many columns: %s, truncating to 20 columns', temp_path)
        df = df.head(20)

    logger.info('Processed file: %s', temp_path)
    return df
