"""
JSON serialization utilities.

This module provides utilities for converting Python objects to JSON-serializable formats.
"""

from datetime import datetime, date
from typing import Any
import numpy as np
import pandas as pd


def make_json_serializable(obj: Any) -> Any:
    """
    Convert objects to JSON-serializable formats.
    
    This function recursively processes objects and converts non-JSON-serializable
    types to their string representations:
    - datetime/date objects -> ISO format strings
    - numpy types -> Python native types
    - pandas Timestamp -> ISO format strings
    - NaN/None -> None
    - Lists and dicts are processed recursively
    
    Args:
        obj: Object to convert
        
    Returns:
        JSON-serializable version of the object
    """
    # Handle None and basic types
    if obj is None:
        return None
    
    # Handle datetime objects
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    
    # Handle pandas Timestamp
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    
    # Handle numpy types
    if isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    
    # Handle NaN
    if isinstance(obj, float) and (np.isnan(obj) or pd.isna(obj)):
        return None
    
    # Handle lists recursively
    if isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    
    # Handle tuples (convert to list)
    if isinstance(obj, tuple):
        return [make_json_serializable(item) for item in obj]
    
    # Handle dicts recursively
    if isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}
    
    # Return as-is for basic JSON-serializable types (str, int, float, bool)
    return obj
