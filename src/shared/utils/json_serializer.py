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
    # Handle None
    if obj is None:
        return None

    # Handle collections first to avoid ambiguity with pd.isna on arrays
    if isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    
    if isinstance(obj, tuple):
        return [make_json_serializable(item) for item in obj]
        
    if isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}

    if isinstance(obj, np.ndarray):
        return obj.tolist()

    # Handle datetime objects
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    

    
    # Handle numpy generic types
    if isinstance(obj, np.generic):
        return make_json_serializable(obj.item())

    # Handle scalar NaN/NaT
    try:
        if pd.isna(obj):
            return None
    except (ValueError, TypeError):
        # Fallback for types where pd.isna might fail or return array
        pass
    
    return obj
