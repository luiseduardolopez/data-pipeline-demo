"""
API Utilities for data extraction from REST APIs
"""

import requests
import pandas as pd
from typing import Dict, Any, Optional, List
import logging
from src.config.settings import get_settings

logger = logging.getLogger(__name__)

def test_api_connection(url: str, headers: Optional[Dict[str, str]] = None) -> bool:
    """
    Test connection to an API endpoint
    
    Args:
        url: API endpoint URL
        headers: Optional headers for authentication
        
    Returns:
        bool: True if connection successful
    """
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"API connection successful to {url}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"API connection failed: {e}")
        return False

def fetch_data_from_api(
    url: str, 
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Fetch data from API endpoint
    
    Args:
        url: API endpoint URL
        headers: Optional headers for authentication
        params: Optional query parameters
        
    Returns:
        Dict: JSON response from API
    """
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from API: {e}")
        raise

def api_data_to_dataframe(data: List[Dict[str, Any]], normalize: bool = True) -> pd.DataFrame:
    """
    Convert API response data to pandas DataFrame
    
    Args:
        data: List of dictionaries from API response
        normalize: Whether to normalize nested JSON
        
    Returns:
        pd.DataFrame: Converted data
    """
    try:
        if normalize and isinstance(data, list) and len(data) > 0:
            # Handle nested JSON structures
            df = pd.json_normalize(data)
        else:
            df = pd.DataFrame(data)
        
        logger.info(f"Converted {len(df)} records to DataFrame")
        return df
    except Exception as e:
        logger.error(f"Failed to convert API data to DataFrame: {e}")
        raise

class APIClient:
    """
    Generic API client for data extraction
    """
    
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None):
        self.base_url = base_url.rstrip('/')
        self.headers = headers or {}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make GET request to API endpoint"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def extract_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Extract data from API and convert to DataFrame"""
        data = self.get(endpoint, params)
        
        # Handle different response formats
        if isinstance(data, dict):
            # Look for common data keys
            for key in ['data', 'results', 'items', 'records']:
                if key in data:
                    data = data[key]
                    break
        
        if not isinstance(data, list):
            data = [data]
        
        return api_data_to_dataframe(data)
