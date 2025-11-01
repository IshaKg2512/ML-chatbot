from typing import Any, Dict, Optional

import pandas as pd
import requests
from config import get_config


class DataFetcher:
    """Fetch datasets by resource id from data.gov.in Datastore API or CKAN datastore_search."""

    def fetch_dataset(self, resource_id: str, use_ckan: bool = False) -> pd.DataFrame:
        """Fetch dataset using either data.gov.in API or CKAN datastore_search.
        
        Args:
            resource_id: The resource ID to fetch
            use_ckan: If True, use CKAN datastore_search endpoint; otherwise use data.gov.in API
        """
        if resource_id.startswith("sample_"):
            return pd.DataFrame()

        api_key = get_config().data_gov_in_api_key
        if not api_key or api_key == "YOUR_KEY_HERE":
            raise RuntimeError(
                "DATA_GOV_IN_API_KEY not set or is placeholder. Create .env with DATA_GOV_IN_API_KEY=<your_key>."
            )

        if use_ckan:
            # Use CKAN datastore_search endpoint
            return self._fetch_from_ckan_datastore(resource_id, api_key)
        else:
            # Use data.gov.in API
            url = f"https://api.data.gov.in/resource/{resource_id}"
            params: Dict[str, Any] = {
                "api-key": api_key,
                "format": "json",
                "limit": 5000,
            }
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            records = data.get("records", [])
            return pd.DataFrame.from_records(records)
    
    def _fetch_from_ckan_datastore(self, resource_id: str, api_key: str) -> pd.DataFrame:
        """Fetch dataset from CKAN datastore_search endpoint."""
        url = "https://ckandev.indiadataportal.com/api/1/action/datastore_search"
        params: Dict[str, Any] = {
            "resource_id": resource_id,
            "limit": 5000,
        }
        headers = {}
        if api_key:
            headers["Authorization"] = api_key
            # Some CKAN APIs use X-API-Key header
            headers["X-API-Key"] = api_key
        
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            
            # CKAN datastore_search returns data in different format
            if data.get("success"):
                records = data.get("result", {}).get("records", [])
                if records:
                    return pd.DataFrame.from_records(records)
            
            # Try alternative response format
            if "records" in data:
                return pd.DataFrame.from_records(data["records"])
            
            return pd.DataFrame()
        except Exception as e:
            # Fallback to data.gov.in API if CKAN fails
            url = f"https://api.data.gov.in/resource/{resource_id}"
            params = {
                "api-key": api_key,
                "format": "json",
                "limit": 5000,
            }
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            records = data.get("records", [])
            return pd.DataFrame.from_records(records)


