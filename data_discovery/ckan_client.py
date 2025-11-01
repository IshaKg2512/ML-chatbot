import os
from typing import Dict, List

import requests
from typing import Optional


class DataGovInClient:
    """Minimal client placeholder for data.gov.in CKAN API.

    This stub exists to unblock the UI. It exposes a small surface area
    sufficient for `DatasetCatalog.discover_datasets` to work in demo mode.
    """

    def __init__(self, base_url: str | None = None, api_key: str | None = None):
        self.base_url = base_url or "https://data.gov.in/api/1"
        # Get API key from parameter, env var, or config
        if api_key:
            self.api_key = api_key
        else:
            from config import get_config
            cfg = get_config()
            self.api_key = cfg.data_gov_in_api_key or os.getenv("DATA_GOV_IN_API_KEY")

    def ping(self) -> bool:
        """Lightweight health check; always returns True in stub mode."""
        return True

    def list_sample_resources(self) -> Dict[str, Dict[str, List[Dict]]]:
        """Return a tiny curated catalog for demo purposes.

        Structure matches what `DatasetCatalog` expects:
        {
            category: {
                subcategory: {
                    'resource_ids': [{'id': '...', 'name': '...'}, ...]
                }
            }
        }
        """
        return {
            "agriculture": {
                "crop_production": {
                    "resource_ids": [
                        {"id": "sample_agri_1", "name": "Crop Production (sample)"},
                    ]
                }
            },
            "climate": {"rainfall": {"resource_ids": []}},
        }

    def list_curated_real_resources(self) -> Dict[str, Dict[str, List[Dict]]]:
        """Return a curated set of real data.gov.in resource IDs.

        Note: Accessing these via API requires DATA_GOV_IN_API_KEY.
        """
        return {
            "agriculture": {
                "crop_production": {
                    "resource_ids": [
                        {
                            "id": "9ef84268-d588-465a-a308-a864a43d0070",
                            "name": "Crop Production in India (All-India)",
                        }
                    ]
                }
            },
            "climate": {"rainfall": {"resource_ids": []}},
        }

    # -------- Auto-discovery (best-effort) --------
    def _ckan_action(self, action: str, params: Dict) -> Optional[Dict]:
        """Call CKAN action API, return JSON or None on failure."""
        try:
            # CKAN action endpoint
            url = f"https://data.gov.in/api/1/action/{action}"
            headers = {}
            if self.api_key:
                headers["api-key"] = self.api_key
            resp = requests.get(url, params=params, headers=headers, timeout=60)
            if resp.ok:
                return resp.json()
            return None
        except Exception:
            return None

    def discover_rainfall_resource_id(self) -> Optional[str]:
        """Try to find an IMD rainfall dataset resource ID automatically."""
        # Search for packages mentioning rainfall from IMD
        data = self._ckan_action(
            "package_search",
            {
                "q": "rainfall",
                "fq": "organization:india-meteorological-department",
                "rows": 20,
            },
        )
        if not data or not data.get("success"):
            return None

        for pkg in data.get("result", {}).get("results", []):
            for res in pkg.get("resources", []):
                fmt = (res.get("format") or "").lower()
                if fmt in {"json", "csv"}:
                    rid = res.get("id") or res.get("resource_id")
                    if rid:
                        return rid
        return None

    def discover_crop_production_resource_id(self) -> Optional[str]:
        """Try to find MoAFW crop production dataset resource ID automatically."""
        data = self._ckan_action(
            "package_search",
            {
                "q": "crop production",
                "fq": "organization:department-of-agriculture-and-farmers-welfare",
                "rows": 20,
            },
        )
        if not data or not data.get("success"):
            return None
        for pkg in data.get("result", {}).get("results", []):
            for res in pkg.get("resources", []):
                fmt = (res.get("format") or "").lower()
                if fmt in {"json", "csv"}:
                    rid = res.get("id") or res.get("resource_id")
                    if rid:
                        return rid
        return None

    def discover_district_crop_production_resource_id(self) -> Optional[str]:
        """Try to find district-wise crop production dataset resource ID automatically."""
        # Search for district crop production datasets
        search_terms = [
            ("district crop production", "organization:department-of-agriculture-and-farmers-welfare"),
            ("district wise crop", "organization:department-of-agriculture-and-farmers-welfare"),
            ("season wise crop production", "organization:department-of-agriculture-and-farmers-welfare"),
            ("district crop production", None),  # Broader search without org filter
        ]
        
        for query, org_filter in search_terms:
            params = {"q": query, "rows": 30}
            if org_filter:
                params["fq"] = org_filter
            
            data = self._ckan_action("package_search", params)
            if not data or not data.get("success"):
                continue
            
            # Look through packages and resources
            for pkg in data.get("result", {}).get("results", []):
                pkg_title = (pkg.get("title") or "").lower()
                pkg_name = (pkg.get("name") or "").lower()
                # Prioritize packages with "district" and "crop" in the title
                if "district" in pkg_title or "district" in pkg_name:
                    for res in pkg.get("resources", []):
                        res_name = (res.get("name") or "").lower()
                        fmt = (res.get("format") or "").lower()
                        if fmt in {"json", "csv"}:
                            rid = res.get("id") or res.get("resource_id")
                            if rid:
                                # Check if it's not the wrong dataset type
                                if "pollutant" not in pkg_title and "air" not in pkg_title:
                                    return rid
            
            # If no district-specific found, try any crop production resource
            for pkg in data.get("result", {}).get("results", []):
                for res in pkg.get("resources", []):
                    fmt = (res.get("format") or "").lower()
                    if fmt in {"json", "csv"}:
                        rid = res.get("id") or res.get("resource_id")
                        if rid:
                            return rid
        
        return None


