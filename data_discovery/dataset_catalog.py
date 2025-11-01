from typing import Dict
from config import get_config
from .ckan_client import DataGovInClient


class DatasetCatalog:
    """Holds discovered datasets grouped by category and subcategory."""

    def __init__(self):
        self.datasets: Dict = {}

    def discover_datasets(self, client) -> None:
        """Populate `self.datasets` using the provided client.

        In demo mode, we rely on `DataGovInClient.list_sample_resources()`.
        """
        cfg = get_config()
        auto_client = DataGovInClient()
        # Auto-discovery first
        rainfall_id = auto_client.discover_rainfall_resource_id()
        crop_id = auto_client.discover_crop_production_resource_id()
        district_crop_id = auto_client.discover_district_crop_production_resource_id()

        # Fallbacks from config if discovery not found
        rainfall_id = rainfall_id or cfg.rainfall_resource_id
        crop_id = crop_id or cfg.crop_production_resource_id
        district_crop_id = district_crop_id or cfg.district_crop_production_resource_id

        self.datasets = {
            "agriculture": {
                "crop_production": {
                    "resource_ids": (
                        [{"id": crop_id, "name": "Crop Production"}] if crop_id else []
                    )
                },
                "district_crop_production": {
                    "resource_ids": (
                        [{"id": district_crop_id, "name": "District Crop Production"}] if district_crop_id else []
                    )
                }
            },
            "climate": {
                "rainfall": {
                    "resource_ids": (
                        [{"id": rainfall_id, "name": "Rainfall"}] if rainfall_id else []
                    )
                }
            },
        }


