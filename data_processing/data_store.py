# data_processing/data_store.py

from typing import Dict, Optional

import pandas as pd

class DataStore:
    """In-memory store for processed datasets"""
    
    def __init__(self):
        self.agriculture_data = {}
        self.climate_data = {}
        self.metadata = {}
        
    def add_dataset(self, category: str, name: str, df: pd.DataFrame, metadata: Dict):
        """Add cleaned dataset to store"""
        if category == 'agriculture':
            self.agriculture_data[name] = df
        elif category == 'climate':
            self.climate_data[name] = df
        
        self.metadata[name] = metadata
    
    def get_dataset(self, name: str) -> Optional[pd.DataFrame]:
        """Retrieve dataset by name"""
        if name in self.agriculture_data:
            return self.agriculture_data[name]
        if name in self.climate_data:
            return self.climate_data[name]
        return None
    
    def list_datasets(self) -> Dict:
        """List all available datasets"""
        return {
            'agriculture': list(self.agriculture_data.keys()),
            'climate': list(self.climate_data.keys())
        }