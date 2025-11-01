import pandas as pd


class DataCleaner:
    """Minimal cleaner that returns the DataFrame unchanged."""

    def clean_dataset(self, df: pd.DataFrame, category: str) -> pd.DataFrame:
        return df


