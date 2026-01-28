import pandas as pd
import os
from src.logger import get_logger

logger = get_logger(__name__)

class DataLoader:
    def __init__(self, filepath):
        self.filepath = filepath

    def load_data(self):
        if not os.path.exists(self.filepath):
            logger.error(f"File not found: {self.filepath}")
            raise FileNotFoundError(f"File not found: {self.filepath}")

        logger.info(f"Loading data from {self.filepath}")
        df = pd.read_csv(self.filepath)
        
        
        df['population'] = pd.to_numeric(df['population'], errors='coerce')

        # Filtering: population > 1 Million:: 
        df_filtered = df[df['population'] > 1000000].copy()
        
        return df_filtered