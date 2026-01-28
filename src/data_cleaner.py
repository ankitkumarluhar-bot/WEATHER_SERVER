import pandas as pd
import numpy as np
from src.logger import get_logger

logger = get_logger(__name__)

class DataCleaner:
    @staticmethod
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"🧹 Starting Data Cleaning on {len(df)} rows...")
        
        # 1. Normalization of coloumns::
        # converting into a Lowercase, strip whitespace & replace spaces with underscores......
        df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
        
        # 2. Handle Numeric Data Types ::
        # 'errors=coerce' turns bad data into a Nan.....
        cols_to_numeric = ['population', 'lat', 'lng']
        for col in cols_to_numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 3. Handle Null Values::
        # drop the rows where lat & lng as a null...
        initial_count = len(df)
        df = df.dropna(subset=['lat', 'lng'])
        
        # Filling missing Country/City with -->"unknown" .....
        string_cols = ['city', 'country', 'iso2', 'iso3', 'admin_name']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna('unknown')

        # 4. Outlier Removal::
        # Lat must be lie btw [-90,90] & Lng must be lie btw [-180,180]......
        df = df[
            (df['lat'].between(-90, 90)) & 
            (df['lng'].between(-180, 180))
        ]

        # 5. String Cleanup::
        # make stirng Lowercase and strip whitespace......
        if 'city' in df.columns:
            df['city'] = df['city'].astype(str).str.lower().str.strip()
        if 'country' in df.columns:
            df['country'] = df['country'].astype(str).str.lower().str.strip()

        # 6. Handle Duplicates::
        df = df.drop_duplicates()
        
        if 'population' in df.columns:
            df = df.sort_values('population', ascending=False)
            #if duplicate present keep higher value item in dataset....
            df = df.drop_duplicates(subset=['city', 'country'], keep='first')

        dropped_count = initial_count - len(df)
        logger.info(f"✅ Cleaning Complete. Dropped {dropped_count} invalid/duplicate rows. Final count: {len(df)}")
            
        return df