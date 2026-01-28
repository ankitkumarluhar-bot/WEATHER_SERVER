import pandas as pd
from src.data_cleaner import DataCleaner

def test_clean_data():
    data = {
        'City': ['Paris ', 'Nowhere'],
        'Lat': [48.8, None],
        'Lng': [2.3, None],
        'Population': [100, 200]
    }
    df = pd.DataFrame(data)
    cleaned = DataCleaner.clean_data(df)
    
    assert len(cleaned) == 1
    assert cleaned.iloc[0]['city'] == 'paris'