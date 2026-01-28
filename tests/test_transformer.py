import pandas as pd
import pytest
from src.transformer import DataTransformer

def test_transformation_categories():
    transformer = DataTransformer()
    
    # 1. Setup Dummy Data
    data = {
        'city': ['City A', 'City B', 'City C'],
        'temperature': [5, 20, 30],           
        'population': [16000000, 6000000, 1000000] 
    }
    df = pd.DataFrame(data)
    
    # 2. Run Transformation
    result_df = transformer.transform(df)
    
    # 3. Assertions
    
    # Check Temperature Categories
    assert result_df.iloc[0]['temp_category'] == 'Cold'
    assert result_df.iloc[1]['temp_category'] == 'Moderate'
    assert result_df.iloc[2]['temp_category'] == 'Hot'
    
    # Check Population Buckets
    assert result_df.iloc[0]['pop_bucket'] == 'Megacity'
    assert result_df.iloc[1]['pop_bucket'] == 'Very Large'
    assert result_df.iloc[2]['pop_bucket'] == 'Large'

def test_transform_handles_null_values():
    """
    Test how the transformer handles missing temperature data.
    """
    transformer = DataTransformer()
    
    data = {
        'city': ['City Null'],
        'temperature': [None],
        'population': [500]
    }
    df = pd.DataFrame(data)
    
    result_df = transformer.transform(df)
    
    
    assert result_df.iloc[0]['temp_category'] == 'Unknown'