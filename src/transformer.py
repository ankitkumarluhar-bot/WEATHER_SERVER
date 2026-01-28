import pandas as pd
from src.api_client import WeatherApiClient
from src.logger import get_logger

logger = get_logger(__name__)

class DataTransformer:
    def __init__(self):
        self.api_client = WeatherApiClient()

    def fetch_and_merge_weather(self, df: pd.DataFrame, limit=5) -> pd.DataFrame:
        target_cities = df.head(limit).copy()
        weather_data = []

        logger.info(f"📡 Fetching weather data for top {limit} cities...")

        for index, row in target_cities.iterrows():
            weather = self.api_client.get_current_weather(row['lat'], row['lng'])
            if weather:
                weather_data.append(weather)
            else:
                weather_data.append({"temperature": None, "humidity": None, "condition": None})

        weather_df = pd.DataFrame(weather_data, index=target_cities.index)
        return pd.concat([target_cities, weather_df], axis=1)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("🔄 Transforming data...")
        
        # categorizing a temperature into cold,moderate & hot::
        def categorize_temp(temp):
            if pd.isna(temp): return "Unknown"
            if temp < 10: return "Cold"
            if 10 <= temp <= 25: return "Moderate"
            return "Hot"

        df['temp_category'] = df['temperature'].apply(categorize_temp)

        # categorizing a population into large,very large & megacity::
        def categorize_pop(pop):
            if pop > 15000000: return "Megacity"
            if pop > 5000000: return "Very Large"
            return "Large"

        df['pop_bucket'] = df['population'].apply(categorize_pop)
        
        return df