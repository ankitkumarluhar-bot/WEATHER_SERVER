import requests
from src.config import Config
from src.logger import get_logger

logger = get_logger(__name__)

class WeatherApiClient:

    #constructor::
    def __init__(self):
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        self.api_key = Config.API_KEY

    def get_current_weather(self, lat, lon):
        if not self.api_key:
            logger.error("Cannot fetch weather: API Key is missing....")
            return None
        
        #declaring a dict. :: 
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": "metric" 
        }

        #error handling using try & except blocks::
        try:
            response = requests.get(self.base_url, params=params, timeout=5)
            response.raise_for_status()
            print(response.raise_for_status())
            data = response.json()
            print(data)
            
            return {
                "temperature": data.get("main", {}).get("temp"),
                "humidity": data.get("main", {}).get("humidity"),
                "condition": data.get("weather", [{}])[0].get("main"),
                "city": data.get("name")
            }
        except Exception as e:
            logger.error(f"Failed to fetch weather for {lat}, {lon}: {e}")
            return None
        
