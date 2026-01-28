import os
from dotenv import load_dotenv

# Load .env file::
load_dotenv()

class Config:
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
    PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH")
    
    if not API_KEY:
        print("⚠️ WARNING:WEATHER_API_KEY is missing......")

