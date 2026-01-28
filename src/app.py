from fastapi import FastAPI, HTTPException, Query,status
from fastapi.responses import JSONResponse  
import pandas as pd
import os
from typing import Optional

from src.data_loader import DataLoader
from src.data_cleaner import DataCleaner
from src.transformer import DataTransformer
from src.config import Config
from src.logger import get_logger

logger = get_logger("API_MAIN")
#initializing a fastapi::
app = FastAPI(title="City Weather Analytics", version="1.0.0")

city_data = pd.DataFrame()

def run_pipeline():
    logger.info("🚀 Starting ETL Pipeline...")
    
    #raw csv file is loading::
    loader = DataLoader(Config.RAW_DATA_PATH)
    df = loader.load_data()
    
    #raw csv file is cleaning::
    df_clean = DataCleaner.clean_data(df)
    
    #calling transfromer to perform transformation operation::
    transformer = DataTransformer()

    #merging a json data come from the openweather api & the cleaned raw dataset::
    df_merged = transformer.fetch_and_merge_weather(df_clean, limit=10)
    
    #adding a temperature & population coloumn,based on the condition::
    df_final = transformer.transform(df_merged)
    
    #making a merged_data.csv file--->>the final dataset:::
    os.makedirs(os.path.dirname(Config.PROCESSED_DATA_PATH), exist_ok=True)
    df_final.to_csv(Config.PROCESSED_DATA_PATH, index=False)
    logger.info(f"✅ Data saved to {Config.PROCESSED_DATA_PATH}")
    return df_final


#Routes::
@app.on_event("startup")
def startup_event():
    global city_data
 
    if not os.path.exists(Config.PROCESSED_DATA_PATH):
        logger.warning("⚠️ Processed data missing. Running pipeline...")
        city_data = run_pipeline()
    else:
        logger.info("📂 Loading existing processed data...")
        city_data = pd.read_csv(Config.PROCESSED_DATA_PATH)
    
    if not city_data.empty:
        city_data = city_data.where(pd.notnull(city_data), None)

@app.get("/", tags=["General"], status_code=status.HTTP_200_OK)
def root():
    return {
        "message": "Welcome to the Weather Analytics.....",
        "docs_url": "/docs",
        "health_check": "/health"
    }

@app.get("/health", tags=["General"], status_code=status.HTTP_200_OK)
def health_check():
    if city_data.empty:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "details": "Data not loaded"}
        )
    return {"status": "healthy", "rows_loaded": len(city_data)}

@app.get("/cities", tags=["Cities"], status_code=status.HTTP_200_OK)
def get_cities(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(5, ge=1, le=100, description="Items per page"),
    temp_category: Optional[str] = Query(None, description="Filter: Cold, Moderate, Hot"),
    min_population: Optional[int] = Query(None, description="Filter: Minimum population")
):
    
    if city_data.empty:
         raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            detail="Data is not available. Check logs."
        )

    result = city_data.copy()

    #Filter by Temperature Category::
    if temp_category:
        result = result[result['temp_category'].str.lower() == temp_category.lower()]

    #Filter by Population::
    if min_population:
        result = result[result['population'] >= min_population]

    # Pagination Logic::
    total_records = len(result)
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    
    paginated_data = result.iloc[start_index:end_index].to_dict(orient='records')

    return {
        "metadata": {
            "page": page,
            "page_size": page_size,
            "total_records": total_records,
            "total_pages": (total_records + page_size - 1) // page_size
        },
        "data": paginated_data
    }

@app.get("/cities/{city_name}", tags=["Cities"], status_code=status.HTTP_200_OK)
def get_city_detail(city_name: str):
    if city_data.empty:
         raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data unavailable")

    result = city_data[city_data['city'] == city_name.lower().strip()]
    
    if result.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"City '{city_name}' not found"
        )
        
    return result.to_dict(orient='records')[0]
