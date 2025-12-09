import polars as pl
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import json
from pathlib import Path
from logger.etl_logger import ETLLogger

# Logger settings
extract_logger = ETLLogger("extract_weather").get()
extract_logger.info("Starting weather data extraction")

# Settings for file path locations
project_root = Path(__file__).resolve().parents[2]
metadata_folder = project_root / "metadata"
metadata_folder.mkdir(parents=True, exist_ok=True)
metadata_file = metadata_folder / "weather_last_date.json"

if metadata_file.exists():
    with open(metadata_file) as f:
        metadata = json.load(f)
    last_date = datetime.fromisoformat(metadata.get("last_date"))
    extract_logger.info(f"Using last_date from metadata: {last_date.date()}")
else:
    last_date = datetime(2010, 1, 1)
    extract_logger.info(f"No metadata found. Starting from default date: {last_date.date()}")




# Settings for pulling data
chunk_days = 30
max_workers = 4
end_date = datetime(2025, 9, 25)




# Open-Meteo variables of interest
variables = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "precipitation_hours",
    "rain_sum",
    "showers_sum",
    "snowfall_sum",
    "windspeed_10m_max",
    "windgusts_10m_max"
]

# Open-Meteo base URL
base_url = "https://archive-api.open-meteo.com/v1/archive"





# Centroid lat/lon coordinates for each borough
borough_coords = {
    "Manhattan": (40.7831, -73.9712),
    "Brooklyn": (40.6782, -73.9442),
    "Queens": (40.7282, -73.7949),
    "Bronx": (40.8448, -73.8648),
    "Staten Island": (40.5795, -74.1502)
}




# Function to help pull data in chunks based on date ranges
def daterange_chunks(start, end, days_per_chunk = 30):
    current_start = start
    while current_start <= end:
        current_end = min(current_start + timedelta(days = days_per_chunk-1), end)
        yield current_start, current_end
        current_start = current_end + timedelta(days = 1)




# Function to help pull lat/lon borough centroid data
def fetch_weather(borough, lat, lon, start, end):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "daily": ",".join(variables),
        "timezone": "America/New_York"
    }
    try:
        resp = requests.get(base_url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "daily" not in data:
            extract_logger.warning(f"No daily data for {borough} {start.date()} to {end.date()}")
            return None
        df = pl.DataFrame(data["daily"])
        df = df.with_columns([
            pl.lit(borough).alias("borough"),
            pl.lit(lat).alias("latitude"),
            pl.lit(lon).alias("longitude")
        ])
        return df
    except Exception as e:
        extract_logger.error(f"Error fetching {borough} {start.date()} to {end.date()}: {e}")
        return None




# Main function for weather extraction
def extract_weather():
    current_max_date = last_date
    all_chunks = []
    
    for chunk_start, chunk_end in daterange_chunks(last_date + timedelta(days = 1), end_date, chunk_days):
        extract_logger.info(f"Fetching data from {chunk_start.date()} to {chunk_end.date()}")
        dfs = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_weather, b, *borough_coords[b], chunk_start, chunk_end): b
                    for b in borough_coords}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    dfs.append(result)
                    
        if dfs:
            chunk_df = pl.concat(dfs, rechunk=True)
            all_chunks.append(chunk_df)
            
            max_chunk_date = max(chunk_df["time"].to_list())
            if isinstance(max_chunk_date, str):
                max_chunk_date = datetime.fromisoformat(max_chunk_date)
            if max_chunk_date > current_max_date:
                current_max_date = max_chunk_date
                
    if not all_chunks:
        extract_logger.info("No new weather data to extract.")
        return None
    
    combined_df = pl.concat(all_chunks, rechunk=True)
    
    # Updating metadata files
    with open(metadata_file, "w") as f:
        json.dump({"last_date": current_max_date.isoformat()}, f)
    extract_logger.info(f"Weather extraction complete. last_date updated to {current_max_date.date()}")
    
    return combined_df




# Entry point for weather extraction function
if __name__ == "__main__":
    weather_df = extract_weather()