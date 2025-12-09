import polars as pl
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd


# Settings for bulk extraction
output_parquet = "nyc_open_meteo_full_weather.parquet"
max_workers = 1                     # Lowering max_workers due to API requests throttling :(
chunk_days = 30                    # Loading a month per chunk
start_date = datetime(2010, 1, 1)
end_date = datetime(2025, 9, 25)

# Setting variables to pull from open-meteo
variables = ["temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "precipitation_hours",
            "rain_sum",
            "showers_sum",
            "snowfall_sum",
            "windspeed_10m_max",
            "windgusts_10m_max" 
            ]


# Base open-meteo URL
base_url = "https://archive-api.open-meteo.com/v1/archive"


# Centroid borough coordinates for pulling weather data
borough_coords = {
    "Manhattan": (40.7831, -73.9712),
    "Brooklyn": (40.6782, -73.9442),
    "Queens": (40.7282, -73.7949),
    "Bronx": (40.8448, -73.8648),
    "Staten Island": (40.5795, -74.1502)
}


# Functions for data extraction
def daterange_chunks(start, end, days_per_chunk=30):
    current_start = start
    while current_start <= end:
        current_end = min(current_start + timedelta(days = days_per_chunk-1), end)
        yield current_start, current_end
        current_start = current_end + timedelta(days = 1)

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
        resp = requests.get(base_url, params = params, timeout = 60)
        resp.raise_for_status()
        data = resp.json()
        if "daily" not in data:
            return None
        df = pl.DataFrame(data["daily"])
        df = df.with_columns([
            pl.lit(borough).alias("borough"),
            pl.lit(lat).alias("latitude"),
            pl.lit(lon).alias("longitude")
        ])
        return df
    except Exception as e:
        print(f"Error fetching {borough} {start}-{end}: {e}")
        return None


# Loop to pull historic data
first_chunk = True
writer = None

for chunk_start, chunk_end in daterange_chunks(start_date, end_date, chunk_days):
    print(f"Fetching data from {chunk_start.date()} to {chunk_end.date()}...")
    dfs = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_weather, b, *borough_coords[b], chunk_start, chunk_end): b 
                for b in borough_coords}

        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                dfs.append(result)

    if dfs:
        chunk_df = pl.concat(dfs)
        table = chunk_df.to_arrow()
        if first_chunk:
            writer = pq.ParquetWriter(output_parquet, table.schema, compression = "snappy")
            first_chunk = False
        writer.write_table(table)
        print(f"Saved {chunk_start.date()} to {chunk_end.date()}.")

if writer:
    writer.close()

print("All data downloaded")