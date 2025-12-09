# Importing libraries needed for extraction
import polars as pl 
import pyarrow as pa 
import pyarrow.parquet as pq 
import urllib.parse
import json
import os
from datetime import datetime 
from concurrent.futures import ThreadPoolExecutor, as_completed
from frictionless import Resource, Schema, Field
from logger.etl_logger import ETLLogger
from pathlib import Path


# Setting logging for extraction
extract_logger = ETLLogger("extract").get()
extract_logger.info("Starting 311 data extraction")

# Paths for project root, metadata, and full data
project_root = Path(__file__).resolve().parents[2]
main_parquet = project_root / "data" / "nyc_311_full_preprocessed.parquet"
metadata_folder = project_root / "metadata"

# Columns for extraction
columns = [
    "unique_key", "created_date", "closed_date", "agency", "agency_name",
    "complaint_type", "descriptor", "location_type", "incident_zip",
    "city", "status", "resolution_action_updated_date", "borough",
    "latitude", "longitude"
]

# columns needed for overwriting
slowly_changing_dimensions = [
    "created_date",
    "closed_date",
    "resolution_action_updated_date"
]

# Frictionless schema for validation
schema = Schema(fields = [
    Field(name = "unique_key", type = "string"),
    Field(name = "created_date", type = "datetime"),
    Field(name = "closed_date", type = "datetime"),
    Field(name = "agency", type = "string"),
    Field(name = "agency_name", type = "string"),
    Field(name = "complaint_type", type = "string"),
    Field(name = "descriptor", type = "string"),
    Field(name = "location_type", type = "string"),
    Field(name = "incident_zip", type = "string"),
    Field(name = "city", type = "string"),
    Field(name = "status", type = "string"),
    Field(name = "resolution_action_updated_date", type ="datetime"),
    Field(name = "borough", type = "string"),
    Field(name = "latitude", type = "number"),
    Field(name = "longitude", type = "number"),
])

# Settings for extraction
base_url = r"https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
chunk_size = 100_000
max_workers = 4



# Frictionless schema for validation upon extraction
schema = Schema(fields = [
    Field(name = "unique_key", type = "string"),
    Field(name = "created_date", type = "datetime"),
    Field(name = "closed_date", type = "datetime"),
    Field(name = "agency", type = "string"),
    Field(name = "agency_name", type = "string"),
    Field(name = "complaint_type", type = "string"),
    Field(name = "descriptor", type = "string"),
    Field(name = "location_type", type = "string"),
    Field(name = "incident_zip", type = "string"),
    Field(name = "city", type = "string"),
    Field(name = "status", type = "string"),
    Field(name = "resolution_action_updated_date", type = "datetime"),
    Field(name = "borough", type = "string"),
    Field(name = "latitude", type = "number"),
    Field(name = "longitude", type = "number"),
])


# Function for downloading by chunks from Socrata API via URL (Faster i/o)
def download_chunk(offset, latest_date):
    soql = f"""
        SELECT {', '.join(columns)}
        WHERE created_date > '{latest_date}'
        LIMIT {chunk_size} OFFSET {offset}
    """
    encoded_query = urllib.parse.quote(soql, safe='')
    url = f"{base_url}?$query={encoded_query}"

    try:
        df_chunk = pl.read_csv(url, columns=columns, dtypes={"incident_zip": pl.Utf8})
        if df_chunk.height == 0:
            return None
        return df_chunk
    except Exception as e:
        extract_logger.error(f"Error at offset {offset}: {e}")
        return None



def extract_311():
    # Determining latest date in metadata extraction files
    metadata_folder.mkdir(parents = True, exist_ok = True)
    metadata_file = metadata_folder / "last_date.json"

    if metadata_file.exists():
        with open(metadata_file) as f:
            report_json = json.load(f)
            latest_date = report_json.get("last_date")
            extract_logger.info(f"Using latest_date from metadata: {latest_date}")
    else:
        latest_date = "2025-09-25T01:44:42"  # default start date
        extract_logger.info(f"No metadata found, using default start date: {latest_date}")
    
    offset = 0
    all_chunks = []
    finished = False
    
    while not finished:
        offsets = [offset + i * chunk_size for i in range(max_workers)]
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(download_chunk, o, latest_date): o for o in offsets}
            for future in as_completed(futures):
                df_chunk = future.result()
                if df_chunk is not None and df_chunk.height > 0:
                    results.append(df_chunk)

        if not results:  # Stopping if all chunks are empty
            finished = True
        else:
            all_chunks.extend(results)
            offset += max_workers * chunk_size

    if not all_chunks:
        extract_logger.info("No new 311 data to extract.")
        return None
    
    # Concatenating data
    new_data = pl.concat(all_chunks, rechunk=True)
    
    # Schema Validation
    try:
        resource = Resource(data = new_data.to_dicts(), schema = schema)
        validation_report = resource.validate()
        if validation_report.valid:
            extract_logger.info("Extracted data matches schema.")
        else:
            extract_logger.warning("Schema validation failed. Check extracted data.")
    except Exception as e:
        extract_logger.error(f"Schema validation failed: {e}")

    # Merging with main dataset with incremental updatation
    if main_parquet.exists():
        df_main = pl.read_parquet(main_parquet)
        
        # Joining on SCD columns
        df_update = new_data.select(["unique_key"] + slowly_changing_dimensions)
        df_main = df_main.join(df_update, on="unique_key", how="left")
        
        # Overwriting SCD columns if new value exists
        for col in slowly_changing_dimensions:
            right_col = f"{col}_right"
            if right_col in df_main.columns:
                df_main = df_main.with_columns(
                    pl.when(pl.col(right_col).is_not_null())
                    .then(pl.col(right_col))
                    .otherwise(pl.col(col))
                    .alias(col)
                ).drop(right_col)
                
        # Adding new rows that do not exist
        new_rows = new_data.join(df_main, on="unique_key", how="anti")
        if new_rows.height > 0:
            df_main = pl.concat([df_main, new_rows], rechunk=True)
            
        combined = df_main
    else:
        combined = new_data
        
    # Updating metadata files
    last_date = new_data.select(pl.col("created_date").max()).item()
    with open(metadata_file, "w") as f:
        json.dump({"last_date": last_date}, f)
        
    # Saving updated dataset
    combined.write_parquet(main_parquet)
    extract_logger.info(f"Extraction completed. Total records: {combined.height}")
    
    return combined


# Entry point for the extraction function
if __name__ == "__main__":
    extract_311()