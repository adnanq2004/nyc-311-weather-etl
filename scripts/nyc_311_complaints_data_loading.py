import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Settings for API requests
base_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
chunk_size = 100_000
output_parquet = "nyc_311_full.parquet"
max_workers = 4                      # Using 4 threads (multi-threading) to load data faster
columns = [
    "unique_key", "created_date", "closed_date", "agency", "agency_name",
    "complaint_type", "descriptor", "location_type", "incident_zip",
    "city", "status", "resolution_action_updated_date", "borough",
    "latitude", "longitude"
]

# Function to load data by chunk with polars and write into parquet for memory efficiency
def download_chunk(offset):
    # Using Socrata API SQL
    soql = (
        f"SELECT {', '.join(columns)} "
        f"LIMIT {chunk_size} OFFSET {offset}"
    )
    encoded_query = urllib.parse.quote(soql, safe='')
    url = f"{base_url}?$query={encoded_query}"
    
    try:
        # Forcing zip to be a string for zip-codes with dashes
        df_chunk = pl.read_csv(url, columns=columns, dtypes={"incident_zip": pl.Utf8})
        if df_chunk.height == 0:
            return None
        table = df_chunk.to_arrow() # Converting polars df to arrow
        return (offset, table)
    except Exception as e:
        print(f"Error at offset {offset}: {e}")
        return None

# Download loop and offset counter
offset = 0
first_chunk = True
writer = None

while True:
    # Preparing offsets for parallel threads
    offsets = [offset + i*chunk_size for i in range(max_workers)]
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_chunk, o): o for o in offsets}
        
        all_done = False
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                all_done = True
                continue
            off, table = result
            if first_chunk:
                # Writing arrow to parquet
                writer = pq.ParquetWriter(output_parquet, table.schema, compression="snappy")
                first_chunk = False
            writer.write_table(table)
            print(f"Downloaded {off + table.num_rows} rows so far")
    
    if all_done:
        break
    offset += max_workers * chunk_size

# Closing file writer
if writer:
    writer.close()

print("All data downloaded and saved to Parquet.")
