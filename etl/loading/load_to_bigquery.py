from google.cloud import bigquery
from google.oauth2 import service_account
import polars as pl
from pathlib import Path
from logger.etl_logger import ETLLogger

# Initializing logger
load_logger = ETLLogger("load").get()
load_logger.info("Starting data loading")

# BigQuery client setup
script_dir = Path(__file__).parent
credentials_path = script_dir.parent / "credentials" / "bigquery_nyc_weather_etl_credentials.json"
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(project="nyc-311-weather-etl", credentials=credentials)
dataset_id = "nyc_311_weather"

def load_to_bigquery(df_dict, chunk_size = 10_000):
    for table_name, df in df_dict.items():
        if df is None or df.height == 0:
            load_logger.info(f"No data for table {table_name}, skipping...")
            continue
        load_logger.info(f"Starting load for table {table_name} ({df.height} rows)")
        table_ref = f"{client.project}.{dataset_id}.{table_name}"
        # Check if table exists
        try:
            table = client.get_table(table_ref)
            table_exists = True
        except Exception:
            table_exists = False
            load_logger.info(f"Table {table_name} does not exist. Will create new table automatically.")
        # Convert Polars to pandas for BigQuery
        df_pd = df.to_pandas()
        
        if table_name.startswith("dim_") and table_exists:
            # For dimension tables, remove duplicates based on primary key
            pk_field = table.schema[0].name
            # Load existing primary keys
            existing_df = client.list_rows(table).to_dataframe()
            df_pd = df_pd.merge(existing_df[[pk_field]], on=pk_field, how="left", indicator=True)
            df_pd = df_pd[df_pd["_merge"] == "left_only"].drop(columns=["_merge"])
            load_logger.info(f"{len(df_pd)} new rows detected for {table_name}")
        
        if len(df_pd) == 0:
            load_logger.info(f"No new rows to load for {table_name}")
            continue
        
        # Loading in chunks
        for start in range(0, len(df_pd), chunk_size):
            end = start + chunk_size
            chunk_df = df_pd.iloc[start:end]

            try:
                job = client.load_table_from_dataframe(
                    chunk_df,
                    table_ref,
                    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                )
                job.result()  # Wait for completion
                load_logger.info(f"Loaded rows {start} to {end} into {table_name}")
            except Exception as e:
                load_logger.error(f"Error loading rows {start} to {end} into {table_name}: {e}")

        load_logger.info(f"Finished loading table {table_name}")

if __name__ == "__main__":
    load_to_bigquery()