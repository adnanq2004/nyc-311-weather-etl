# Importing google cloud bigquery
from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path


# File location path settings
script_dir = Path(__file__).parent
credentials_path = script_dir.parent / "credentials" / "bigquery_nyc_weather_etl_credentials.json"


# Setting BigQuery credentials
credentials = service_account.Credentials.from_service_account_file(credentials_path)


# Intializing client with project name, dataset ID, and dataset reference
client = bigquery.Client(project = "nyc-311-weather-etl", credentials = credentials)
dataset_id = "nyc_311_weather"
dataset_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
dataset_ref.location = "US"
dataset = client.create_dataset(dataset_ref, exists_ok = True)



# Helper function to help create BigQuery tables
def create_table(table_id, schema):
    table_ref = client.dataset(dataset_id).table(table_id)
    # Delete table if it exists
    client.delete_table(table_ref, not_found_ok = True)
    # Create the table
    table = bigquery.Table(table_ref, schema = schema)
    table = client.create_table(table)
    print(f"Created or replaced table: {table.project}.{table.dataset_id}.{table.table_id}")


## Dimension Tables 


# Creating dim_date Schema
dim_date_schema = [
    bigquery.SchemaField("date_id", "INT64", mode = "REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode = "REQUIRED"),
    bigquery.SchemaField("day", "INT64"),
    bigquery.SchemaField("month", "INT64"),
    bigquery.SchemaField("year", "INT64"),
    bigquery.SchemaField("weekday", "INT64"),
    bigquery.SchemaField("weekday_name", "STRING"),
]
# Creating dim_date table
create_table("dim_date", dim_date_schema)


# Creating dim_borough schema
dim_borough_schema = [
    bigquery.SchemaField("borough_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("borough_name", "STRING"),
]
# Creating dim_borough table
create_table("dim_borough", dim_borough_schema)

# Creating dim_location schema
dim_location_schema = [
    bigquery.SchemaField("location_id", "INT64", mode = "REQUIRED"),
    bigquery.SchemaField("incident_zip", "STRING"),
    bigquery.SchemaField("borough", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("location_type", "STRING"),
    bigquery.SchemaField("latitude", "FLOAT"),
    bigquery.SchemaField("longitude", "FLOAT"),
]
# Creating dim_location table
create_table("dim_location", dim_location_schema)


# Creating dim_complaint_type schema
dim_complaint_type_schema = [
    bigquery.SchemaField("complaint_type_id", "INT64", mode = "REQUIRED"),
    bigquery.SchemaField("complaint_type", "STRING"),
    bigquery.SchemaField("complaint_category", "STRING"),
    bigquery.SchemaField("complaint_descriptor", "STRING"),
]
# Creating dim_complaint_type table
create_table("dim_complaint_type", dim_complaint_type_schema)


# Creating dim_agency schema
dim_agency_schema = [
    bigquery.SchemaField("agency_id", "INT64", mode = "REQUIRED"),
    bigquery.SchemaField("agency", "STRING"),
    bigquery.SchemaField("agency_name", "STRING"),
]
# Creating dim_agency table
create_table("dim_agency", dim_agency_schema)



## Fact Tables


# Creating fact_incidents schema
fact_incidents_schema = [
    bigquery.SchemaField("incident_id", "INT64", mode = "REQUIRED"),
    bigquery.SchemaField("date_id", "INT64"),
    bigquery.SchemaField("location_id", "INT64"),
    bigquery.SchemaField("agency_id", "INT64"),
    bigquery.SchemaField("complaint_type_id", "INT64"),
    bigquery.SchemaField("created_date_id", "INT64"),
    bigquery.SchemaField("closed_date_id", "INT64"),
    bigquery.SchemaField("resolution_status", "STRING"),
    bigquery.SchemaField("time_to_resolve_interval", "STRING"),
    bigquery.SchemaField("is_resolved_same_day", "INT64"),
    bigquery.SchemaField("complaint_count", "INT64"),
]
# Creating fact_incidents table
create_table("fact_incidents", fact_incidents_schema)


# Creating fact_weather schema
fact_weather_schema = [
    bigquery.SchemaField("weather_id", "INT64", mode = "REQUIRED"),
    bigquery.SchemaField("date_id", "INT64"),
    bigquery.SchemaField("borough_id", "INT64"),
    bigquery.SchemaField("temperature_max", "FLOAT"),
    bigquery.SchemaField("temperature_min", "FLOAT"),
    bigquery.SchemaField("precipitation_total", "FLOAT"),
    bigquery.SchemaField("precipitation_hours", "INT64"),
    bigquery.SchemaField("rain_total", "FLOAT"),
    bigquery.SchemaField("showers_total", "FLOAT"),
    bigquery.SchemaField("snowfall_total", "FLOAT"),
    bigquery.SchemaField("windspeed_max", "FLOAT"),
    bigquery.SchemaField("windgust_max", "FLOAT"),
    bigquery.SchemaField("rain_flag", "INT64"),
    bigquery.SchemaField("showers_flag", "INT64"),
    bigquery.SchemaField("snow_flag", "INT64"),
    bigquery.SchemaField("high_wind_flag", "INT64"),
]
# Creating fact_weather table
create_table("fact_weather", fact_weather_schema)


# Crating fact_daily_summary schema
fact_daily_summary_schema = [
    bigquery.SchemaField("date_id", "INT64"),
    bigquery.SchemaField("borough_id", "INT64"),
    bigquery.SchemaField("total_incidents", "INT64"),
    bigquery.SchemaField("percent_resolved_same_day", "FLOAT"),
    bigquery.SchemaField("temperature_avg", "FLOAT"),
    bigquery.SchemaField("temperature_max", "FLOAT"),
    bigquery.SchemaField("temperature_min", "FLOAT"),
    bigquery.SchemaField("precipitation_total", "FLOAT"),
    bigquery.SchemaField("precipitation_per_hour", "FLOAT"),
    bigquery.SchemaField("rain_total", "FLOAT"),
    bigquery.SchemaField("showers_total", "FLOAT"),
    bigquery.SchemaField("windspeed_max", "FLOAT"),
    bigquery.SchemaField("windgust_max", "FLOAT"),
    bigquery.SchemaField("resolved_same_day_flag", "INT64"),
    bigquery.SchemaField("rain_flag", "INT64"),
    bigquery.SchemaField("showers_flag", "INT64"),
    bigquery.SchemaField("snow_flag", "INT64"),
    bigquery.SchemaField("high_wind_flag", "INT64"),
]
# Creating fact_daily_summary table
create_table("fact_daily_summary", fact_daily_summary_schema)