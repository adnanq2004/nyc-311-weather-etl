# transform/transform_311_weather.py
import polars as pl

def transform_combined(cases: pl.DataFrame, weather: pl.DataFrame) -> dict:
    # Ensuring data types
    cases = cases.with_columns([
        pl.col("unique_key").cast(pl.Int64),
        pl.col("created_date").cast(pl.Date),
        pl.col("closed_date").cast(pl.Date),
        pl.col("resolution_action_updated_date").cast(pl.Date),
        pl.col("agency").cast(pl.Utf8),
        pl.col("agency_name").cast(pl.Utf8),
        pl.col("complaint_type").cast(pl.Utf8),
        pl.col("descriptor").cast(pl.Utf8),
        pl.col("location_type").cast(pl.Utf8),
        pl.col("incident_zip").cast(pl.Utf8),
        pl.col("city").cast(pl.Utf8),
        pl.col("status").cast(pl.Utf8),
        pl.col("borough").cast(pl.Utf8),
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
        pl.col("complaint_category").cast(pl.Utf8),
    ])
    cases = cases.with_columns([
        pl.when(pl.col("closed_date") < pl.col("created_date"))
        .then(None)
        .otherwise(pl.col("closed_date"))
        .alias("closed_date")
    ])

    weather = weather.with_columns([
        pl.col("time").str.strptime(pl.Date, "%Y-%m-%d").alias("date"),
        pl.col("temperature_2m_max").cast(pl.Float64),
        pl.col("temperature_2m_min").cast(pl.Float64),
        pl.col("precipitation_sum").cast(pl.Float64),
        pl.col("precipitation_hours").cast(pl.Int64),
        pl.col("rain_sum").cast(pl.Float64),
        pl.col("showers_sum").cast(pl.Float64),
        pl.col("snowfall_sum").cast(pl.Float64),
        pl.col("windspeed_10m_max").cast(pl.Float64),
        pl.col("windgusts_10m_max").cast(pl.Float64),
        pl.col("borough").cast(pl.Utf8),
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
    ])

    # dim_date
    dim_date = pl.concat([
        cases.select(["created_date", "closed_date"]).melt().select("value").rename({"value":"date"}),
        weather.select(["time"]).rename({"time":"date"})
    ]).unique().with_columns([
        pl.col("date").cast(pl.Date),
        pl.arange(1, pl.count() + 1).alias("date_id")
    ]).sort("date")

    dim_date = dim_date.with_columns([
        pl.col("date").dt.day().alias("day"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.weekday().alias("weekday"),
        pl.col("date").dt.strftime("%A").alias("weekday_name")
    ])
    
    # dim_borough 
    dim_borough = pl.DataFrame({
        "borough_id": [1, 2, 3, 4, 5],
        "borough_name": ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
    })

    # dim_location
    weather_loc = weather.select([
        pl.col("borough"),
        pl.col("latitude"),
        pl.col("longitude"),
        pl.lit(None).cast(pl.Utf8).alias("city"),
        pl.lit(None).cast(pl.Utf8).alias("location_type"),
        pl.lit(None).cast(pl.Utf8).alias("incident_zip"),
    ])
    dim_location = pl.concat([
        cases.select(["borough","latitude","longitude","city","location_type","incident_zip"]),
        weather_loc
    ]).unique().with_columns([
        pl.arange(1, pl.count() + 1).alias("location_id")
    ])
    dim_location = dim_location.select([
        "location_id","incident_zip","borough","city","location_type","latitude","longitude"
    ])
    
    # dim_agency
    dim_agency = cases.select(["agency","agency_name"]).unique().with_columns([
        pl.arange(1, pl.count() + 1).alias("agency_id")
    ])
    
    # dim_complaint_type
    dim_complaint_type = cases.select(["complaint_type","descriptor","complaint_category"]).unique().with_columns([
        pl.arange(1, pl.count() + 1).alias("complaint_type_id")
    ])
    
    
    # fact_incidents
    fact_incidents = cases.join(
        dim_date.rename({"date": "created_date", "date_id": "created_date_id"}), 
        on="created_date", how="left"
    ).join(
        dim_date.rename({"date": "closed_date", "date_id": "closed_date_id"}), 
        on="closed_date", how="left"
    ).join(
        dim_location, 
        on=["borough", "latitude", "longitude"], how="left"
    ).join(
        dim_borough, on="borough", how="left"
    ).join(
        dim_agency, on="agency", how="left"
    ).join(
        dim_complaint_type, 
        on=["complaint_type", "descriptor", "complaint_category"], how="left"
    ).with_columns([
        ((pl.col("closed_date") - pl.col("created_date"))).alias("time_to_resolve_interval"),
        ((pl.col("closed_date") == pl.col("created_date")).cast(pl.Int64)).alias("is_resolved_same_day"),
        pl.lit(1).alias("complaint_count")
    ])
    
    # Dropping unnecessary columns
    drop_cols_incidents = [
        "agency_name", "complaint_type", "descriptor", "location_type", 
        "incident_zip", "city", "status", "resolution_action_updated_date", 
        "borough", "latitude", "longitude"
    ]
    fact_incidents = fact_incidents.drop([col for col in drop_cols_incidents if col in fact_incidents.columns])
    
    # fact_weather
    weather = weather.with_columns([pl.col("time").cast(pl.Date).alias("date")])
    
    fact_weather = weather.rename({
        "temperature_2m_max": "temperature_max",
        "temperature_2m_min": "temperature_min",
        "precipitation_sum": "precipitation_total",
        "rain_sum": "rain_total",
        "showers_sum": "showers_total",
        "snowfall_sum": "snowfall_total",
        "windspeed_10m_max": "windspeed_max",
        "windgusts_10m_max": "windgust_max"
    }).join(
        dim_date.rename({"date": "date", "date_id": "date_id"}), on="date", how="left"
    ).join(
        dim_borough, on="borough", how="left"
    ).with_columns([
        (pl.col("rain_total") > 0).cast(pl.Int64).alias("rain_flag"),
        (pl.col("showers_total") > 0).cast(pl.Int64).alias("showers_flag"),
        (pl.col("snowfall_total") > 0).cast(pl.Int64).alias("snow_flag"),
        ((pl.col("windspeed_max") > 15) | (pl.col("windgust_max") > 20)).cast(pl.Int64).alias("high_wind_flag")
    ])
    # Filtering cols
    fact_weather = fact_weather.select([
        "date_id", "borough_id", "temperature_max", "temperature_min", "precipitation_total",
        "precipitation_hours", "rain_total", "showers_total", "snowfall_total", "windspeed_max",
        "windgust_max", "rain_flag", "showers_flag", "snow_flag", "high_wind_flag"
    ])
    
    # fact_daily_summary 
    daily_incidents = fact_incidents.group_by(["created_date_id","borough_id"]).agg([
        pl.count("incident_id").alias("total_incidents"),
        (pl.col("is_resolved_same_day").mean() * 100).alias("percent_resolved_same_day")
    ])
    
    daily_weather = fact_weather.group_by(["date_id","borough_id"]).agg([
        pl.col("temperature_max").mean().alias("temperature_max"),
        pl.col("temperature_min").mean().alias("temperature_min"),
        ((pl.col("temperature_max") + pl.col("temperature_min"))/2).mean().alias("temperature_avg"),
        pl.col("precipitation_total").sum().alias("precipitation_total"),
        (pl.when(pl.col("precipitation_total") > 0).then(pl.col("precipitation_total")/24).otherwise(0)).alias("precipitation_per_hour"),
        pl.col("rain_total").sum().alias("rain_total"),
        pl.col("showers_total").sum().alias("showers_total"),
        pl.col("snowfall_total").sum().alias("snowfall_total"),
        pl.col("windspeed_max").max().alias("windspeed_max"),
        pl.col("windgust_max").max().alias("windgust_max"),
        pl.col("rain_flag").max().alias("rain_flag"),
        pl.col("showers_flag").max().alias("showers_flag"),
        pl.col("snow_flag").max().alias("snow_flag"),
        pl.col("high_wind_flag").max().alias("high_wind_flag")
    ])
    
    fact_daily_summary = daily_incidents.join(
        daily_weather, left_on=["created_date_id","borough_id"], right_on=["date_id","borough_id"], how="left"
    )
    
    # Renamung columns to match BigQuery schema
    dim_date = dim_date.rename({
        "date_id":"date_id","date":"date","day":"day","month":"month",
        "year":"year","weekday":"weekday","weekday_name":"weekday_name"
    })
    dim_borough = dim_borough.rename({"borough_id":"borough_id","borough_name":"borough_name"})
    dim_location = dim_location.rename({
        "location_id":"location_id","incident_zip":"incident_zip","borough":"borough",
        "city":"city","location_type":"location_type","latitude":"latitude","longitude":"longitude"
    })
    dim_agency = dim_agency.rename({
        "agency_id":"agency_id","agency":"agency","agency_name":"agency_name"
    })
    dim_complaint_type = dim_complaint_type.rename({
        "complaint_type_id":"complaint_type_id","complaint_type":"complaint_type",
        "complaint_category":"complaint_category","descriptor":"complaint_descriptor"
    })
    fact_incidents = fact_incidents.rename({
        "unique_key":"incident_id","created_date_id":"created_date_id","closed_date_id":"closed_date_id",
        "agency_id":"agency_id","complaint_type_id":"complaint_type_id","date_id":"date_id",
        "location_id":"location_id","resolution_status":"resolution_status",
        "time_to_resolve_interval":"time_to_resolve_interval",
        "is_resolved_same_day":"is_resolved_same_day",
        "complaint_count":"complaint_count"
    })
    fact_weather = fact_weather.rename({
        "date_id":"date_id","borough_id":"borough_id","temperature_max":"temperature_max",
        "temperature_min":"temperature_min","precipitation_total":"precipitation_total",
        "precipitation_hours":"precipitation_hours","rain_total":"rain_total",
        "showers_total":"showers_total","snowfall_total":"snowfall_total","windspeed_max":"windspeed_max",
        "windgust_max":"windgust_max","rain_flag":"rain_flag","showers_flag":"showers_flag",
        "snow_flag":"snow_flag","high_wind_flag":"high_wind_flag"
    })
    fact_daily_summary = fact_daily_summary.rename({
        "created_date_id":"date_id","borough_id":"borough_id","total_incidents":"total_incidents",
        "percent_resolved_same_day":"percent_resolved_same_day","temperature_avg":"temperature_avg",
        "temperature_max":"temperature_max","temperature_min":"temperature_min",
        "precipitation_total":"precipitation_total","precipitation_per_hour":"precipitation_per_hour",
        "rain_total":"rain_total","showers_total":"showers_total","snowfall_total":"snowfall_total",
        "windspeed_max":"windspeed_max","windgust_max":"windgust_max",
        "rain_flag":"rain_flag","showers_flag":"showers_flag","snow_flag":"snow_flag",
        "high_wind_flag":"high_wind_flag"
    })
    
    # Return all tables in a dict
    return {
        "dim_date": dim_date,
        "dim_borough": dim_borough,
        "dim_location": dim_location,
        "dim_agency": dim_agency,
        "dim_complaint_type": dim_complaint_type,
        "fact_incidents": fact_incidents,
        "fact_weather": fact_weather,
        "fact_daily_summary": fact_daily_summary
    }

# Function entry point
if __name__ == "__main__":
    transform_combined()