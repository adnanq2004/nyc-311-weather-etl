# Importing libraries for making ER Diagram for the final data warehouse
from dataclasses import dataclass
from erdantic import create
import datetime


## Dimension Tables

# Date Dimension Table
@dataclass
class dim_date:
    date_id: int
    date: datetime.date
    day: int
    month: int
    year: int
    weekday: int
    weekday_name: str

# Location Dimension Table
@dataclass
class dim_location:
    location_id: int
    borough_id: int
    incident_zip: str
    borough: str
    city: str
    location_type: str
    latitude: float
    longitude: float

@dataclass
class dim_borough:
    borough_id: int
    borough_name: str

# Complaint Type Dimension
@dataclass
class dim_complaint_type:
    complaint_type_id: int
    complaint_type: str
    complaint_category: str
    complaint_descriptor: str

# Agency Dimension
@dataclass
class dim_agency:
    agency_id: int
    agency_acronym: str
    agency_name: str



## Fact Tables

# Incident Fact Table
@dataclass
class fact_incidents:
    incident_id: int
    date_id: dim_date
    location_id: dim_location
    borough_id: dim_borough
    agency_id: dim_agency
    complaint_type_id: dim_complaint_type
    created_date_id: dim_date
    closed_date_id: dim_date
    resolution_status: str
    time_to_resolve_interval: datetime.timedelta
    time_to_resolve_days: int 
    is_resolved_same_day: int
    complaint_count: int

# Weather Fact Table
@dataclass
class fact_weather:
    weather_id: int
    date: dim_date
    borough_id: dim_borough
    temperature_max: float
    temperature_min: float
    precipitation_total: float
    precipitation_hours: int
    rain_total: float
    showers_total: float
    snowfall_total: float
    windspeed_max: float
    windgust_max: float
    rain_flag: int
    showers_flag: int
    snow_flag: int
    high_wind_flag: int

# Daily Summary Fact Table
@dataclass
class fact_daily_summary:
    date: dim_date
    borough_id: dim_borough
    total_incidents: int
    time_to_resolve_days_avg: float
    percent_resolved_same_day: float
    temperature_avg: float
    temperature_max: float
    temperature_min: float
    precipitation_total: float
    precipitation_per_hour: float
    rain_total: float
    showers_total: float
    windspeed_max: float
    windgust_max: float
    resolved_same_day_flag: int
    rain_flag: int
    showers_flag: int
    snow_flag: int
    high_wind_flag: int


## Creating the ER Diagram
erd = create(
    dim_date,
    dim_location,
    dim_borough,
    dim_complaint_type,
    dim_agency,
    fact_incidents,
    fact_weather,
    fact_daily_summary
)


# Converting ERD to Graphviz
graph = erd.to_graphviz()

# Setting straight lines for the ERD and other settings for allignment
graph.graph_attr.update(
    rankdir = "LR",
    splines = "ortho",
)

# Setting size for graphviz
graph.graph_attr["size"] = "15,15!"

# Rendering the ER Diagram
graph.draw("data_warehouse_erd.png", prog = "dot")  # outputs PNG
