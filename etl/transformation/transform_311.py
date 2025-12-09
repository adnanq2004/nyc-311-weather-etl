# Functions needed for transformation
import polars as pl
import json
from pathlib import Path
from logger.etl_logger import ETLLogger
from rapidfuzz import process, fuzz


# Settings for logging transformation
transform_logger = ETLLogger("transform_311").get()
transform_logger.info("Starting 311 data transformation")

# Settings for file path locations
project_root = Path(__file__).resolve().parents[2]
mapping_dir = project_root / "mappings"
metadata_folder = project_root / "metadata"
metadata_folder.mkdir(parents=True, exist_ok=True)

# Loading mappings
mappings = {}
for file in mapping_dir.glob("*.json"):
    with open(file, "r") as f:
        mappings[file.stem] = json.load(f)



# Function to set data types for each column
def data_type_transformer(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.col("unique_key").cast(pl.Utf8),
        pl.col("created_date").str.strptime(pl.Datetime, fmt = r"%Y-%m-%dT%H:%M:%S", strict = False),
        pl.col("closed_date").str.strptime(pl.Datetime, fmt = r"%Y-%m-%dT%H:%M:%S", strict = False),
        pl.col("agency").cast(pl.Utf8),
        pl.col("agency_name").cast(pl.Utf8),
        pl.col("complaint_type").cast(pl.Utf8),
        pl.col("descriptor").cast(pl.Utf8),
        pl.col("location_type").cast(pl.Utf8),
        pl.col("incident_zip").cast(pl.Utf8),
        pl.col("city").cast(pl.Utf8),
        pl.col("status").cast(pl.Utf8),
        pl.col("resolution_action_updated_date").str.strptime(pl.Datetime, fmt = r"%Y-%m-%dT%H:%M:%S", strict = False),
        pl.col("borough").cast(pl.Utf8),
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64)
    ])
    transform_logger.info("Data types transformed")
    return df




# Funciton to clean string columns before mapping them
def clean_strings_before_mapping(df: pl.DataFrame, cols) -> pl.DataFrame:
    for col in cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                .str.strip_chars()
                .str.replace_all(r"\s+", " ")
                .alias(col)
            )
    transform_logger.info("String columns cleaned")
    return df




# Function to filter newly pulled data with complaint types not relevant.
def filter_relevant_complaints(df: pl.DataFrame, relevant_complaints) -> pl.DataFrame:
    df = df.filter(
        (pl.col("complaint_type").str.contains(r"^[a-zA-Z0-9\s\.,\-\(\)&]+$")) &
        (pl.col("complaint_type").is_in(relevant_complaints))
    )
    transform_logger.info(f"Filtered relevant complaints: {df.height} rows remain")
    return df




# Function to deduplicate data based duplicate "unique_key", keeping the first record
def dedupe(df: pl.DataFrame) -> pl.DataFrame:
    df = df.sort("created_date", reverse = False)
    transform_logger.info(f"Deduplicated complaints: {df.height} rows remain")
    return df.unique(subset = "unique_key", keep = "first")




# Function to clean zip codes - replacing zip codes under 4 digits to null, and replacing zip codes with over 5 to only the first 5
def clean_zip_codes(df: pl.DataFrame, zip_col: str = "incident_zip") -> pl.DataFrame:
    if zip_col in df.columns:
        df = df.with_columns(
            pl.when(pl.col(zip_col).str.len_chars() < 5)
            .then(None)
            .otherwise(pl.col(zip_col).str.slice(0, 5))
            .alias(zip_col)
        )
    transform_logger.info("Zip codes cleaned")
    return df




# Function to make a select list of string columns to be title cased
def title_casing(df: pl.DataFrame) -> pl.DataFrame:
    title_case_columns = [
        "descriptor", "location_type", "city", "status",
        "borough", "complaint_category", "agency_name", "complaint_type"
    ]
    for col in title_case_columns:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                .str.to_titlecase()
                .alias(col)
            )
    
    # Making the agency acronym column all caps
    if "agency" in df.columns:
        df = df.with_columns(
            pl.col("agency").str.to_uppercase().alias("agency")
        )
        
    transform_logger.info("Applied title casing")
    return df



# Function to apply mappings based on JSON files in order to reduce computation for common mispellings
# Falls back on fuzzy string matching if there are categories that do not match keys (80% similiarity score matching)
def apply_mapping(df: pl.DataFrame, column, mapping, use_fuzzy = True, score_cutoff = 80) -> pl.DataFrame:
    if column not in df.columns or not mapping:
        return df
    
    # Applying mappings before falling back to fuzzy string matching
    df = df.with_columns(
        pl.col(column)
        .map_elements(lambda x: mapping.get(x, x))
        .alias(column)
    )
    
    if use_fuzzy:
        # Collecting unique values that still are not mapped after applying mappings (still unclean categories)
        unmapped = [x for x in df[column].unique().to_list() if x not in mapping.values() and x is not None]
        
        # Dictionary for fuzzy mapping
        fuzzy_map = {}
        for val in unmapped:
            # Finding the best match based on an 80% similiary cutoff score to exisiting categories.
            best_match = process.extractOne(val, mapping.keys(), scorer = fuzz.ratio)
            if best_match and best_match[1] >= score_cutoff:
                fuzzy_map[val] = mapping[best_match[0]]
                
        if fuzzy_map:
            df = df.with_columns(
                pl.col(column)
                .map_elements(lambda x: fuzzy_map.get(x, x))
                .alias(column)
            )
    transform_logger.info(f"Applied mapping on {column} with fuzzy matching: {use_fuzzy}")
    return df



def transform_311(df: pl.DataFrame, mappings: dict) -> pl.DataFrame:
    df = data_type_transformer(df)
    
    df = clean_strings_before_mapping(df, [
        "complaint_type", "location_type", "city", "borough", "agency_name", "descriptor"
    ])
    
    df = filter_relevant_complaints(df, mappings["relevant_complaints"])
    
    df = dedupe(df)
    
    mapping_columns = {
        "complaint_type": "complaint_mapping",
        "complaint_category": "complaint_categories",
        "agency": "agency_mapping",
        "agency_name": "agency_name_mapping",
        "city": "city_mapping",
        "borough": "borough_mapping",
        "location_type": "location_type_mapping"
    }
    for col, mapping_name in mapping_columns.items():
        df = apply_mapping(df, col, mappings.get(mapping_name, {}))

    df = clean_zip_codes(df)

    df = title_casing(df)

    str_columns = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
    df = df.with_columns([
        pl.when(pl.col(col) == "missing")
        .then(None)
        .otherwise(pl.col(col))
        .alias(col)
        for col in str_columns
    ])

    return df


# Function entry point for the main 311 transformation function
if __name__ == "__main__":
    transform_311()