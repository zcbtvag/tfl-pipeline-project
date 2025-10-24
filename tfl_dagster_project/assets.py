import os
import requests
import duckdb
from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Get API key from environment variable
API_KEY = os.getenv("TFL_API_KEY")

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent
DUCKDB_PATH = PROJECT_ROOT / "tfl_data.duckdb"
DBT_PROJECT_DIR = PROJECT_ROOT / "tfl_dbt"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


@asset
def raw_tfl_bike_points(context: AssetExecutionContext):
    """
    Extract TfL bike point data from API and load into DuckDB raw schema.
    This is Layer 1: Raw data ingestion with minimal transformation.
    """
    # Fetch data from TfL API
    url = "https://api.tfl.gov.uk/BikePoint"
    context.log.info(f"Fetching bike point data from TfL API: {url}")
    response = requests.get(url, params={"app_key": API_KEY})
    response.raise_for_status()
    data = response.json()
    context.log.info(f"Fetched {len(data)} bike points from API")

    # Connect to DuckDB and create raw schema
    conn = duckdb.connect(str(DUCKDB_PATH))

    # Create raw schema if it doesn't exist
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Drop and recreate the raw table
    conn.execute("DROP TABLE IF EXISTS raw.bike_points")

    # Insert data - DuckDB will infer schema from JSON
    context.log.info("Loading data into DuckDB raw.bike_points table")
    conn.execute("""
        CREATE TABLE raw.bike_points AS
        SELECT * FROM read_json_auto(?)
    """, [data])

    # Get row count for logging
    row_count = conn.execute("SELECT COUNT(*) FROM raw.bike_points").fetchone()[0]
    context.log.info(f"Successfully loaded {row_count} rows into raw.bike_points")

    conn.close()

    return {
        "rows_loaded": row_count,
        "table": "raw.bike_points"
    }


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
)
def tfl_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    dbt models for TfL data transformation.
    Layer 2 (Staging): stg_bike_points - cleaned and flattened data
    Layer 3 (Marts): bike_point_availability - analytics-ready metrics
    """
    yield from dbt.cli(["build"], context=context).stream()
