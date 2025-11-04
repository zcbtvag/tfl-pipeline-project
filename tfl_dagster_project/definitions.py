from dagster import (
    Definitions,
    load_assets_from_modules,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dbt import DbtCliResource
from pathlib import Path

from tfl_dagster_project.defs.assets import assets  # noqa: TID252
from tfl_dagster_project.defs.resources import database_resource

# Get the dbt project directory
PROJECT_ROOT = Path(__file__).parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "tfl_dbt"

all_assets = load_assets_from_modules([assets])

# Define a job that materializes all assets
tfl_pipeline_job = define_asset_job(
    name="tfl_pipeline_job",
    selection="*",  # Select all assets
)

# Schedule to run every 20 minutes
tfl_schedule = ScheduleDefinition(
    name="tfl_20min_schedule",
    job=tfl_pipeline_job,
    cron_schedule="*/20 * * * *",  # Every 20 minutes
    description="Fetches TfL bike point data and updates analytics every 20 minutes",
)

defs = Definitions(
    assets=all_assets,
    jobs=[tfl_pipeline_job],
    schedules=[tfl_schedule],
    resources={
        "database": database_resource,
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
        ),
    },
)
