from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from pathlib import Path

from tfl_dagster_project import assets  # noqa: TID252

# Get the dbt project directory
PROJECT_ROOT = Path(__file__).parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "tfl_dbt"

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
        ),
    },
)
