"""Settings from hc-pipeline/.env"""
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def path(name):
    return Path(os.path.expanduser(os.environ[name]))


S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
MIGRATION_PROJECT = path("MIGRATION_PROJECT")
DBT_DIR = MIGRATION_PROJECT / "dbt"
DBT_BIN = MIGRATION_PROJECT / ".venv" / "bin" / "dbt"
SNOWPIPE_TIMEOUT = int(os.getenv("SNOWPIPE_TIMEOUT", "600"))
APPROVAL_MODE = os.getenv("APPROVAL_MODE", "auto").lower()
DATABASE = os.getenv("SNOWFLAKE_DATABASE", "HC_MIGRATION_DB")
