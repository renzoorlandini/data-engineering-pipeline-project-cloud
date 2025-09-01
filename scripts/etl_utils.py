# etl_utils.py
import os
from sqlalchemy import create_engine

def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def build_db_url() -> str:
    """
    Build SQLAlchemy URL for Amazon RDS for PostgreSQL (cloud-only).
    Uses password auth. If you enforce TLS, append sslmode=require.
    """
    host = _require("RDS_HOST")
    port = os.getenv("RDS_PORT", "5432")
    db   = os.getenv("RDS_DB", "ecommerce")
    user = os.getenv("RDS_USER", "postgres")
    pwd  = _require("RDS_PASSWORD")

    # If your RDS requires TLS:
    # return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode=require"
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

def get_engine():
    return create_engine(build_db_url(), pool_pre_ping=True)
