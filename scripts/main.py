# main.py  — CLOUD RAW LOADER (S3 -> RDS)
# -----------------------------------------------------------------------------
# WHY THIS CHANGE:
# - Default pandas.to_sql() issues 1 INSERT per row (very slow to remote RDS).
# - We switch to batched, multi-row INSERTs:
#       method='multi', chunksize=10000
#   This keeps your logic identical (replace, same schema), but reduces
#   network round-trips ~10,000x. No infra changes, no new deps.
#
# WHAT STAYS THE SAME:
# - Reads CSVs from S3 using the EC2 instance role (no explicit creds).
# - Overwrites the target table each run (idempotent raw layer).
# - Uses your existing SQLAlchemy engine (pool_pre_ping=True).
# -----------------------------------------------------------------------------

import os
import pandas as pd
from etl_utils import get_engine  # builds psycopg2 engine with pool_pre_ping

# Map S3 CSV filenames to target raw tables
FILES_TO_LOAD = {
    'olist_customers_dataset.csv': 'customers',
    'olist_geolocation_dataset.csv': 'geolocation',
    'olist_order_items_dataset.csv': 'order_items',
    'olist_order_payments_dataset.csv': 'order_payments',
    'olist_order_reviews_dataset.csv': 'order_reviews',
    'olist_orders_dataset.csv': 'orders',
    'olist_products_dataset.csv': 'products',
    'olist_sellers_dataset.csv': 'sellers',
    'product_category_name_translation.csv': 'product_category_name_translation'
}

def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def load_csv_from_s3(bucket: str, key: str, table_name: str, engine):
    """
    Stream a CSV from S3 and write to RDS.
    Minimal fix: use batched multi-row INSERTs for remote Postgres.
    """
    s3_uri = f"s3://{bucket}/{key}"
    print(f"Loading {s3_uri} → {table_name} ...")

    # With an EC2 IAM role + S3 gateway endpoint, pandas can read s3:// directly.
    # (No storage_options needed.)
    df = pd.read_csv(s3_uri)

    # *** THE ONE-LINE FIX ***
    # Before (slow over the network):
    #   df.to_sql(table_name, engine, if_exists='replace', index=False)
    # After (fast, same result/behavior):
    df.to_sql(
        table_name,
        engine,
        if_exists='replace',   # idempotent raw load
        index=False,
        method='multi',        # multi-row INSERT statements
        chunksize=10000        # send 10k rows per batch (safe for RDS)
    )

    print(f"Loaded {len(df)} rows into {table_name}.")

def main():
    print("[MODE] cloud → S3 ➜ RDS")
    bucket = _require("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX", "").lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    engine = get_engine()  # psycopg2 + pool_pre_ping (keeps long jobs stable)

    try:
        for filename, table in FILES_TO_LOAD.items():
            key = f"{prefix}{filename}"
            load_csv_from_s3(bucket, key, table, engine)
        print("\nETL raw load completed successfully.")
    except Exception as e:
        print(f"An error occurred in main.py: {e}")
        raise

if __name__ == "__main__":
    main()
