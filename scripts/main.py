# main.py
import os
import pandas as pd
from etl_utils import get_engine

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
    s3_uri = f"s3://{bucket}/{key}"
    print(f"Loading {s3_uri} → {table_name} ...")
    # With IAM role on EC2, no explicit storage_options needed.
    df = pd.read_csv(s3_uri)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} rows into {table_name}.")

def main():
    print("[MODE] cloud → S3 ➜ RDS")
    bucket = _require("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX", "").lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    engine = get_engine()

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
