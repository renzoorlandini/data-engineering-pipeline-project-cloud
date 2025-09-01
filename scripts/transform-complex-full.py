# transform.py
"""
Builds the production-grade master_table (order-item grain) directly in RDS.

- All cleaning is performed in SQL (type casting, text normalization, NULL handling).
- Joins orders/customers/items/products/sellers/payments/reviews.
- Resolves customer/seller location_id via dim_locations (zip_prefix + city + state).
- Adds delivery KPIs and useful indexes.
"""

from sqlalchemy import text
from etl_utils import get_engine

MASTER_SQL = """
BEGIN;

DROP TABLE IF EXISTS master_table;

WITH
c_orders AS (
  SELECT
      o.order_id,
      o.customer_id,
      LOWER(TRIM(o.order_status))               AS order_status,
      CAST(o.order_purchase_timestamp AS timestamp)        AS order_purchase_ts,
      CAST(o.order_approved_at        AS timestamp)        AS order_approved_ts,
      CAST(o.order_delivered_carrier_date AS timestamp)    AS order_delivered_carrier_ts,
      CAST(o.order_delivered_customer_date AS timestamp)   AS order_delivered_customer_ts,
      CAST(o.order_estimated_delivery_date AS timestamp)   AS order_estimated_delivery_ts
  FROM orders o
),
c_customers AS (
  SELECT
      c.customer_id,
      TRIM(c.customer_zip_code_prefix)::text              AS customer_zip_prefix,
      UPPER(TRIM(c.customer_city))                        AS customer_city_norm,
      UPPER(TRIM(c.customer_state))                       AS customer_state_norm
  FROM customers c
),
c_sellers AS (
  SELECT
      s.seller_id,
      TRIM(s.seller_zip_code_prefix)::text               AS seller_zip_prefix,
      UPPER(TRIM(s.seller_city))                         AS seller_city_norm,
      UPPER(TRIM(s.seller_state))                        AS seller_state_norm
  FROM sellers s
),
c_products AS (
  SELECT
      p.product_id,
      LOWER(TRIM(p.product_category_name)) AS product_category_pt,
      LOWER(TRIM(COALESCE(t.product_category_name_english, p.product_category_name))) AS product_category_en
  FROM products p
  LEFT JOIN product_category_name_translation t
    ON t.product_category_name = p.product_category_name
),
c_items AS (
  SELECT
      oi.order_id,
      oi.order_item_id,
      oi.product_id,
      oi.seller_id,
      CAST(oi.price AS numeric(12,2))         AS item_price,
      CAST(oi.freight_value AS numeric(12,2)) AS item_freight
  FROM order_items oi
),
pay_agg AS (
  SELECT
      op.order_id,
      SUM(CAST(op.payment_value AS numeric(12,2)))                AS total_payment_value,
      COALESCE(SUM(NULLIF(op.payment_installments,0)), 0)         AS total_installments,
      (
        SELECT op2.payment_type
        FROM order_payments op2
        WHERE op2.order_id = op.order_id
        GROUP BY op2.payment_type
        ORDER BY SUM(CAST(op2.payment_value AS numeric(12,2))) DESC
        LIMIT 1
      ) AS primary_payment_type
  FROM order_payments op
  GROUP BY op.order_id
),
rev_agg AS (
  SELECT
      r.order_id,
      AVG(CAST(r.review_score AS numeric))                       AS review_score_avg,
      MIN(CAST(r.review_creation_date AS timestamp))             AS first_review_creation_ts,
      MAX(CAST(r.review_answer_timestamp AS timestamp))          AS last_review_answer_ts
  FROM order_reviews r
  GROUP BY r.order_id
),
loc AS (
  SELECT
      dl.location_id,
      TRIM(dl.zip_code_prefix)::text             AS zip_prefix,
      UPPER(TRIM(dl.city))                       AS city_norm,
      UPPER(TRIM(dl.state_code))                 AS state_norm
  FROM dim_locations dl
),
cust_with_loc AS (
  SELECT
      c.customer_id,
      c.customer_zip_prefix,
      c.customer_city_norm,
      c.customer_state_norm,
      l.location_id AS customer_location_id
  FROM c_customers c
  LEFT JOIN loc l
    ON l.zip_prefix = c.customer_zip_prefix
   AND l.city_norm  = c.customer_city_norm
   AND l.state_norm = c.customer_state_norm
),
seller_with_loc AS (
  SELECT
      s.seller_id,
      s.seller_zip_prefix,
      s.seller_city_norm,
      s.seller_state_norm,
      l.location_id AS seller_location_id
  FROM c_sellers s
  LEFT JOIN loc l
    ON l.zip_prefix = s.seller_zip_prefix
   AND l.city_norm  = s.seller_city_norm
   AND l.state_norm = s.seller_state_norm
)

CREATE TABLE master_table AS
SELECT
    it.order_id,
    it.order_item_id,

    o.order_status,
    o.order_purchase_ts,
    o.order_approved_ts,
    o.order_delivered_carrier_ts,
    o.order_delivered_customer_ts,
    o.order_estimated_delivery_ts,

    o.customer_id,
    cwl.customer_location_id,
    cwl.customer_zip_prefix,
    cwl.customer_city_norm,
    cwl.customer_state_norm,

    it.seller_id,
    swl.seller_location_id,
    swl.seller_zip_prefix,
    swl.seller_city_norm,
    swl.seller_state_norm,

    it.product_id,
    pr.product_category_pt,
    pr.product_category_en,

    it.item_price,
    it.item_freight,
    (it.item_price + it.item_freight) AS item_gross_revenue,

    pa.total_payment_value,
    pa.total_installments,
    LOWER(TRIM(pa.primary_payment_type)) AS primary_payment_type,

    ra.review_score_avg,
    ra.first_review_creation_ts,
    ra.last_review_answer_ts,

    CASE
      WHEN o.order_delivered_customer_ts IS NOT NULL AND o.order_purchase_ts IS NOT NULL
        THEN EXTRACT(EPOCH FROM (o.order_delivered_customer_ts - o.order_purchase_ts)) / 86400.0
      ELSE NULL
    END AS delivery_days_actual,

    CASE
      WHEN o.order_estimated_delivery_ts IS NOT NULL AND o.order_purchase_ts IS NOT NULL
        THEN EXTRACT(EPOCH FROM (o.order_estimated_delivery_ts - o.order_purchase_ts)) / 86400.0
      ELSE NULL
    END AS delivery_days_estimated,

    CASE
      WHEN o.order_delivered_customer_ts IS NOT NULL
       AND o.order_estimated_delivery_ts IS NOT NULL
        THEN EXTRACT(EPOCH FROM (o.order_delivered_customer_ts - o.order_estimated_delivery_ts)) / 86400.0
      ELSE NULL
    END AS delivery_delay_days,

    CASE
      WHEN o.order_delivered_customer_ts IS NOT NULL
       AND o.order_estimated_delivery_ts IS NOT NULL
       AND o.order_delivered_customer_ts > o.order_estimated_delivery_ts
        THEN TRUE
      WHEN o.order_delivered_customer_ts IS NOT NULL
       AND o.order_estimated_delivery_ts IS NOT NULL
        THEN FALSE
      ELSE NULL
    END AS delivered_late_flag

FROM c_items it
JOIN c_orders o
  ON o.order_id = it.order_id
LEFT JOIN pay_agg pa
  ON pa.order_id = it.order_id
LEFT JOIN rev_agg ra
  ON ra.order_id = it.order_id
LEFT JOIN c_products pr
  ON pr.product_id = it.product_id
LEFT JOIN cust_with_loc cwl
  ON cwl.customer_id = o.customer_id
LEFT JOIN seller_with_loc swl
  ON swl.seller_id = it.seller_id
;

ALTER TABLE master_table
  ADD CONSTRAINT pk_master_table PRIMARY KEY (order_id, order_item_id);

CREATE INDEX idx_master_order_status      ON master_table (order_status);
CREATE INDEX idx_master_purchase_ts       ON master_table (order_purchase_ts);
CREATE INDEX idx_master_product_category  ON master_table (product_category_en);
CREATE INDEX idx_master_customer_loc      ON master_table (customer_location_id);
CREATE INDEX idx_master_seller_loc        ON master_table (seller_location_id);
CREATE INDEX idx_master_seller_id         ON master_table (seller_id);
CREATE INDEX idx_master_product_id        ON master_table (product_id);

COMMIT;
"""

def main():
    print("Starting transform: building master_table in RDS...")
    engine = get_engine()
    try:
        with engine.begin() as conn:
            # Optional: enforce a ceiling to avoid runaway queries (e.g., 30 minutes)
            # conn.execute(text("SET LOCAL statement_timeout = '1800000';"))
            conn.execute(text(MASTER_SQL))
        print("master_table created and indexed successfully.")
    except Exception as e:
        print(f"An error occurred in transform.py: {e}")
        raise

if __name__ == "__main__":
    main()
