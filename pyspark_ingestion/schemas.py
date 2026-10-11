"""
Explicit PySpark schemas for Cartographer source ingestion.

Schema inference (inferSchema=True) is strictly forbidden (Finding F3).
Key Design Decisions:
1. Brazilian CEP zip code prefixes are StringType to preserve leading zeros (e.g., '01037').
2. Monetary metrics (price, freight, payments) are DecimalType(12, 2) to eliminate float drift.
3. Natural source typos (e.g., product_name_lenght) are preserved to maintain Raw Fidelity.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DecimalType,
    TimestampType
)

# 1. Orders
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), False),
    StructField("order_purchase_timestamp", TimestampType(), False),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), False),
])

# 2. Order Items
ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), False),
    StructField("price", DecimalType(12, 2), False),
    StructField("freight_value", DecimalType(12, 2), False),
])

# 3. Order Reviews
ORDER_REVIEWS_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", IntegerType(), False),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), False),
    StructField("review_answer_timestamp", TimestampType(), False),
])

# 4. Order Payments
ORDER_PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("payment_type", StringType(), False),
    StructField("payment_installments", IntegerType(), False),
    StructField("payment_value", DecimalType(12, 2), False),
])

# 5. Customers
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), False),
    StructField("customer_zip_code_prefix", StringType(), False),  # Leading zero preservation
    StructField("customer_city", StringType(), False),
    StructField("customer_state", StringType(), False),
])

# 6. Sellers
SELLERS_SCHEMA = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), False),    # Leading zero preservation
    StructField("seller_city", StringType(), False),
    StructField("seller_state", StringType(), False),
])

# 7. Products
PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),        # Preserved typo (Raw Fidelity)
    StructField("product_description_lenght", IntegerType(), True), # Preserved typo (Raw Fidelity)
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True),
])

# 8. Product Category Translations
CATEGORY_TRANSLATION_SCHEMA = StructType([
    StructField("product_category_name", StringType(), False),
    StructField("product_category_name_english", StringType(), False),
])

# 9. Geolocation
GEOLOCATION_SCHEMA = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), False),  # Leading zero preservation
    StructField("geolocation_lat", DoubleType(), False),
    StructField("geolocation_lng", DoubleType(), False),
    StructField("geolocation_city", StringType(), False),
    StructField("geolocation_state", StringType(), False),
])

# Unified Registry mapping dataset names to explicit schemas
SCHEMAS = {
    "olist_orders_dataset": ORDERS_SCHEMA,
    "olist_order_items_dataset": ORDER_ITEMS_SCHEMA,
    "olist_order_reviews_dataset": ORDER_REVIEWS_SCHEMA,
    "olist_order_payments_dataset": ORDER_PAYMENTS_SCHEMA,
    "olist_customers_dataset": CUSTOMERS_SCHEMA,
    "olist_sellers_dataset": SELLERS_SCHEMA,
    "olist_products_dataset": PRODUCTS_SCHEMA,
    "product_category_name_translation": CATEGORY_TRANSLATION_SCHEMA,
    "olist_geolocation_dataset": GEOLOCATION_SCHEMA,
}
