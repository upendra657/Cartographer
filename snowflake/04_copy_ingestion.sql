USE ROLE LOADER;
USE SCHEMA CARTOGRAPHER_DB.RAW;

-- 1. Orders
CREATE OR REPLACE TABLE RAW_ORDERS (
    order_id                        VARCHAR NOT NULL,
    customer_id                     VARCHAR NOT NULL,
    order_status                    VARCHAR NOT NULL,
    order_purchase_timestamp        TIMESTAMP_NTZ NOT NULL,
    order_approved_at               TIMESTAMP_NTZ,
    order_delivered_carrier_date    TIMESTAMP_NTZ,
    order_delivered_customer_date   TIMESTAMP_NTZ,
    order_estimated_delivery_date   TIMESTAMP_NTZ NOT NULL,
    _batch_id                       VARCHAR NOT NULL,
    _as_of_date                     DATE NOT NULL,
    _extracted_at                   TIMESTAMP_NTZ NOT NULL,
    _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2. Order Items
CREATE OR REPLACE TABLE RAW_ORDER_ITEMS (
    order_id                        VARCHAR NOT NULL,
    order_item_id                   INTEGER NOT NULL,
    product_id                      VARCHAR NOT NULL,
    seller_id                       VARCHAR NOT NULL,
    shipping_limit_date             TIMESTAMP_NTZ NOT NULL,
    price                           NUMBER(12, 2) NOT NULL,
    freight_value                   NUMBER(12, 2) NOT NULL,
    _batch_id                       VARCHAR NOT NULL,
    _as_of_date                     DATE NOT NULL,
    _extracted_at                   TIMESTAMP_NTZ NOT NULL,
    _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Order Reviews
CREATE OR REPLACE TABLE RAW_ORDER_REVIEWS (
    review_id                       VARCHAR NOT NULL,
    order_id                        VARCHAR NOT NULL,
    review_score                    INTEGER NOT NULL,
    review_comment_title            VARCHAR,
    review_comment_message          VARCHAR,
    review_creation_date            TIMESTAMP_NTZ NOT NULL,
    review_answer_timestamp         TIMESTAMP_NTZ NOT NULL,
    _batch_id                       VARCHAR NOT NULL,
    _as_of_date                     DATE NOT NULL,
    _extracted_at                   TIMESTAMP_NTZ NOT NULL,
    _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Order Payments
CREATE OR REPLACE TABLE RAW_ORDER_PAYMENTS (
    order_id                        VARCHAR NOT NULL,
    payment_sequential              INTEGER NOT NULL,
    payment_type                    VARCHAR NOT NULL,
    payment_installments            INTEGER NOT NULL,
    payment_value                   NUMBER(12, 2) NOT NULL,
    _batch_id                       VARCHAR NOT NULL,
    _as_of_date                     DATE NOT NULL,
    _extracted_at                   TIMESTAMP_NTZ NOT NULL,
    _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 5. Customers
CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    customer_id                     VARCHAR NOT NULL,
    customer_unique_id              VARCHAR NOT NULL,
    customer_zip_code_prefix        VARCHAR NOT NULL,  
    customer_city                   VARCHAR NOT NULL,
    customer_state                  VARCHAR NOT NULL,
    _batch_id                       VARCHAR NOT NULL,
    _as_of_date                     DATE NOT NULL,
    _extracted_at                   TIMESTAMP_NTZ NOT NULL,
    _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 6. Sellers
CREATE OR REPLACE TABLE RAW_SELLERS (
    seller_id                       VARCHAR NOT NULL,
    seller_zip_code_prefix          VARCHAR NOT NULL,  
    seller_city                     VARCHAR NOT NULL,
    seller_state                    VARCHAR NOT NULL,
    _batch_id                       VARCHAR NOT NULL,
    _as_of_date                     DATE NOT NULL,
    _extracted_at                   TIMESTAMP_NTZ NOT NULL,
    _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 7. Products 
CREATE OR REPLACE TABLE RAW_PRODUCTS (
    product_id                      VARCHAR NOT NULL,
    product_category_name           VARCHAR,
    product_name_lenght             INTEGER,           
    product_description_lenght      INTEGER,           
    product_photos_qty              INTEGER,
    product_weight_g                INTEGER,
    product_length_cm               INTEGER,
    product_height_cm               INTEGER,
    product_width_cm                INTEGER,
    _batch_id                       VARCHAR NOT NULL,
    _as_of_date                     DATE NOT NULL,
    _extracted_at                   TIMESTAMP_NTZ NOT NULL,
    _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 8. Product Category Translations
CREATE OR REPLACE TABLE RAW_PRODUCT_CATEGORY_NAME_TRANSLATION (
    product_category_name           VARCHAR NOT NULL,
    product_category_name_english   VARCHAR NOT NULL,
    _batch_id                       VARCHAR NOT NULL,
    _as_of_date                     DATE NOT NULL,
    _extracted_at                   TIMESTAMP_NTZ NOT NULL,
    _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 9. Geolocation
CREATE OR REPLACE TABLE RAW_GEOLOCATION (
    geolocation_zip_code_prefix     VARCHAR NOT NULL,  
    geolocation_lat                 FLOAT NOT NULL,
    geolocation_lng                 FLOAT NOT NULL,
    geolocation_city                VARCHAR NOT NULL,
    geolocation_state               VARCHAR NOT NULL,
    _batch_id                       VARCHAR NOT NULL,
    _as_of_date                     DATE NOT NULL,
    _extracted_at                   TIMESTAMP_NTZ NOT NULL,
    _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


USE ROLE LOADER;
USE WAREHOUSE CARTOGRAPHER_WH;
USE SCHEMA CARTOGRAPHER_DB.RAW;

-- 1. Orders
COPY INTO RAW_ORDERS
FROM @OLIST_INTERNAL_STAGE/olist_orders_dataset/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- 2. Order Items
COPY INTO RAW_ORDER_ITEMS
FROM @OLIST_INTERNAL_STAGE/olist_order_items_dataset/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- 3. Order Reviews
COPY INTO RAW_ORDER_REVIEWS
FROM @OLIST_INTERNAL_STAGE/olist_order_reviews_dataset/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- 4. Order Payments
COPY INTO RAW_ORDER_PAYMENTS
FROM @OLIST_INTERNAL_STAGE/olist_order_payments_dataset/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- 5. Customers
COPY INTO RAW_CUSTOMERS
FROM @OLIST_INTERNAL_STAGE/olist_customers_dataset/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- 6. Sellers
COPY INTO RAW_SELLERS
FROM @OLIST_INTERNAL_STAGE/olist_sellers_dataset/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- 7. Products
COPY INTO RAW_PRODUCTS
FROM @OLIST_INTERNAL_STAGE/olist_products_dataset/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- 8. Product Category Translations
COPY INTO RAW_PRODUCT_CATEGORY_NAME_TRANSLATION
FROM @OLIST_INTERNAL_STAGE/product_category_name_translation/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- 9. Geolocation
COPY INTO RAW_GEOLOCATION
FROM @OLIST_INTERNAL_STAGE/olist_geolocation_dataset/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;
