-- ============================================================================
-- CARTOGRAPHER: Parquet Storage Stage & File Format (Step 1.14, ADR-004)
-- File Format: Snappy-compressed Parquet
-- Stage: Internal named stage for landing PySpark replay parquet batches
-- ============================================================================

USE ROLE LOADER;
USE SCHEMA CARTOGRAPHER_DB.RAW;

-- 1. Create Parquet File Format
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY
    COMMENT = 'Snappy-compressed Parquet file format for Cartographer source ingestion';

-- 2. Create Internal Named Stage
CREATE OR REPLACE STAGE OLIST_INTERNAL_STAGE
    FILE_FORMAT = PARQUET_FORMAT
    COMMENT = 'Internal named stage holding partitioned Parquet files by entity and batch';

-- 3. Reference PUT Commands (Orchestrated via SnowSQL / Python Connector)
-- Example: PUT file://data/parquet/olist_orders_dataset/* @OLIST_INTERNAL_STAGE/olist_orders_dataset/ AUTO_COMPRESS=FALSE;
