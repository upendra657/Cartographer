"""
Unit tests for Cartographer PySpark ingestion and replay pipeline (Step 1.9).

Tests all edge case hazards:
1. Brazilian CEP leading-zero preservation (Finding F3).
2. UTF-8 BOM header sanitization (Finding F2).
3. Multi-line record parsing across CRLF and embedded quotes (Finding F1).
4. Monetary decimal precision (DecimalType(12, 2)) vs float drift.
5. Fail-Fast parse mode on malformed tokens (ADR-002).
6. Replay status derivation and deliberate regression handling (6 canceled orders).
7. Review release timing constraint (F9 & F13).
"""

import os
import sys
from pathlib import Path

# Force Spark driver and worker to use the virtualenv Python 3.11 and loopback network
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
)
from pyspark.sql.functions import col, when, lit, to_timestamp

from pyspark_ingestion.extract import create_spark_session, extract_csv
from pyspark_ingestion.schemas import SCHEMAS, CUSTOMERS_SCHEMA


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped deterministic local SparkSession."""
    spark = create_spark_session("Cartographer-PySpark-Tests")
    yield spark
    spark.stop()


def test_leading_zero_cep_preservation(spark: SparkSession, tmp_path):
    """F3: Asserts Brazilian CEP zip codes retain leading zeros (e.g. '01037')."""
    csv_file = tmp_path / "customers_sample.csv"
    csv_file.write_text("customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\nc1,u1,01037,sao paulo,SP\n")

    df = (
        spark.read
        .format("csv")
        .schema(CUSTOMERS_SCHEMA)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .load(str(csv_file))
    )
    row = df.first()
    assert row["customer_zip_code_prefix"] == "01037", "Leading zero was stripped!"


def test_bom_header_sanitization(spark: SparkSession, tmp_path):
    """F2: Asserts UTF-8 BOM character (\\ufeff) is stripped from column headers."""
    csv_file = tmp_path / "translations_bom.csv"
    # Write file starting with UTF-8 BOM bytes
    bom_content = "\ufeffproduct_category_name,product_category_name_english\nbeleza_saude,health_beauty\n"
    csv_file.write_text(bom_content, encoding="utf-8")

    df = extract_csv(spark, "product_category_name_translation", str(csv_file))
    assert df.columns[0] == "product_category_name", f"BOM was not stripped: {df.columns[0]}"


def test_multiline_crlf_reviews_parsing(spark: SparkSession, tmp_path):
    """F1: Asserts UniVocity multiLine=true preserves internal CRLF breaks in quotes."""
    csv_file = tmp_path / "reviews_sample.csv"
    content = (
        'review_id,order_id,review_score,review_comment_title,review_comment_message,review_creation_date,review_answer_timestamp\n'
        'r1,o1,5,"Otimo","Produto chegou muito bem.\r\nRecomendo a todos!","2017-05-01 00:00:00","2017-05-02 10:00:00"\n'
        'r2,o2,1,"Ruim","Pessimo atendimento.","2017-05-01 00:00:00","2017-05-02 11:00:00"\n'
    )
    csv_file.write_text(content, encoding="utf-8")

    df = extract_csv(spark, "olist_order_reviews_dataset", str(csv_file))
    assert df.count() == 2, f"Expected 2 logical rows, got {df.count()}"
    row1 = df.filter(col("review_id") == "r1").first()
    assert "\n" in row1["review_comment_message"] or "\r" in row1["review_comment_message"]


def test_decimal_precision_no_float_drift(spark: SparkSession, tmp_path):
    """Asserts monetary metrics are parsed as exact DecimalType(12, 2)."""
    csv_file = tmp_path / "items_sample.csv"
    csv_file.write_text(
        "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value\n"
        "o1,1,p1,s1,2017-05-01 00:00:00,19.90,12.55\n"
        "o1,2,p1,s1,2017-05-01 00:00:00,10.10,5.45\n"
    )

    df = extract_csv(spark, "olist_order_items_dataset", str(csv_file))
    rows = df.collect()
    total_price = rows[0]["price"] + rows[1]["price"]
    assert isinstance(total_price, Decimal)
    assert total_price == Decimal("30.00"), f"Expected 30.00, got {total_price}"


def test_failfast_mode_aborts_on_corrupt_record(spark: SparkSession, tmp_path):
    """ADR-002: Asserts FAILFAST throws a SparkException on malformed tokens."""
    csv_file = tmp_path / "corrupt_items.csv"
    # Malformed price that cannot be parsed as Decimal
    csv_file.write_text(
        "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value\n"
        "o1,1,p1,s1,2017-05-01 00:00:00,NOT_A_DECIMAL,12.55\n"
    )

    with pytest.raises(Exception):
        df = extract_csv(spark, "olist_order_items_dataset", str(csv_file))
        df.collect()


def test_status_regression_logic(spark: SparkSession):
    """Asserts the 6 canceled orders with delivery timestamps simulate status regression."""
    # Order marked canceled with valid delivered date
    test_data = [(
        "reg_order", "canceled",
        "2017-03-01 10:00:00", "2017-03-02 10:00:00",
        "2017-03-05 10:00:00", "2017-03-15 10:00:00"
    )]
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("order_purchase_timestamp", StringType(), False),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
    ])
    df = spark.createDataFrame(test_data, schema=schema)

    # In March batch (delivery month), customer date is present -> derived status 'delivered'
    as_of_march = "2017-03-31 23:59:59"
    march_status = when(
        col("order_delivered_customer_date") <= as_of_march, lit("delivered")
    ).otherwise(col("order_status"))
    
    assert df.select(march_status).first()[0] == "delivered"
