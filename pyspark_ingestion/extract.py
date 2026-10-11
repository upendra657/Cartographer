"""
Cartographer: Source Extraction Engine (Step 1.5 & Step 1.6).

Extracts all 9 raw Olist CSV datasets into typed PySpark DataFrames enforcing:
1. Explicit StructType schemas from schemas.py (banning inferSchema=True; F3).
2. Fail-Stop parse mode (mode='FAILFAST'; ADR-002).
3. UniVocity multi-line record buffering for review comments with CRLF breaks (F1).
4. Windows UTF-8 Byte Order Mark (BOM) sanitization on translations (F2).
5. RFC 4180 double-quote escaping (escape='"', quote='"').
6. Explicit ISO timestamp parsing (timestampFormat='yyyy-MM-dd HH:mm:ss').
7. Raw Fidelity preservation of source column names and natural typos.
8. Injection of technical audit metadata columns (_batch_id, _as_of_date, _extracted_at).
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, Optional

# Force Spark driver and block manager to use loopback interface on macOS
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Add project root to sys.path to ensure module imports succeed when run as a direct script
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, to_date, current_timestamp

from pyspark_ingestion.schemas import SCHEMAS


def create_spark_session(app_name: str = "Cartographer-Extractor") -> SparkSession:
    """
    Creates a local single-node SparkSession configured for deterministic execution.

    - master('local[1]'): Single worker thread to avoid concurrency overhead on dev laptops.
    - spark.driver.bindAddress='127.0.0.1': Loopback binding to prevent macOS DNS resolution latency.
    - spark.driver.host='127.0.0.1': Forces Netty BlockTransferService to bind loopback, preventing connection refused on WiFi IPs.
    - spark.ui.enabled='false': Disables embedded Jetty UI to avoid port collisions (4040).
    - spark.sql.session.timeZone='UTC': Deterministic UTC timestamp evaluation.
    """
    return (
        SparkSession.builder
        .master("local[1]")
        .appName(app_name)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def extract_csv(
    spark: SparkSession,
    table_name: str,
    file_path: str
) -> DataFrame:
    """
    Reads a raw Olist CSV into a typed DataFrame using its registered schema.

    Enforces:
    - FAILFAST mode (ADR-002): fails immediately on malformed records
    - multiLine=True: handles 3,852 review comments with embedded CRLF breaks (F1)
    - escape='"': correctly parses escaped double quotes inside text fields
    - UTF-8 encoding: strips Windows BOM on category translations (F2)
    """
    if table_name not in SCHEMAS:
        raise ValueError(f"No registered schema found for table: {table_name}")

    schema = SCHEMAS[table_name]

    # Universal robust reader options
    reader = (
        spark.read
        .format("csv")
        .schema(schema)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .option("escape", "\"")
        .option("quote", "\"")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    )

    # Enable multi-line parsing specifically for tables containing freeform user text
    if table_name == "olist_order_reviews_dataset":
        reader = reader.option("multiLine", "true")

    df = reader.load(file_path)

    # Clean BOM artifact on header if present (Finding F2)
    first_col = df.columns[0]
    if first_col.startswith("\ufeff"):
        clean_first_col = first_col.replace("\ufeff", "")
        df = df.withColumnRenamed(first_col, clean_first_col)

    return df


def add_technical_metadata(
    df: DataFrame,
    batch_id: str = "2018-10",
    as_of_date: str = "2018-10-31"
) -> DataFrame:
    """
    Appends audit and replay metadata columns per Step 1.6:
    - _batch_id: Identifies the monthly replay batch (e.g., '2017-01' ... '2018-10')
    - _as_of_date: Logical point-in-time boundary for the batch
    - _extracted_at: UTC timestamp when the Spark extraction job executed
    """
    return (
        df
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_as_of_date", to_date(lit(as_of_date)))
        .withColumn("_extracted_at", current_timestamp())
    )


def extract_all(
    spark: SparkSession,
    source_dir: str = "Datasets",
    add_metadata: bool = False,
    batch_id: str = "2018-10",
    as_of_date: str = "2018-10-31"
) -> Dict[str, DataFrame]:
    """
    Extracts all 9 source files into memory as typed Spark DataFrames.
    Optionally enriches them with technical audit metadata columns.
    """
    dataframes = {}
    for table_name in SCHEMAS.keys():
        file_path = f"{source_dir}/{table_name}.csv"
        df = extract_csv(spark, table_name, file_path)
        if add_metadata:
            df = add_technical_metadata(df, batch_id=batch_id, as_of_date=as_of_date)
        dataframes[table_name] = df
    return dataframes


def run_extraction_audit() -> None:
    """
    Executes an end-to-end extraction audit asserting acceptance criteria for Step 1.5 & 1.6:
    1. Zero FAILFAST exceptions across all 9 raw files.
    2. olist_order_reviews_dataset resolves to exactly 99,224 rows (F1).
    3. Multi-line review comments with CRLF/LF line breaks count exactly 3,852 (F1).
    4. product_category_name_translation column 0 is clean (no BOM; F2).
    5. Raw fidelity preserved (product_name_lenght typo intact; Step 1.6).
    6. Technical metadata columns (_batch_id, _as_of_date, _extracted_at) injected cleanly.
    7. Total row counts match source_baseline.json (1,550,922 rows).
    """
    print("=" * 80)
    print("CARTOGRAPHER: INGESTION ENGINE EXTRACTION AUDIT (Step 1.5 & Step 1.6)")
    print("=" * 80)

    # 1. Load baseline
    baseline_path = Path("pyspark_ingestion/source_baseline.json")
    if not baseline_path.exists():
        raise FileNotFoundError(f"Missing baseline file at {baseline_path}")

    with open(baseline_path, "r", encoding="utf-8") as f:
        baseline = json.load(f)

    # 2. Spin up deterministic local SparkSession
    print("\n[1/3] Initializing local single-node SparkSession...")
    spark = create_spark_session("Cartographer-Extract-Audit")

    try:
        # 3. Extract all datasets
        print("\n[2/3] Extracting 9 raw datasets with FAILFAST & explicit schemas...")
        dfs = extract_all(spark, source_dir="Datasets", add_metadata=True)

        print("\n[3/3] Validating Step 1.5 & Step 1.6 acceptance criteria:\n")
        total_rows_extracted = 0

        for table_name, expected_info in baseline["tables"].items():
            df = dfs[table_name]
            expected_count = expected_info["expected_row_count"]
            actual_count = df.count()
            total_rows_extracted += actual_count

            # Assert exact row count matches baseline
            assert actual_count == expected_count, (
                f"Row count mismatch for {table_name}: actual {actual_count} vs expected {expected_count}"
            )

            # Assert technical metadata columns exist
            for tech_col in ["_batch_id", "_as_of_date", "_extracted_at"]:
                assert tech_col in df.columns, f"Missing technical column {tech_col} in {table_name}"

            print(f"  ✓ {table_name:<38} | Rows: {actual_count:>7,} (Expected: {expected_count:>7,}) | Status: PASSED")

        # Specific Acceptance Check 1: Review multi-line resolution (Finding F1)
        reviews_df = dfs["olist_order_reviews_dataset"]
        assert reviews_df.count() == 99224, f"Reviews row count {reviews_df.count()} != 99,224"
        multiline_comments = reviews_df.filter(col("review_comment_message").rlike("[\r\n]")).count()
        assert multiline_comments == 3852, f"Multi-line comments {multiline_comments} != 3,852"
        print(f"\n  ✓ Finding F1 Verified: olist_order_reviews_dataset parsed to exactly 99,224 logical records")
        print(f"  ✓ Finding F1 Verified: Exactly {multiline_comments:,} review comments preserved internal CRLF line breaks")

        # Specific Acceptance Check 2: UTF-8 BOM sanitization (Finding F2)
        trans_df = dfs["product_category_name_translation"]
        assert trans_df.columns[0] == "product_category_name", (
            f"BOM not cleaned: first column is {trans_df.columns[0]}"
        )
        print(f"  ✓ Finding F2 Verified: product_category_name_translation column 0 is clean ('product_category_name')")

        # Specific Acceptance Check 3: Raw Fidelity rule (Step 1.6)
        products_df = dfs["olist_products_dataset"]
        assert "product_name_lenght" in products_df.columns, "Source typo product_name_lenght was unexpectedly altered"
        assert "product_description_lenght" in products_df.columns, "Source typo product_description_lenght was unexpectedly altered"
        print(f"  ✓ Raw Fidelity Verified: Upstream typos (product_name_lenght) preserved intact for dbt staging")

        # Total reconciliation
        assert total_rows_extracted == baseline["metadata"]["total_rows"], (
            f"Total row mismatch: {total_rows_extracted} vs {baseline['metadata']['total_rows']}"
        )
        print(f"\n" + "-" * 80)
        print(f"TOTAL RECONCILIATION: {total_rows_extracted:,} / {baseline['metadata']['total_rows']:,} rows extracted with zero drift.")
        print(f"STEP 1.5 & STEP 1.6 ACCEPTANCE: ALL CHECKS PASSED.")
        print("-" * 80 + "\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_extraction_audit()
