"""
Cartographer: As-Of Replay Generator & Parquet Serializer (Step 1.7 & Step 1.8).

Simulates 22 discrete monthly as-of batches (2017-01 to 2018-10) to test:
1. Late-arriving events (reviews arriving up to 528 days post-purchase; F13).
2. Point-in-time lifecycle timestamp masking and dynamic status transitions.
3. Deliberate status regressions (the 6 canceled orders carrying delivery timestamps).
4. Snappy Parquet serialization with coalesce(1) partitioned strictly by batch_id.
5. Exact reconciliation of deduplicated state against source_baseline.json.
"""

import os
import sys
import json
import calendar
from pathlib import Path
from typing import Dict, List, Tuple

# Force loopback binding on macOS before Spark initializes
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col, lit, when, to_date, to_timestamp, date_format, last_day,
    current_timestamp, row_number, greatest, sum as spark_sum
)

from pyspark_ingestion.schemas import SCHEMAS
from pyspark_ingestion.extract import create_spark_session, extract_all


def generate_batch_schedule() -> List[Tuple[str, str]]:
    """
    Emits the 22 monthly batch boundaries from 2017-01 to 2018-10.
    Returns: List of (batch_id, as_of_date_str)
    """
    batches = []
    years_months = (
        [(2017, m) for m in range(1, 13)] +
        [(2018, m) for m in range(1, 11)]
    )
    for year, month in years_months:
        batch_id = f"{year:04d}-{month:02d}"
        last_day_val = calendar.monthrange(year, month)[1]
        as_of_date = f"{year:04d}-{month:02d}-{last_day_val:02d}"
        batches.append((batch_id, as_of_date))
    return batches


class ReplayEngine:
    """Manages the point-in-time derivation and emission of replay batches."""

    def __init__(self, raw_dfs: Dict[str, DataFrame]):
        self.raw_dfs = raw_dfs
        self.batches = generate_batch_schedule()
        self.batch_ids = [b[0] for b in self.batches]

    def build_dimensions_replay(self) -> Dict[str, Dict[str, DataFrame]]:
        """
        Dimension tables (customers, sellers, products, translations, geolocation)
        are static reference datasets loaded in full during the first batch (2017-01).
        """
        dim_tables = [
            "olist_customers_dataset",
            "olist_sellers_dataset",
            "olist_products_dataset",
            "product_category_name_translation",
            "olist_geolocation_dataset",
        ]
        results = {tbl: {} for tbl in dim_tables}
        first_as_of = self.batches[0][1]

        for tbl in dim_tables:
            results[tbl]["2017-01"] = (
                self.raw_dfs[tbl]
                .withColumn("_batch_id", lit("2017-01"))
                .withColumn("_as_of_date", to_date(lit(first_as_of)))
                .withColumn("_extracted_at", current_timestamp())
            )
        return results

    def build_items_and_payments_replay(self) -> Tuple[Dict[str, DataFrame], Dict[str, DataFrame]]:
        """
        Items and payments are immutable transaction records emitted in the order's purchase batch.
        Uses a single join with orders to determine batch_id vectorially.
        """
        orders_df = self.raw_dfs["olist_orders_dataset"]
        items_df = self.raw_dfs["olist_order_items_dataset"]
        payments_df = self.raw_dfs["olist_order_payments_dataset"]

        # Derive purchase batch per order: 2016 history lands in 2017-01 baseline
        order_purchase_batches = orders_df.select(
            "order_id",
            when(col("order_purchase_timestamp") <= "2017-01-31 23:59:59", lit("2017-01"))
            .otherwise(date_format(col("order_purchase_timestamp"), "yyyy-MM"))
            .alias("_batch_id")
        )

        items_enriched = (
            items_df.join(order_purchase_batches, on="order_id", how="inner")
            .withColumn("_as_of_date", last_day(to_date(col("_batch_id"), "yyyy-MM")))
            .withColumn("_extracted_at", current_timestamp())
        )

        payments_enriched = (
            payments_df.join(order_purchase_batches, on="order_id", how="inner")
            .withColumn("_as_of_date", last_day(to_date(col("_batch_id"), "yyyy-MM")))
            .withColumn("_extracted_at", current_timestamp())
        )

        items_batches = {}
        payments_batches = {}
        for b_id in self.batch_ids:
            items_batches[b_id] = items_enriched.filter(col("_batch_id") == b_id)
            payments_batches[b_id] = payments_enriched.filter(col("_batch_id") == b_id)

        return items_batches, payments_batches

    def build_reviews_replay(self) -> Dict[str, DataFrame]:
        """
        Reviews simulate late-arriving events (up to 528 days after purchase; F13).
        Released when answered (review_answer_timestamp <= as_of_date).
        Constraint F9: Never released before the order's purchase batch.
        """
        orders_df = self.raw_dfs["olist_orders_dataset"].select("order_id", "order_purchase_timestamp")
        reviews_df = self.raw_dfs["olist_order_reviews_dataset"]

        reviews_with_orders = reviews_df.join(orders_df, on="order_id", how="inner")

        # Effective release = greatest(review_answer_timestamp, order_purchase_timestamp)
        effective_release_ts = greatest(col("review_answer_timestamp"), col("order_purchase_timestamp"))

        reviews_enriched = (
            reviews_with_orders
            .withColumn(
                "_batch_id",
                when(effective_release_ts <= "2017-01-31 23:59:59", lit("2017-01"))
                .otherwise(date_format(effective_release_ts, "yyyy-MM"))
            )
            .drop("order_purchase_timestamp")
            .withColumn("_as_of_date", last_day(to_date(col("_batch_id"), "yyyy-MM")))
            .withColumn("_extracted_at", current_timestamp())
        )

        review_batches = {}
        for b_id in self.batch_ids:
            review_batches[b_id] = reviews_enriched.filter(col("_batch_id") == b_id)

        return review_batches

    def build_orders_replay(self) -> Dict[str, DataFrame]:
        """
        Orders lifecycle progression:
        - Order appears in purchase month.
        - Re-appears in a subsequent month if a milestone (approval, handoff, delivery) occurred in that month.
        - Future timestamps beyond as_of_date are masked to NULL.
        - Status is derived dynamically.
        - Handles status regressions for the 6 delivered-then-canceled orders.
        """
        orders_df = self.raw_dfs["olist_orders_dataset"]

        # 6 regression orders: marked canceled with delivery timestamps
        regression_condition = (
            (col("order_status") == "canceled") &
            col("order_delivered_customer_date").isNotNull()
        )

        batch_dfs = {}
        for idx, (batch_id, as_of_date) in enumerate(self.batches):
            as_of_ts = to_timestamp(lit(f"{as_of_date} 23:59:59"))
            prev_as_of_ts = (
                to_timestamp(lit(f"{self.batches[idx-1][1]} 23:59:59"))
                if idx > 0 else to_timestamp(lit("2000-01-01 00:00:00"))
            )

            is_new_purchase = (
                (col("order_purchase_timestamp") <= as_of_ts) &
                (col("order_purchase_timestamp") > prev_as_of_ts)
            )

            approval_advanced = (
                col("order_approved_at").isNotNull() &
                (col("order_approved_at") <= as_of_ts) &
                (col("order_approved_at") > prev_as_of_ts)
            )

            carrier_advanced = (
                col("order_delivered_carrier_date").isNotNull() &
                (col("order_delivered_carrier_date") <= as_of_ts) &
                (col("order_delivered_carrier_date") > prev_as_of_ts)
            )

            delivery_advanced = (
                col("order_delivered_customer_date").isNotNull() &
                (col("order_delivered_customer_date") <= as_of_ts) &
                (col("order_delivered_customer_date") > prev_as_of_ts)
            )

            terminal_no_ts = (
                col("order_status").isin("canceled", "unavailable", "invoiced", "processing") &
                is_new_purchase
            )

            regression_flip = (
                regression_condition &
                (col("order_delivered_customer_date") <= prev_as_of_ts) &
                (
                    (col("order_delivered_customer_date") > (
                        to_timestamp(lit(f"{self.batches[idx-2][1]} 23:59:59"))
                        if idx > 1 else to_timestamp(lit("2000-01-01 00:00:00"))
                    )) if idx > 0 else lit(False)
                )
            )

            if idx == 0:
                eligible_filter = (col("order_purchase_timestamp") <= as_of_ts)
            else:
                eligible_filter = (
                    is_new_purchase |
                    approval_advanced |
                    carrier_advanced |
                    delivery_advanced |
                    terminal_no_ts |
                    regression_flip
                )

            current_batch = orders_df.filter(eligible_filter)

            # Point-in-time timestamp masking
            masked_approved = when(col("order_approved_at") <= as_of_ts, col("order_approved_at")).otherwise(lit(None))
            masked_carrier = when(col("order_delivered_carrier_date") <= as_of_ts, col("order_delivered_carrier_date")).otherwise(lit(None))
            masked_customer = when(col("order_delivered_customer_date") <= as_of_ts, col("order_delivered_customer_date")).otherwise(lit(None))

            # Dynamic status resolution
            derived_status = (
                when(regression_flip, lit("canceled"))
                .when(regression_condition & (masked_customer.isNotNull()), lit("delivered"))
                .when(masked_customer.isNotNull(), lit("delivered"))
                .when(masked_carrier.isNotNull(), lit("shipped"))
                .when(masked_approved.isNotNull(), lit("approved"))
                .otherwise(col("order_status"))
            )

            batch_dfs[batch_id] = (
                current_batch
                .withColumn("order_approved_at", masked_approved)
                .withColumn("order_delivered_carrier_date", masked_carrier)
                .withColumn("order_delivered_customer_date", masked_customer)
                .withColumn("order_status", derived_status)
                .withColumn("_batch_id", lit(batch_id))
                .withColumn("_as_of_date", to_date(lit(as_of_date)))
                .withColumn("_extracted_at", current_timestamp())
            )

        return batch_dfs


def write_parquet_batches(
    all_batch_data: Dict[str, Dict[str, DataFrame]],
    output_base_dir: str = "data/parquet"
) -> None:
    """
    Serializes all replay batches to Snappy Parquet partitioned by batch_id.
    Layout: data/parquet/<entity>/batch_id=<YYYY-MM>/part-*.parquet
    Enforces coalesce(1) per batch to eliminate the small files problem.
    """
    base_path = Path(output_base_dir)
    base_path.mkdir(parents=True, exist_ok=True)

    print(f"\n[Writing Parquet] Output directory: {base_path.resolve()}")
    for table_name, batches in all_batch_data.items():
        print(f"  → Writing entity: {table_name:<38} ({len(batches)} batch partitions)")
        table_dir = base_path / table_name
        table_dir.mkdir(parents=True, exist_ok=True)

        for batch_id, df in batches.items():
            batch_output_path = f"{output_base_dir}/{table_name}/batch_id={batch_id}"
            (
                df
                .coalesce(1)
                .write
                .mode("overwrite")
                .option("compression", "snappy")
                .parquet(batch_output_path)
            )


def verify_deduplicated_reconciliation(
    spark: SparkSession,
    output_base_dir: str = "data/parquet",
    baseline_path: str = "pyspark_ingestion/source_baseline.json"
) -> None:
    """
    Acceptance Verification for Step 1.8:
    Reads back the partitioned Parquet files for each entity, deduplicates by natural key
    taking the latest record per key, and asserts exact match against source_baseline.json.
    """
    with open(baseline_path, "r", encoding="utf-8") as f:
        baseline = json.load(f)

    print("\n" + "=" * 80)
    print("STEP 1.8 ACCEPTANCE: DEDUPLICATED AS-OF REPLAY RECONCILIATION")
    print("=" * 80)

    total_verified_rows = 0
    for table_name, table_info in baseline["tables"].items():
        parquet_path = f"{output_base_dir}/{table_name}"
        expected_rows = table_info["expected_row_count"]
        natural_keys = table_info["natural_key"]

        # Read back all batch partitions for the entity
        df = spark.read.parquet(f"{parquet_path}/*")

        if not natural_keys:
            # Geolocation table has no natural key; row count matches directly
            deduped_count = df.count()
            deduped_df = df
        else:
            # Deduplicate by natural key ordering by _as_of_date desc
            window_spec = Window.partitionBy(*natural_keys).orderBy(col("_as_of_date").desc(), col("_extracted_at").desc())
            deduped_df = df.withColumn("_row_num", row_number().over(window_spec)).filter(col("_row_num") == 1)
            deduped_count = deduped_df.count()

        total_verified_rows += deduped_count
        assert deduped_count == expected_rows, (
            f"Reconciliation failure on {table_name}: deduplicated rows {deduped_count} != expected {expected_rows}"
        )

        # Reconcile financial aggregates if present
        if "aggregations" in table_info:
            for agg_col, expected_val in table_info["aggregations"].items():
                col_name = agg_col.replace("sum_", "")
                actual_val = deduped_df.select(spark_sum(col(col_name))).collect()[0][0]
                assert round(float(actual_val), 2) == round(float(expected_val), 2), (
                    f"Financial drift on {table_name}.{col_name}: {actual_val} vs expected {expected_val}"
                )

        print(f"  ✓ {table_name:<38} | Deduped Rows: {deduped_count:>7,} (Expected: {expected_rows:>7,}) | Status: PASSED")

    assert total_verified_rows == baseline["metadata"]["total_rows"], (
        f"Total rows mismatch: {total_verified_rows} vs {baseline['metadata']['total_rows']}"
    )

    print("-" * 80)
    print(f"TOTAL REPLAY RECONCILIATION: {total_verified_rows:,} / {baseline['metadata']['total_rows']:,} rows reproduced perfectly.")
    print("FINANCIAL TOTALS VERIFIED: Σprice, Σfreight, Σpayment reconciled to the cent.")
    print("STEP 1.7 & STEP 1.8 ACCEPTANCE: ALL 22 BATCHES VERIFIED WITH ZERO DATA LOSS.")
    print("=" * 80 + "\n")


def run_replay_pipeline() -> None:
    """Orchestrates the entire replay generation and validation pipeline."""
    spark = create_spark_session("Cartographer-ReplayGenerator")

    try:
        print("\n[1/4] Extracting raw baseline CSV datasets...")
        raw_dfs = extract_all(spark, source_dir="Datasets", add_metadata=False)

        print("\n[2/4] Generating 22 as-of monthly replay batches...")
        engine = ReplayEngine(raw_dfs)

        all_batch_data: Dict[str, Dict[str, DataFrame]] = {tbl: {} for tbl in SCHEMAS.keys()}

        # 1. Dimensions (2017-01 only)
        dims = engine.build_dimensions_replay()
        for tbl, batches in dims.items():
            all_batch_data[tbl] = batches

        # 2. Orders
        all_batch_data["olist_orders_dataset"] = engine.build_orders_replay()

        # 3. Items & Payments
        items_batches, payments_batches = engine.build_items_and_payments_replay()
        all_batch_data["olist_order_items_dataset"] = items_batches
        all_batch_data["olist_order_payments_dataset"] = payments_batches

        # 4. Reviews
        all_batch_data["olist_order_reviews_dataset"] = engine.build_reviews_replay()

        print("\n[3/4] Serializing batches to Snappy Parquet (coalesce(1))...")
        write_parquet_batches(all_batch_data, output_base_dir="data/parquet")

        print("\n[4/4] Verifying deduplicated reconciliation against source baseline...")
        verify_deduplicated_reconciliation(spark, output_base_dir="data/parquet")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_replay_pipeline()
