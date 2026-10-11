#!/usr/bin/env python3
"""
CARTOGRAPHER: Parquet Stage Ingestion Engine (Step 1.14, Option B)
Uploads partitioned Snappy Parquet replay batches from local filesystem
to Snowflake internal named stage @OLIST_INTERNAL_STAGE using LOADER role.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables from .env in project root
project_root = Path(__file__).resolve().parent.parent
dotenv_path = project_root / ".env"
load_dotenv(dotenv_path=dotenv_path)


def get_snowflake_connection():
    """Validates configuration and opens connection to Snowflake."""
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER", "UPENDRA657")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    role = os.getenv("SNOWFLAKE_ROLE", "LOADER")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "CARTOGRAPHER_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "CARTOGRAPHER_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

    # Guardrails: Validate non-empty / non-placeholder credentials
    placeholders = ["your_account_identifier", "your_password", "", None]
    if account in placeholders or password in placeholders:
        print("[ERROR] Snowflake credentials are not configured in .env.")
        print("Please edit .env and provide valid SNOWFLAKE_ACCOUNT and SNOWFLAKE_PASSWORD.")
        sys.exit(1)

    print(f"Connecting to Snowflake account [{account}] as user [{user}] with role [{role}]...")
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )
    return conn


def upload_parquet_files():
    """Constructs absolute paths and executes PUT command to stage Parquet files."""
    absolute_path_to_data = (project_root / "data" / "parquet").resolve()

    if not absolute_path_to_data.exists():
        print(f"[ERROR] Parquet directory not found: {absolute_path_to_data}")
        sys.exit(1)

    entities = [
        d for d in sorted(os.listdir(absolute_path_to_data))
        if (absolute_path_to_data / d).is_dir() and not d.startswith(".")
    ]

    if not entities:
        print(f"[ERROR] No entity directories found in {absolute_path_to_data}")
        sys.exit(1)

    total_files = len(list(absolute_path_to_data.glob("*/*/*.parquet")))
    print(f"Found {len(entities)} entities with {total_files} total Parquet files in {absolute_path_to_data}")

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        # Verify active context
        cursor.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        curr_role, curr_wh, curr_db, curr_schema = cursor.fetchone()
        print(f"Active Session: Role={curr_role}, WH={curr_wh}, DB={curr_db}, Schema={curr_schema}")

        uploaded_count = 0
        for entity in entities:
            entity_dir = absolute_path_to_data / entity
            entity_files = list(entity_dir.glob("*/*.parquet"))
            print(f"\nStaging entity [{entity}] ({len(entity_files)} files)...")

            # Execute PUT command logic for entity parquet files
            # Target path @OLIST_INTERNAL_STAGE/{entity}/ preserves clean partition routing
            put_sql = (
                f"PUT file://{entity_dir}/*/*.parquet "
                f"@OLIST_INTERNAL_STAGE/{entity}/ "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            print(f"Executing: {put_sql}")
            cursor.execute(put_sql)

            results = cursor.fetchall()
            uploaded_count += len(results)
            print(f"-> Successfully staged {len(results)} files for {entity}")

        print(f"\n[SUCCESS] Completed staging {uploaded_count} files to @OLIST_INTERNAL_STAGE.")

        # Stage listing audit
        cursor.execute("LIST @OLIST_INTERNAL_STAGE")
        stage_files = cursor.fetchall()
        print(f"Stage Verification: {len(stage_files)} files currently in @OLIST_INTERNAL_STAGE")

    finally:
        cursor.close()
        conn.close()
        print("Snowflake connection closed.")


if __name__ == "__main__":
    upload_parquet_files()
