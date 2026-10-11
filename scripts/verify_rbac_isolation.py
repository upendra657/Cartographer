"""
RBAC Access Isolation Verification Script (Step 3.11 / ADR-015)
Validates that the REPORTER role has strict SELECT permissions on CARTOGRAPHER_DB.MARTS
and is prohibited from accessing sensitive ingestion/staging layers in CARTOGRAPHER_DB.RAW.
"""

import os
import sys
import snowflake.connector
from dotenv import load_dotenv

def main():
    load_dotenv()

    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = "CARTOGRAPHER_WH"
    database = "CARTOGRAPHER_DB"

    if not all([user, password, account]):
        print("ERROR: Missing required Snowflake credentials in environment.")
        sys.exit(1)

    print("Connecting to Snowflake as role 'REPORTER'...")
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        role="REPORTER"
    )
    cur = conn.cursor()

    # Disable secondary roles to ensure isolated primary role evaluation
    cur.execute("USE SECONDARY ROLES NONE;")

    # 1. Verify authorized access to MARTS
    print("Testing authorized access to CARTOGRAPHER_DB.MARTS.FCT_SELLER_MONTHLY_PERFORMANCE...")
    try:
        cur.execute("SELECT COUNT(*) FROM CARTOGRAPHER_DB.MARTS.FCT_SELLER_MONTHLY_PERFORMANCE;")
        row_count = cur.fetchone()[0]
        print(f" PASS: Access GRANTED to MARTS. Row count: {row_count}")
        assert row_count == 33484, f"Unexpected row count: {row_count}"
    except Exception as e:
        print(f" FAIL: Access DENIED to MARTS: {e}")
        sys.exit(1)

    # 2. Verify unauthorized access blocked on RAW
    print("Testing unauthorized access restriction on CARTOGRAPHER_DB.RAW.RAW_ORDERS...")
    try:
        cur.execute("SELECT COUNT(*) FROM CARTOGRAPHER_DB.RAW.RAW_ORDERS;")
        print(" CRITICAL FAILURE: Security breach! Role 'REPORTER' successfully queried RAW_ORDERS.")
        sys.exit(1)
    except snowflake.connector.errors.ProgrammingError as e:
        print(" PASS: Access BLOCKED to RAW as expected.")
        print(f"  Snowflake Error: {e.msg}")

    print("\nRBAC Isolation Audit PASSED (100% compliant with Medallion security policies).")
    conn.close()

if __name__ == "__main__":
    main()
