"""
Cartographer Cost Governance & Warehouse Guardrail Audit
Step 4.4: Asserts warehouse auto-suspend, resource monitor quota, and total credit consumption.
"""

import os
import sys
import snowflake.connector
from dotenv import load_dotenv

def audit_cost_governance():
    load_dotenv()
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")

    if not all([user, password, account]):
        print("[-] Missing Snowflake credentials in .env")
        sys.exit(1)

    print("=" * 70)
    print("CARTOGRAPHER COST GOVERNANCE & FINOPS AUDIT (STEP 4.4)")
    print("=" * 70)

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        role="ACCOUNTADMIN"
    )
    cur = conn.cursor()

    try:
        # 1. Audit Warehouse Configuration & Auto-Suspend
        cur.execute("SHOW WAREHOUSES LIKE 'CARTOGRAPHER_WH';")
        wh_rows = cur.fetchall()
        if not wh_rows:
            print("[-] FATAL: CARTOGRAPHER_WH does not exist.")
            sys.exit(1)

        wh_cols = [col[0].lower() for col in cur.description]
        wh_data = dict(zip(wh_cols, wh_rows[0]))

        wh_size = wh_data.get("size")
        auto_suspend = int(wh_data.get("auto_suspend", 0))
        auto_resume = str(wh_data.get("auto_resume")).lower()
        res_monitor = wh_data.get("resource_monitor")

        print(f"[+] Warehouse Name:        {wh_data.get('name')}")
        print(f"[+] Warehouse Size:        {wh_size}")
        print(f"[+] Auto-Suspend Interval: {auto_suspend} seconds (Target: 60s)")
        print(f"[+] Auto-Resume Enabled:   {auto_resume}")
        print(f"[+] Attached Resource Mon: {res_monitor}")

        assert auto_suspend == 60, f"Auto-suspend ({auto_suspend}s) is not set to 60s!"
        assert wh_size.lower() == "x-small", f"Warehouse size ({wh_size}) is not X-Small!"

        # 2. Audit Resource Monitor
        cur.execute("SHOW RESOURCE MONITORS LIKE 'CARTOGRAPHER_RM';")
        rm_rows = cur.fetchall()
        if not rm_rows:
            print("[-] FATAL: CARTOGRAPHER_RM not found.")
            sys.exit(1)

        rm_cols = [col[0].lower() for col in cur.description]
        rm_data = dict(zip(rm_cols, rm_rows[0]))

        credit_quota = float(rm_data.get("credit_quota", 0.0))
        frequency = rm_data.get("frequency")
        notify_triggers = rm_data.get("notify_at")
        suspend_trigger = rm_data.get("suspend_at")

        print("-" * 70)
        print(f"[+] Resource Monitor:      {rm_data.get('name')}")
        print(f"[+] Credit Quota:          {credit_quota} credits / month (Target: 10.0)")
        print(f"[+] Monitor Frequency:     {frequency}")
        print(f"[+] Notification Triggers: {notify_triggers}")
        print(f"[+] Hard Suspend Trigger:  {suspend_trigger}")

        assert credit_quota == 10.0, f"Credit quota ({credit_quota}) is not 10.0!"
        assert suspend_trigger == "100%", f"Suspend trigger ({suspend_trigger}) is not 100%!"

        # 3. Audit Metering History & Actual Credit Consumption
        cur.execute("""
            SELECT 
                WAREHOUSE_NAME,
                COALESCE(SUM(CREDITS_USED), 0) AS TOTAL_CREDITS,
                COALESCE(MIN(START_TIME), CURRENT_TIMESTAMP()) AS FIRST_START,
                COALESCE(MAX(END_TIME), CURRENT_TIMESTAMP()) AS LAST_END
            FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
                DATE_RANGE_START => DATEADD('day', -14, CURRENT_DATE()),
                WAREHOUSE_NAME => 'CARTOGRAPHER_WH'
            ))
            GROUP BY WAREHOUSE_NAME;
        """)
        metering = cur.fetchall()
        if metering:
            _, total_credits, first_start, last_end = metering[0]
            total_credits = float(total_credits)
        else:
            total_credits = 0.0
            first_start, last_end = "N/A", "N/A"

        print("-" * 70)
        print(f"[+] Total Credits Incurred: {total_credits:.4f} credits (Quota: 10.0 credits)")
        print(f"[+] Usage Window Start:     {first_start}")
        print(f"[+] Usage Window End:       {last_end}")
        percent_used = (total_credits / credit_quota) * 100.0
        print(f"[+] FinOps Budget Used:     {percent_used:.2f}% of monthly quota")

        assert total_credits < credit_quota, f"Total credits ({total_credits}) exceeded quota ({credit_quota})!"

        print("=" * 70)
        print("[+] STEP 4.4 VERIFICATION PASSED: All FinOps Guardrails & Quotas Active.")
        print("=" * 70)

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    audit_cost_governance()
