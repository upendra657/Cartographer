import os
import time
import snowflake.connector
from dotenv import load_dotenv

def audit_reporting():
    load_dotenv()
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="CARTOGRAPHER_WH",
        database="CARTOGRAPHER_DB",
        schema="MARTS",
        role="REPORTER"
    )
    cur = conn.cursor()
    cur.execute("USE SECONDARY ROLES NONE;")

    # Model 1: rpt_seller_health_scorecard
    t0 = time.time()
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT seller_id), MIN(month), MAX(month), SUM(gmv) FROM CARTOGRAPHER_DB.MARTS.RPT_SELLER_HEALTH_SCORECARD")
    scorecard_stats = cur.fetchone()
    scorecard_duration = time.time() - t0

    cur.execute("DESCRIBE VIEW CARTOGRAPHER_DB.MARTS.RPT_SELLER_HEALTH_SCORECARD")
    scorecard_cols = [c[0] for c in cur.fetchall()]

    print("--- RPT_SELLER_HEALTH_SCORECARD AUDIT ---")
    print(f"Row Count: {scorecard_stats[0]}")
    print(f"Distinct Sellers: {scorecard_stats[1]}")
    print(f"Time Horizon: {scorecard_stats[2]} to {scorecard_stats[3]}")
    print(f"Total Realized GMV: R$ {float(scorecard_stats[4]):,.2f}")
    print(f"Projected Columns ({len(scorecard_cols)}): {scorecard_cols}")
    print(f"Response Time: {scorecard_duration:.3f}s")

    # Model 2: rpt_executive_kpis_monthly
    t0 = time.time()
    cur.execute("SELECT month, total_gmv, active_sellers_count, churned_sellers_count, monthly_churn_rate, macro_fulfillment_sla_rate, macro_order_defect_rate FROM CARTOGRAPHER_DB.MARTS.RPT_EXECUTIVE_KPIS_MONTHLY ORDER BY month DESC LIMIT 5")
    macro_sample = cur.fetchall()
    cur.execute("SELECT COUNT(*) FROM CARTOGRAPHER_DB.MARTS.RPT_EXECUTIVE_KPIS_MONTHLY")
    macro_count = cur.fetchone()[0]
    macro_duration = time.time() - t0

    cur.execute("DESCRIBE VIEW CARTOGRAPHER_DB.MARTS.RPT_EXECUTIVE_KPIS_MONTHLY")
    macro_cols = [c[0] for c in cur.fetchall()]

    print("\n--- RPT_EXECUTIVE_KPIS_MONTHLY AUDIT ---")
    print(f"Row Count (Months): {macro_count}")
    print(f"Projected Columns ({len(macro_cols)}): {macro_cols}")
    print(f"Response Time: {macro_duration:.3f}s")
    print("Sample Most Recent Months:")
    for r in macro_sample:
        gmv_val = float(r[1]) if r[1] is not None else 0.0
        print(f"  {r[0]}: GMV=R${gmv_val:,.2f}, Active={r[2]}, Churned={r[3]}, ChurnRate={r[4]}, SLA={r[5]}, ODR={r[6]}")

    conn.close()

if __name__ == "__main__":
    audit_reporting()
