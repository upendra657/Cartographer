"""
Snowflake Connection & Resilient Data Access Utility for Streamlit Application
Connects strictly using role 'REPORTER' to enforce least-privilege Medallion consumption.
Supports seamless failover to certified local Parquet snapshots for instant public cloud deployment.
"""

import os
from pathlib import Path
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

CACHE_DIR = Path(__file__).resolve().parent / "cache"

def get_snowflake_credentials():
    """
    Retrieve Snowflake credentials from Streamlit Secrets (Cloud) or .env (Local).
    """
    load_dotenv()
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")

    # Try Streamlit Secrets if available
    try:
        import streamlit as st
        if hasattr(st, "secrets"):
            user = user or st.secrets.get("SNOWFLAKE_USER")
            password = password or st.secrets.get("SNOWFLAKE_PASSWORD")
            account = account or st.secrets.get("SNOWFLAKE_ACCOUNT")
    except Exception:
        pass

    return user, password, account

def get_reporter_connection():
    """
    Establish and return a Snowflake connection configured strictly for role REPORTER.
    Disables secondary roles to guarantee zero access to RAW PII or intermediate models.
    """
    user, password, account = get_snowflake_credentials()

    if not all([user, password, account]):
        return None

    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse="CARTOGRAPHER_WH",
            database="CARTOGRAPHER_DB",
            schema="MARTS",
            role="REPORTER",
            login_timeout=10,
            network_timeout=15
        )
        # Enforce pure REPORTER role without inheriting administrative secondary roles
        cur = conn.cursor()
        cur.execute("USE SECONDARY ROLES NONE;")
        cur.close()
        return conn
    except Exception as e:
        return None

def fetch_reporting_data():
    """
    Retrieve executive KPIs and seller scorecards.
    Attempts live query against Snowflake CARTOGRAPHER_DB.MARTS under role REPORTER.
    If credentials are missing or warehouse is unreachable, seamlessly loads certified Parquet cache.
    Returns: (df_kpis, df_scorecard, connection_status)
    """
    conn = get_reporter_connection()

    if conn is not None:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT * 
                FROM CARTOGRAPHER_DB.MARTS.RPT_EXECUTIVE_KPIS_MONTHLY 
                ORDER BY month ASC;
            """)
            df_kpis = cur.fetch_pandas_all()

            cur.execute("""
                SELECT * 
                FROM CARTOGRAPHER_DB.MARTS.RPT_SELLER_HEALTH_SCORECARD;
            """)
            df_scorecard = cur.fetch_pandas_all()

            cur.close()
            conn.close()

            df_kpis["MONTH"] = pd.to_datetime(df_kpis["MONTH"]).dt.date
            df_scorecard["MONTH"] = pd.to_datetime(df_scorecard["MONTH"]).dt.date
            if "FIRST_SALE_DATE" in df_scorecard.columns:
                df_scorecard["FIRST_SALE_DATE"] = pd.to_datetime(df_scorecard["FIRST_SALE_DATE"]).dt.date
            if "LAST_SALE_DATE" in df_scorecard.columns:
                df_scorecard["LAST_SALE_DATE"] = pd.to_datetime(df_scorecard["LAST_SALE_DATE"]).dt.date

            return df_kpis, df_scorecard, "LIVE_SNOWFLAKE"
        except Exception:
            pass  # Fall back to certified cache

    # Fallback to local Parquet cache
    kpi_cache = CACHE_DIR / "rpt_executive_kpis_monthly.parquet"
    scorecard_cache = CACHE_DIR / "rpt_seller_health_scorecard.parquet"

    if kpi_cache.exists() and scorecard_cache.exists():
        df_kpis = pd.read_parquet(kpi_cache)
        df_scorecard = pd.read_parquet(scorecard_cache)

        df_kpis["MONTH"] = pd.to_datetime(df_kpis["MONTH"]).dt.date
        df_scorecard["MONTH"] = pd.to_datetime(df_scorecard["MONTH"]).dt.date
        if "FIRST_SALE_DATE" in df_scorecard.columns:
            df_scorecard["FIRST_SALE_DATE"] = pd.to_datetime(df_scorecard["FIRST_SALE_DATE"]).dt.date
        if "LAST_SALE_DATE" in df_scorecard.columns:
            df_scorecard["LAST_SALE_DATE"] = pd.to_datetime(df_scorecard["LAST_SALE_DATE"]).dt.date

        return df_kpis, df_scorecard, "CERTIFIED_CACHE"

    raise RuntimeError("Unable to connect to Snowflake and certified local cache is missing.")
