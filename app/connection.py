"""
Snowflake Connection Utility for Streamlit Application
Connects strictly using role 'REPORTER' to enforce least-privilege Medallion consumption.
"""

import os
import snowflake.connector
from dotenv import load_dotenv

def get_reporter_connection():
    """
    Establish and return a Snowflake connection configured for the REPORTER role.
    Disables secondary roles to ensure complete privilege isolation to CARTOGRAPHER_DB.MARTS.
    """
    load_dotenv()

    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = "CARTOGRAPHER_WH"
    database = "CARTOGRAPHER_DB"
    schema = "MARTS"
    role = "REPORTER"

    if not all([user, password, account]):
        raise ValueError("Missing Snowflake credentials in environment variables (.env).")

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )

    # Enforce pure REPORTER role without inheriting administrative secondary roles
    cur = conn.cursor()
    cur.execute("USE SECONDARY ROLES NONE;")
    cur.close()

    return conn
