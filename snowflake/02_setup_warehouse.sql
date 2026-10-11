-- ============================================================================
-- CARTOGRAPHER: Compute Warehouse Sizing & Cost Governance (Step 1.13, ADR-006)
-- Sizing: X-Small (1 credit/hour), 60s auto-suspend, auto-resume
-- Guardrails: Statement timeout, credit quota resource monitor
-- ============================================================================

-- 1. Create Resource Monitor for Cost Protection (Quota: 10 credits / month)
CREATE OR REPLACE RESOURCE MONITOR CARTOGRAPHER_RM
    WITH CREDIT_QUOTA = 10
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- 2. Create the Primary Analytical Virtual Warehouse
CREATE WAREHOUSE IF NOT EXISTS CARTOGRAPHER_WH
    WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 1800
    COMMENT = 'Virtual warehouse for Cartographer marketplace analytics engine workloads';

-- 3. Attach the Resource Monitor to the Warehouse
ALTER WAREHOUSE CARTOGRAPHER_WH 
    SET RESOURCE_MONITOR = CARTOGRAPHER_RM;

-- 4. Grant Usage on Warehouse to Operating Roles
GRANT USAGE ON WAREHOUSE CARTOGRAPHER_WH TO ROLE LOADER;
GRANT USAGE ON WAREHOUSE CARTOGRAPHER_WH TO ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE CARTOGRAPHER_WH TO ROLE REPORTER;
