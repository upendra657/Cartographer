# Cartographer: Production Operations Runbook

## 1. Overview & Objective
This runbook provides complete, step-by-step operational procedures for provisioning, loading, building, validating, and maintaining the **Cartographer** marketplace supply analytics warehouse.

---

## 2. Environment Prerequisites & Bootstrap

### 2.1 Toolchain Requirements
- **macOS / Linux**: x86_64 or ARM64.
- **Python**: `3.11.x` (ensures binary wheel compatibility with `pyarrow`, `dbt-snowflake`, `snowflake-connector-python`).
- **Java**: `17 LTS` (OpenJDK or Eclipse Temurin) required for Apache Spark 3.5.x Catalyst engine. Verify with `java -version`.
- **Snowflake Account**: Administrative account access (`ACCOUNTADMIN` or `SECURITYADMIN` + `SYSADMIN`).

### 2.2 Bootstrap Virtual Environment
```bash
# 1. Navigate to project root
cd /path/to/Cartographer

# 2. Initialize virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# 3. Upgrade package installer
pip install --upgrade pip setuptools wheel

# 4. Install pinned project dependencies
pip install -r requirements.txt

# 5. Verify CLI tool installations
dbt --version
sqlfluff --version
streamlit --version
python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
```

### 2.3 Environment Variable Setup
Create `.env` at root with the following keys:
```ini
SNOWFLAKE_ACCOUNT="YOUR_ACCOUNT_LOCATOR.REGION"
SNOWFLAKE_USER="YOUR_USERNAME"
SNOWFLAKE_PASSWORD="YOUR_PASSWORD"
SNOWFLAKE_ROLE="TRANSFORMER"
SNOWFLAKE_WAREHOUSE="CARTOGRAPHER_WH"
SNOWFLAKE_DATABASE="CARTOGRAPHER_DB"
SNOWFLAKE_SCHEMA="MARTS"
```

Verify dbt profile resolution:
```bash
dbt debug --config-dir
dbt debug
```
Ensure all checks pass (`Connection test: OK`).

---

## 3. Snowflake Infrastructure & RBAC Initialization

The warehouse follows a strict 3-tier Medallion architecture with least-privilege security boundaries.

### 3.1 DDL Execution Order
Execute scripts sequentially using a Snowflake Worksheet or SnowSQL under role `ACCOUNTADMIN`:

1. **`snowflake/01_setup_database.sql`**:
   - Creates database `CARTOGRAPHER_DB`.
   - Creates schemas: `RAW`, `STAGING`, `MARTS`.
   - Provisions functional roles: `LOADER`, `TRANSFORMER`, `REPORTER`.
   - Configures schema grants:
     - `LOADER`: Full control on `RAW` schema.
     - `TRANSFORMER`: Read on `RAW`, full control on `STAGING` and `MARTS`.
     - `REPORTER`: Read-only (`USAGE`, `SELECT`) on `MARTS` only.
2. **`snowflake/02_setup_warehouse.sql`**:
   - Creates virtual warehouse `CARTOGRAPHER_WH`:
     - Size: `X-SMALL` (1 credit/hour).
     - Auto-suspend: `60 seconds` (prevents idle billing).
     - Auto-resume: `TRUE`.
     - Statement timeout: `300 seconds` (5-minute circuit breaker against runaway queries).
3. **`snowflake/03_storage_stage.sql`**:
   - Creates internal named stage `@CARTOGRAPHER_DB.RAW.CARTOGRAPHER_STAGE`.
   - Configures binary Parquet file format (`TYPE = PARQUET, COMPRESSION = SNAPPY`).

### 3.2 RBAC Isolation Verification
Run the following test to verify that the reporting boundary is secure:
```sql
USE ROLE REPORTER;
USE WAREHOUSE CARTOGRAPHER_WH;
USE SECONDARY ROLES NONE;

-- 1. This MUST succeed:
SELECT COUNT(*) FROM CARTOGRAPHER_DB.MARTS.FCT_SELLER_MONTHLY_PERFORMANCE;

-- 2. This MUST fail with Error 002003:
SELECT COUNT(*) FROM CARTOGRAPHER_DB.RAW.RAW_ORDERS;
```

---

## 4. Ingestion Pipeline & Parquet Staging Execution

### 4.1 PySpark Data Extraction & Replay Generation
Cartographer uses a local single-node PySpark 3.5 pipeline to convert raw CSVs into strongly typed Snappy Parquet files and simulate a 22-month temporal as-of replay.

```bash
# 1. Execute extraction and schema validation
python pyspark_ingestion/extract.py

# 2. Generate 22 temporal replay batches (2017-01 to 2018-10)
python pyspark_ingestion/replay_generator.py
```
- Input: `Datasets/*.csv` (9 source files, 1.55M rows).
- Output: `data/staged/*.parquet` partitioned by `batch_id`.
- Compression: Snappy Parquet (shrinks 122 MB raw CSVs down to ~60 MB).
- Contract verification: Generates `pyspark_ingestion/conversion_reconciliation.json` confirming 0 row loss.

### 4.2 Staging Upload & Snowflake Loading
```bash
# 1. Upload Parquet files to Snowflake internal named stage
python snowflake/upload_to_stage.py

# 2. Execute idempotent COPY INTO commands
# Run snowflake/04_copy_ingestion.sql under role LOADER
```
The `COPY INTO` commands use `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` to map Parquet columns to strongly typed Snowflake relational tables without schema drift.

---

## 5. dbt Build Sequencing & Execution

### 5.1 Standard Production Build Sequence
Execute the models and tests in topological dependency order:

```bash
# Step 1: Install external packages (dbt-utils)
dbt deps

# Step 2: Seed reference datasets (if applicable)
dbt seed

# Step 3: Compile models and validate Jinja macro DAG
dbt compile

# Step 4: Execute full pipeline build (Staging -> Intermediate -> Marts -> Reporting)
dbt build

# Expected Output:
# PASS=76 WARN=0 ERROR=0 SKIP=0 TOTAL=76
```

### 5.2 Targeted & Incremental Build Commands
For day-to-day development or rapid iteration:

- **Run only Staging layer**:
  ```bash
  dbt run --select path:models/staging
  ```
- **Run only Intermediate layer**:
  ```bash
  dbt run --select path:models/intermediate
  ```
- **Run Core Dimensional Marts**:
  ```bash
  dbt run --select path:models/marts/core
  ```
- **Run Reporting Views**:
  ```bash
  dbt run --select path:models/marts/reporting
  ```
- **Execute Monthly Full Refresh (Reconciliation for 0.48% long-tail events)**:
  ```bash
  dbt run --select path:models/marts/core --full-refresh
  ```

### 5.3 Quality Gate Testing
Execute data assertions across the suite:

```bash
# Run all tests across the warehouse
dbt test

# Run fast CI quality gate (Marts & deterministic unit tests in ~13 seconds)
dbt test --select "path:models/marts path:tests/unit"

# Run financial and row-count reconciliation invariants
dbt test --select path:tests/singular
```

---

## 6. Code Quality & CI/CD Pipeline Operations

### 6.1 Local SQLFluff Linting
Before opening a pull request, run the SQLFluff linter configured for Snowflake dialect and dbt templating:

```bash
# Lint all models
sqlfluff lint models/

# Lint specific directory
sqlfluff lint models/marts/reporting/

# Automatically fix whitespace and formatting errors
sqlfluff fix models/
```

### 6.2 GitHub Actions CI Pipeline
The automated CI workflow is configured in `.github/workflows/ci.yml`.

- **Trigger**: Every `pull_request` targeting `main`.
- **Environment**: Ubuntu Latest, Python 3.11.
- **Fail-Fast Gates**:
  1. **Lint Gate**: `sqlfluff lint models/` blocks PR if SQL standards are breached.
  2. **Compile Gate**: `dbt compile` checks Jinja macros and Snowflake DAG resolution.
  3. **Test Gate**: `dbt test --select "path:models/marts path:tests/unit"` validates 48 core assertions.
- **Required Secrets in GitHub Repository**:
  - `SNOWFLAKE_ACCOUNT`
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_PASSWORD`

---

## 7. Streamlit BI Dashboard Operations

### 7.1 Local Application Execution
Launch the executive intelligence portal:
```bash
streamlit run app/dashboard.py
```
- Default URL: `http://localhost:8501`.
- Authentication: Reads credentials from `.env` using role `REPORTER`.
- Caching: `@st.cache_data(ttl=600)` retains query results in memory for 10 minutes.
- Serialization: Fetches data via native Snowflake-to-Arrow binary streaming (`cursor.fetch_pandas_all()`), reducing transfer overhead by 70%.

### 7.2 Application Verification Checklist
1. **Executive Health Overview Tab**:
   - Verify KPI cards display total GMV (R$ 13.59M), active seller counts, on-time rate, and defect rate.
   - Verify monthly trend chart loads smoothly.
2. **Seller Health Tiers & Risk Matrix Tab**:
   - Verify health tier distribution donut chart renders 4 tiers.
   - Verify risk quadrant scatter plot maps sellers by GMV vs Defect Rate.
3. **Partner Drilldown Tab**:
   - Verify seller selector allows filtering individual partners.
   - Verify lifecycle transition history and monthly metrics load accurately.

---

## 8. Incident Response & Troubleshooting

| Symptom | Probable Cause | Remediation Procedure |
| :--- | :--- | :--- |
| **Snowflake Error `002003: Schema does not exist`** | Role lacking `USAGE` privilege on schema or database. | Confirm active role using `SELECT CURRENT_ROLE();`. If running as `REPORTER`, access to `RAW` is intentionally blocked. |
| **dbt Error: `Relation does not exist`** | Model reference altered or upstream table not yet created. | Run `dbt compile` to inspect generated SQL. Build upstream dependencies with `dbt build --select +model_name`. |
| **SQLFluff Lint Failure: `LT05 Line too long`** | SQL statement or comment exceeds 100 characters. | Wrap comments or break SELECT expressions across multiple lines with 4-space indentation. |
| **Streamlit App Hangs on Data Fetch** | Virtual warehouse suspended or connection timed out. | Verify warehouse auto-resume is enabled. Check `.env` credentials and test connection via `python -c "from app.connection import get_reporter_connection; conn=get_reporter_connection(); print(conn)"`. |
| **Parquet Reader Error in Spark** | Embedded CRLF linebreaks in source CSVs. | Ensure `multiLine=true` and `escape="\""` are set in PySpark UniVocity reader options (`extract.py`). |
