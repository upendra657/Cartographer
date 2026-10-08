# Execution Playbook: Cartographer

| | |
|---|---|
| **Purpose** | Step-by-step build plan for the Seller Health Data Mart defined in [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) |
| **Structure** | 5 phases, 55 steps, each sized to one reviewable commit |
| **Effort baseline** | 16–19 focused days |
| **Governance** | Phase-gated execution; every architectural choice is recorded as an ADR before code changes |

## How to use this playbook

- One step is one reviewable unit of work and one focused commit (Conventional Commits: `feat`, `fix`, `test`, `docs`, `ci`, `refactor`, `chore`).
- Each step states a **deliverable**, the **acceptance** evidence that closes it, and the **ADR** it depends on, where one applies.
- Acceptance numbers come from the source profile in PROJECT_CONTEXT.md §6. A step closes when the evidence matches, not when the code runs.
- Findings are referenced as F1–F14 (PROJECT_CONTEXT.md §6) and owner decisions as D1–D8 (§9).

## Operating rules

1. **Phase gate.** One phase per working session. At the end of each phase, the implementation agent stops and outputs exactly: `PHASE [X] COMPLETE. Halting execution. Awaiting human code review and Ownership Checkpoint clearance before proceeding.`
2. **No silent architecture changes.** A new technology, materialization, clustering key or join pattern needs a written proposal (why it is needed, at least one rejected alternative, and the cost, latency and complexity trade-off), owner approval, and an ADR in `DECISIONS.md` before any file changes.
3. **Component cycle.** Understand → Design (grain, lineage, materialization) → Implement → Explain (nulls, late data, fan-out, duplicates) → Validate → Document → Commit → Ownership checkpoint.
4. **Explain before merge.** No code merges until the owner can explain what it does, why it is shaped that way, and which edge cases it handles.
5. **Evidence over assertion.** README and interview claims use only numbers produced by a run: row counts, test counts, drift, build time, credits.

## Target architecture

```
Olist CSVs (9 files, 1,550,922 rows, ~120 MB)
  │  PySpark: explicit schemas · multi-line + BOM-safe parsing · FAILFAST
  ▼
Parquet (typed, snappy) ── as-of replay batches: batch_id 2017-01 … 2018-10
  │  PUT
  ▼
Snowflake internal stage
  │  COPY INTO: match by column name · abort on error · load metadata
  ▼
RAW            append-only, typed, one table per source
  ▼  dbt
STAGING        views · rename, cast, fix typos, latest version per key
  ▼
INTERMEDIATE   views · reviews → order grain · items → seller-order grain · seller-month spine
  ▼
MARTS          dim_date · dim_sellers · dim_products · dim_customers
               fct_order_items                 (transaction)
               fct_seller_orders               (accumulating snapshot)
               fct_seller_monthly_performance  (periodic snapshot)
```

### Bus matrix

| Business process | Fact | Grain | Date | Seller | Product | Customer |
|---|---|---|:-:|:-:|:-:|:-:|
| Order line sales | `fct_order_items` | One row per `order_id`, `order_item_id` | X | X | X | X |
| Seller-order fulfillment | `fct_seller_orders` | One row per `seller_id`, `order_id` | X | X | | X |
| Seller health | `fct_seller_monthly_performance` | One row per `seller_id`, calendar month | X | X | | |

### Planned model inventory

| Layer | Model | Grain | Expected rows | Materialization |
|---|---|---|---:|---|
| Staging | `stg_olist__orders` | `order_id` | 99,441 | view |
| Staging | `stg_olist__order_items` | `order_id`, `order_item_id` | 112,650 | view |
| Staging | `stg_olist__order_reviews` | `review_id`, `order_id` | 99,224 | view |
| Staging | `stg_olist__order_payments` | `order_id`, `payment_sequential` | 103,886 | view |
| Staging | `stg_olist__customers` | `customer_id` | 99,441 | view |
| Staging | `stg_olist__sellers` | `seller_id` | 3,095 | view |
| Staging | `stg_olist__products` | `product_id` | 32,951 | view |
| Staging | `stg_olist__product_category_translations` | `product_category_name` | 71 | view |
| Intermediate | `int_reviews_rolled_up_to_orders` | `order_id` | 98,673 | view |
| Intermediate | `int_order_items_sellers_joined` | `order_id`, `order_item_id` | 112,650 | view |
| Intermediate | `int_seller_orders_aggregated` | `seller_id`, `order_id` | 100,010 | view |
| Intermediate | `int_seller_month_spine` | `seller_id`, month | 32,057 | view |
| Marts | `dim_date` | calendar date | 852 | table |
| Marts | `dim_sellers` | `seller_id` | 3,095 | table |
| Marts | `dim_products` | `product_id` | 32,951 | table |
| Marts | `dim_customers` | `customer_unique_id` | 96,096 | table |
| Marts | `fct_order_items` | `order_id`, `order_item_id` | 112,650 | incremental (merge) |
| Marts | `fct_seller_orders` | `seller_id`, `order_id` | 100,010 | incremental (merge) |
| Marts | `fct_seller_monthly_performance` | `seller_id`, month | 32,057 | incremental (merge) |

The spine and monthly-fact counts assume D1–D6 as recommended; recompute them if a decision changes.

## ADR backlog

The recommendations below are proposals. Each becomes binding only when the owner approves it and it is recorded in `DECISIONS.md`.

| ADR | Decision | Recommended | Rejected alternative | Trade-off to defend | Phase |
|---|---|---|---|---|:-:|
| 001 | Route CSVs through PySpark → Parquet | Yes: a typed contract boundary and the replay generator | COPY INTO directly from CSV | Adds a JVM runtime and an extra hop; buys schema enforcement before the warehouse. At 120 MB this is a contract decision, not a performance one | 1 |
| 002 | Error policy | Fail fast: Spark `FAILFAST`, COPY `ON_ERROR = ABORT_STATEMENT` | Permissive parsing with a quarantine table; `ON_ERROR = CONTINUE` | A fixed snapshot has no bad rows, only parser bugs. A live feed would need quarantine | 1 |
| 003 | Late-arriving data | Monthly as-of replay derived from lifecycle timestamps | Append-only monthly split; hand-built delta file | Exercises real updates and status regressions; costs one extra job (~1 day) | 1 |
| 004 | Stage type | Internal named stage | S3 external stage; external tables | No cloud account, IAM role or storage integration; gives up production parity and Snowpipe auto-ingest | 1 |
| 005 | RAW typing | Typed tables loaded with `MATCH_BY_COLUMN_NAME` | A single `VARIANT` column; DDL generated by `INFER_SCHEMA` | Type errors fail at load time, not query time. Risk: a renamed source column loads as NULL silently, which not-null and reconciliation tests catch | 1 |
| 006 | Warehouse sizing | X-Small, 60 s auto-suspend, resource monitor | Small | 1 credit/hour vs. 2. No spill is expected at this volume; verified in the query profile | 1 |
| 007 | Staging materialization | Views | Tables | No storage and always current; recomputed on every read, which is cheap at this volume | 2 |
| 008 | Where deduplication lives | Staging: latest version per natural key | MERGE into RAW at load time | RAW stays an immutable, auditable load log; staging carries one window function per model | 2 |
| 009 | Test severity | `error` for grain, keys and referential integrity; `warn` for known anomalies, escalating to `error` above 2x baseline | All `error`; all `warn` | The build stops only for defects that corrupt joins. Known source noise stays visible and escalates when it grows | 2 |
| 010 | Fact table set | Three facts (transaction, accumulating snapshot, periodic snapshot) and four conformed dimensions | One wide seller-month table | Drill-through and category cuts vs. fewer models to maintain | 3 |
| 011 | Surrogate keys | Hash of the natural key | Natural keys only; database sequences | Deterministic across full refreshes and environments, at the cost of 32-character join keys | 3 |
| 012 | Incremental strategy and lookback | `merge` on the unique key; reprocess the trailing 3 purchase months (lookback variable = 2); `incremental_predicates` to prune the target scan | `microbatch`; `delete+insert`; full rebuild on every run | Catches 99.52% of late events per run for a fraction of full-rebuild compute; one variable tunes it | 3 |
| 013 | Late-tail correction | Scheduled full refresh, with drift measured before each one | Recompute affected keys from a `_loaded_at` watermark | Simple and auditable, but exact only after the refresh. The watermark approach is exact on every run but adds state and complexity | 3 |
| 014 | Seller-month density | Dense spine from each seller's first active month | Sparse: active months only | Churn and at-risk status need zero-activity rows; costs 1.98x the rows (32,057 vs. 16,171) | 3 |
| 015 | CI scope | Slim CI: `state:modified+` with `--defer` to the prod manifest, in a per-PR schema | Full build on every PR | Saves minutes and credits; gives up coverage of unmodified downstream models | 4 |
| 016 | CI authentication | Key-pair service user with a least-privilege role | Password; personal credentials | Security; Snowflake is phasing out single-factor password sign-in | 4 |

---

## Phase 1: PySpark ELT and Snowflake ingestion

**Objective.** Land all 9 source files in Snowflake RAW, typed and reconciled to the row and the cent, through a reproducible PySpark → Parquet → COPY INTO path.
**Effort.** 4–5 days. **Entry.** PROJECT_CONTEXT.md reviewed. **Exit.** Steps 1.1–1.17 accepted; ADR-001 to 006 recorded; checkpoint cleared.
**Sequencing.** Parts 1A and 1B need no Snowflake account. Open the trial at step 1.11 so its clock only runs while warehouse work is happening.

### 1A. Foundations

**1.1 Repository hygiene**
- Deliverable: a standalone repository whose `.gitignore` covers source data, Parquet output, virtual environments, dbt artifacts, `profiles.yml`, keys and `.env`. This is already in place from the scaffolding commit.
- This folder sits inside another local repository (`Desktop/Projects`). Add `Cartographer/` to that repository's ignore file so the two never mix.
- Decide whether source files stay in `Datasets/` or move to `data/raw/`, then use that one path everywhere.
- Acceptance: after a full local run, `git status` shows no data, keys or build artifacts.

**1.2 Pinned toolchain**
- Deliverable: a Python 3.11 virtual environment; Java 17 (Temurin) with `JAVA_HOME` set for the project; `requirements.txt` with exact pins for pyspark, dbt-core, dbt-snowflake, the Snowflake connector or CLI, and pytest.
- Why: this machine runs Python 3.14 and Java 19. Spark 3.5 supports Java 8/11/17 and Spark 4.x supports Java 17/21, so Java 19 appears in neither list. Current dbt and PySpark releases support Python 3.11; confirm 3.14 support before adopting it.
- Acceptance: a local SparkSession reads one CSV; `dbt --version` lists the Snowflake adapter; versions are recorded for the README.

**1.3 Freeze the source baseline**
- Deliverable: a machine-readable expected-values file (row counts, distinct key counts and money totals per file) built from PROJECT_CONTEXT.md §6. Every reconciliation step reads from it.
- Acceptance: values match §6 exactly: 9 row counts; Σprice R$13,591,643.70; Σfreight R$2,251,909.54; Σpayment R$16,008,872.12.

### 1B. PySpark ELT: CSV → Parquet

**1.4 Explicit schemas** (F3)
- One declared schema per file, with schema inference disabled.
- IDs and zip prefixes as strings; timestamps parsed with an explicit pattern; money as decimal, never float, since float sums drift when reconciling R$13.6M to the cent; counts and scores as integers.
- Acceptance: a test proves zip prefix `01037` survives the round trip unchanged.

**1.5 Parser configuration** (F1, F2, F4; ADR-002)
- Multi-line records and quote escaping enabled for reviews; BOM-safe header handling for the translation file; UTF-8 throughout; fail-fast mode.
- Acceptance: reviews parse to exactly 99,224 rows and all 3,852 multi-line comments keep their line breaks; the translation file's first column reads `product_category_name`.

**1.6 Raw-fidelity rule and technical columns**
- Spark does no renames, business logic or deduplication. Source typos such as `product_name_lenght` survive to RAW and are fixed in staging, so RAW always matches the source contract.
- Add technical columns only: `_batch_id`, `_as_of_date`, `_extracted_at`. Snowflake adds `_source_file` and `_loaded_at` at load.
- Acceptance: Parquet column names are the source header plus the technical columns, nothing else.

**1.7 Output layout**
- One directory per entity, partitioned by `_batch_id` only, with snappy compression and 1–4 files per entity per batch. Many small files slow both Spark and COPY. Snowflake's loading guidance targets files of roughly 100–250 MB compressed, a size this dataset never reaches.
- Acceptance: file counts are within the limit; total Parquet size is recorded against the 120 MB CSV input.

**1.8 As-of replay generator** (F13; ADR-003)
- Emit 22 monthly batches with as-of dates from 2017-01-31 to 2018-10-31. The first batch carries all 2016 history; the last two carry the September–October 2018 tail and late review answers.
- Each batch contains only what changed since the previous one:
  - Orders: new purchases, plus orders whose lifecycle advanced (approval, handoff, delivery). Timestamps later than the as-of date are nulled; status is derived from the furthest milestone reached.
  - Statuses that timestamps can't express (`canceled`, `unavailable`, `invoiced`, `processing`) are applied in the batch of the order's last recorded event. Deferring them all to the final batch would turn every such order into an artificial late update and invalidate the drift test in step 3.11.
  - Reviews: released when answered, never before the order's first batch (74 reviews pre-date their purchase, F9).
  - Items and payments: released with the order's first batch, since they don't change in the source.
  - Customers, sellers, products, translations and geolocation: loaded in full in the first batch.
- Edge cases the replay must produce on purpose: the 6 canceled orders that carry a delivery date appear first as `delivered` and flip to `canceled` one batch later (a status regression), and reviews are answered up to 528 days after purchase.
- Acceptance: applying all 22 batches and keeping the latest version per key reproduces every source file exactly (row counts and column checksums); per-batch counts are logged.

**1.9 PySpark tests**
- pytest with a local SparkSession over small fixture files that reproduce each hazard: BOM header, CRLF, quoted line break, leading-zero zip, empty timestamp, mixed quoting. Replay rules are tested too: status derivation, review release timing and the regression case.
- Acceptance: removing the multi-line option or the explicit schema makes a test fail, verified once by deliberately removing each.

**1.10 Conversion reconciliation**
- Compare Parquet (latest version per key) with the frozen baseline from step 1.3: row counts, distinct keys and money totals.
- Acceptance: exact match; the report is saved with the run.

### 1C. Snowflake ingestion: DDL and COPY INTO

**1.11 Open the Snowflake trial**
- Choose the region nearest Bangkok (AWS Asia Pacific, Singapore), turn on MFA for the human user, and record the trial end date in the risk register.
- Acceptance: the account is reachable from the pinned connector.

**1.12 `snowflake/01_setup_database.sql`**
- Database `CARTOGRAPHER`; schema `RAW` (dbt creates its own schemas); roles `LOADER` (writes RAW), `TRANSFORMER` (reads RAW, owns the dbt schemas) and `REPORTER` (reads marts); a CI service user prepared for key-pair authentication (ADR-016); append-only RAW tables, typed per ADR-005, with the technical columns.
- Acceptance: `TRANSFORMER` cannot write to RAW; `REPORTER` cannot read RAW.

**1.13 `snowflake/02_setup_warehouse.sql`** (ADR-006)
- X-Small; 60-second auto-suspend; auto-resume; initially suspended; a statement timeout to stop runaway queries; a resource monitor with a credit quota that notifies at 75% and suspends at 100%. An optional second X-Small warehouse for CI makes cost attributable per workload.
- Acceptance: the warehouse suspends after 60 seconds idle; the monitor is attached.

**1.14 `snowflake/03_storage_stage.sql`** (ADR-004)
- A Parquet file format; an internal named stage with one path per entity and batch (`<entity>/batch_id=<YYYY-MM>/`); upload with PUT from the Snowflake CLI or the Python connector.
- Acceptance: the stage listing shows the expected file count per path.

**1.15 `snowflake/04_copy_ingestion.sql`** (ADR-002, ADR-005)
- One COPY per entity and batch: match by column name (case-insensitive), abort on error, and write the source file name and load time into `_source_file` and `_loaded_at`.
- Idempotency: Snowflake keeps per-table load metadata for 64 days and skips files it has already loaded, so re-running a COPY is safe. `FORCE = TRUE` reloads files and duplicates rows; reserve it for deliberate reloads into a truncated table.
- Acceptance: running the same COPY twice loads zero rows the second time.

**1.16 Load reconciliation**
- Compare RAW (latest version per key) against the baseline: 9 row counts and money totals to the cent. Load history shows no partially loaded files; the largest COPY's query profile shows no spill.
- Acceptance: exact match; evidence saved for the README.

**1.17 Phase documentation**
- `DECISIONS.md`: ADR-001 to 006. `FAILURE_MODES.md`: multi-line parsing, the BOM header, leading zeros, silent NULLs from column-name matching, double loads via `FORCE`, a warehouse left running. `CHANGELOG.md`: v0.1.0.

**Ownership checkpoint: Phase 1**
1. Why run Spark on 120 MB when Snowflake can load the CSVs directly? What would make you remove it?
2. The reviews file has 104,720 lines and 99,224 records. Explain the gap, and how the pipeline would fail if you got it wrong.
3. What does schema inference do to zip prefix `01037`, and which join breaks?
4. Why an internal stage rather than external tables? When would you move to S3 and Snowpipe?
5. You run the same COPY twice. What happens, and why? What changes with `FORCE`?
6. How did you size the warehouse, and how would you detect local or remote spill?

`PHASE 1 COMPLETE. Halting execution. Awaiting human code review and Ownership Checkpoint clearance before proceeding.`

---

## Phase 2: dbt staging layer

**Objective.** One typed, tested and documented view per source, 1:1 with RAW, with no joins and no business logic.
**Effort.** 2–3 days. **Entry.** Phase 1 cleared. **Exit.** `dbt build --select staging` is green; every key is tested; ADR-007 to 009 recorded.

**2.1 Project setup**
- `dbt_project.yml` (project `cartographer`) with folder defaults: staging and intermediate as views, marts as tables. `packages.yml` pins dbt-utils and dbt-expectations. Use the actively maintained dbt-expectations fork, because the original repository is no longer maintained, and confirm on the dbt package hub before pinning.
- Schema strategy: prod builds into `STAGING`, `INTERMEDIATE` and `MARTS`; dev and CI build into prefixed schemas (`DEV_<initials>_*`, `CI_PR_<number>_*`) through a custom schema-name macro.
- `profiles.yml` lives outside the repository, with `dev`, `ci` and `prod` targets.
- Acceptance: `dbt debug` passes for dev; `dbt deps` resolves the pinned versions.

**2.2 Source declarations** (`models/staging/olist/_stg_olist__sources.yml`)
- All 9 RAW tables are declared. Geolocation is declared for lineage but not staged.
- Source-level tests are limited to what holds on an append-only, versioned table: not-null on natural keys and accepted values on status fields. Uniqueness is tested after deduplication, in staging.
- Freshness on `_loaded_at` is declared at warn level to demonstrate the mechanism; the source itself is static.
- Acceptance: source tests pass.

**2.3 Staging models** (8 models, `stg_olist__<entity>`)
- Every model selects from the source, keeps the latest version per natural key (ordered by `_batch_id`, then `_loaded_at`), renames to convention (`_at` for timestamps, `_date` for dates, `is_`/`has_` for booleans), casts types and converts empty strings to NULL. There is no imputation; that is a business decision made in marts.
- Typo fixes: `product_name_lenght` → `product_name_length`, `product_description_lenght` → `product_description_length`.
- Types: timestamps as `TIMESTAMP_NTZ`, money as `NUMBER(12,2)`, zip prefixes as `VARCHAR(5)`.
- Entity rules:
  - `order_reviews`: the key is (`review_id`, `order_id`) (F5). Every row is kept; the rollup happens in intermediate.
  - `products`: a NULL category stays NULL (610 rows). Labeling it `unknown` is a presentation decision for `dim_products`.
  - `sellers`: `seller_city` is passed through raw; `seller_state` (23 clean values) is the geography of record (F12).
  - Category translations: the 2 missing categories go in a seed file (`seeds/category_translation_overrides.csv`) that is combined with the source translation in `dim_products`.
- Acceptance: each view's row count equals RAW's latest-version count and the §6 baseline.

**2.4 Staging tests** (`_stg_olist__models.yml`; ADR-009)
- Grain (`error`): unique and not-null on every natural key, with composite keys for items, reviews and payments.
- Referential integrity (`error`): items → orders, sellers and products; reviews → orders; payments → orders; orders → customers.
- Domain (`error`): `order_status` within its 8 values; `payment_type` within its 5 values; `review_score` between 1 and 5; `price` > 0; zip prefixes exactly 5 digits.
- Known anomalies (`warn`, escalating to `error` above 2x baseline): handoff before purchase (166 orders), delivery before handoff (23), review before purchase (74), shipping deadline more than 60 days after purchase (12 items), and payments differing from item totals by more than R$0.01 (380 orders).
- Acceptance: the build passes with exactly the expected warnings, and each warning count equals its baseline.

**2.5 Documentation in YAML**
- Every model and column is described, and descriptions are persisted to Snowflake comments so analysts see them in the warehouse.
- Acceptance: `dbt docs generate` shows no undocumented staging columns.

**2.6 Phase documentation**
- `DECISIONS.md`: ADR-007 to 009. `FAILURE_MODES.md`: the non-unique `review_id`, replay versions, an upstream typo fixed downstream. `CHANGELOG.md`: v0.2.0.

**Ownership checkpoint: Phase 2**
1. Why is `review_id` not the primary key of `stg_olist__order_reviews`? What breaks if you assume it is?
2. Why does deduplication live in staging rather than in RAW?
3. Walk through your severity policy. Which failures stop the build, which only warn, and where did the thresholds come from?
4. Why views for staging? At what volume or query pattern would you change that?
5. A new column appears in the source next month. What happens at each layer?

`PHASE 2 COMPLETE. Halting execution. Awaiting human code review and Ownership Checkpoint clearance before proceeding.`

---

## Phase 3: Intermediate and marts (Kimball star schema, incremental processing)

**Objective.** Deliver the Seller Health Data Mart: four conformed dimensions and three facts, incremental where volume grows, tested for grain, integrity and reconciliation, and shown to stay correct under late-arriving data.
**Effort.** 5–6 days. **Entry.** Phase 2 cleared; D1–D7 signed off. **Exit.** All marts green; replay drift measured and explained; ADR-010 to 014 recorded.

**3.1 Design gate**
- No mart code is written until the owner confirms the bus matrix, grains, model inventory, materializations and KPI definitions. Mart models live in `dbt/models/marts/core/`.
- Acceptance: ADR-010 and 011 recorded; a `DATA_DICTIONARY.md` skeleton lists every mart with a one-sentence grain.

**3.2 `int_reviews_rolled_up_to_orders`** (grain: `order_id`; 98,673 rows)
- One row per reviewed order: review count, worst score, latest score, first survey date, last answer time, and a negative-review flag (worst score ≤ 2).
- Built first because reviews are the 1:N child that inflates joins (F5, F6).
- Acceptance: unique `order_id`; 547 orders with more than one review; 14,533 orders flagged negative.

**3.3 `int_order_items_sellers_joined`** (grain: `order_id`, `order_item_id`; 112,650 rows)
- Items joined many-to-one to orders (lifecycle timestamps and status), sellers, products and the translated category. No one-to-many join is allowed at this grain.
- Acceptance: the row count equals `stg_olist__order_items` exactly, since any difference means fan-out or dropped rows; Σprice = R$13,591,643.70.

**3.4 `int_seller_orders_aggregated`** (grain: `seller_id`, `order_id`; 100,010 rows)
- Items aggregated to seller-order level (item count, GMV, freight, shipping deadline), then joined one-to-one to the order lifecycle and the review rollup.
- Flags: handoff on time; overdue and unshipped; impossible sequence (handoff before purchase); delivered late (date grain); negative review; multi-seller order; canceled; stuck in transit (shipped but undelivered after 60 days).
- Acceptance: unique (`seller_id`, `order_id`); 100,010 rows; one shipping deadline per seller-order, asserted rather than assumed; the 1,278 multi-seller orders yield 2,622 seller-orders; GMV reconciles to items.

**3.5 `int_seller_month_spine`** (grain: `seller_id`, month; 32,057 rows)
- Every month from a seller's first active month through 2018-08, whether active or not (ADR-014).
- Acceptance: the row count matches the dense baseline under the signed-off activity definition.

**3.6 Dimensions**
- `dim_date`: one row per day from 2016-09-01 to 2018-12-31 (852 rows), with month start, ISO week, quarter and an in-window flag. Built from a date-spine utility.
- `dim_sellers`: one row per `seller_id` (3,095). SCD Type 1, since the source holds no history (Type 2 is out of scope). Carries first and last sale dates, active-month count, state, zip prefix and raw city.
- `dim_products`: one row per `product_id` (32,951). Portuguese and English category with seed overrides; NULL categories labeled `unknown` (610); missing physical attributes left NULL (2 products).
- `dim_customers`: one row per `customer_unique_id` (96,096), with location from the most recent order, order count and a repeat-buyer flag (2,997 buyers).
- Every dimension carries a hash surrogate key (ADR-011), the natural key, and a reserved unknown-member row, so a missing dimension record never drops a fact row.
- Acceptance: unique, not-null keys; row counts as listed.

**3.7 `fct_order_items`** (transaction fact; grain: `order_id`, `order_item_id`; 112,650 rows)
- Foreign keys to purchase date, seller, product and customer; `order_id` as a degenerate dimension; measures `price` and `freight_value`.
- Order status is deliberately left out. Status changes and item facts don't, so keeping status on `fct_seller_orders` leaves this table effectively append-only.
- Incremental: `merge` on the surrogate key over the lookback window (ADR-012). Reprocessing a window must be idempotent, and a plain append is not.
- Acceptance: unique key; every foreign key resolves; Σprice reconciles to staging.

**3.8 `fct_seller_orders`** (accumulating snapshot; grain: `seller_id`, `order_id`; 100,010 rows)
- One row per seller-order, updated in place as milestones arrive: purchase, approval, shipping deadline, carrier handoff, customer delivery, promised delivery and last review answer. Carries lags in hours and days, the flags from step 3.4, current status, worst review score, GMV, freight and item count.
- Incremental: `merge` on the surrogate key, reprocessing seller-orders purchased in the trailing 3 months (ADR-012).
- Acceptance: unique key; every foreign key resolves; statuses match the source on 100% of rows after the full refresh in step 3.11.

**3.9 `fct_seller_monthly_performance`** (periodic snapshot; grain: `seller_id`, month; 32,057 rows)
- Dense from each seller's first active month (ADR-014). Measures are stored as numerator/denominator pairs: seller-orders, items, GMV and freight; SLA on-time and SLA-eligible; defects and defect-eligible; late and delivered; canceled.
- Lifecycle status (`new`, `active`, `at_risk`, `churned`, `dormant`, `reactivated`) follows PROJECT_CONTEXT.md §4.4. The model also carries trailing three-month volume, a health tier with `insufficient_volume` below 10 seller-orders (D7), and an `is_final` flag set once two full months have closed.
- Incremental: `merge` on (`seller_id`, month), rewriting the trailing 3 months. The model reads 3 more months of activity than it writes, so lifecycle status at the edge of the window is correct. Incremental predicates restrict the target scan to the rewrite window.
- Acceptance: unique (`seller_id`, month); window GMV equals `fct_order_items` for the same orders and statuses; every numerator ≤ its denominator; status values fall within the accepted set.

**3.10 Unit tests for business logic**
- dbt unit tests on mocked seller histories: first sale (`new`); 1 and 2 inactive months (`at_risk`); third inactive month (`churned`); a sale after churn (`reactivated`); a sale after 2 inactive months (`active`, not `reactivated`); a last sale in 2018-07 (never `churned`, because of right-censoring).
- SLA cases: on time, late, overdue and unshipped, and handoff before purchase (excluded).
- Acceptance: every lifecycle branch and SLA rule has a test, and a deliberately broken status rule fails the build.

**3.11 Replay validation: proving the incremental design**
- Load the 22 batches in order and run the incremental build after each one, logging rows inserted and updated per model.
- After the last batch, build the same marts with a full refresh in a separate schema and compare every row of `fct_seller_orders` and `fct_seller_monthly_performance`.
- Prediction: differences are limited to the 473 orders (0.48%) whose last lifecycle event lands 3 or more months after purchase, plus the seller-months they touch. A measured count outside this prediction is a defect to investigate, not a number to explain away.
- Then run the full refresh in place and confirm zero differences.
- Acceptance: predicted vs. measured drift, and the zero-difference result after the refresh, are recorded in ADR-012 and ADR-013.

**3.12 Reconciliation and performance**
- Singular tests: mart GMV equals staging GMV for the window to R$0.01; the seller-order count is 100,010; lifecycle status counts sum to the spine row count.
- Query profiles for the three incremental models: partitions scanned vs. total on an incremental run (proof that pruning works), bytes spilled (expected 0), and incremental vs. full-refresh duration.
- Acceptance: tests green; numbers recorded in ADR-012.

**3.13 Exposures and documentation**
- A dbt exposure for the Supply Ops seller-health scorecard, so the DAG shows its downstream consumer.
- `DATA_DICTIONARY.md`: every mart with its grain, columns, types, nullability, primary and foreign keys, and links to the KPI rules in PROJECT_CONTEXT.md §4. Generated from the YAML descriptions to keep a single source of truth.
- `DECISIONS.md`: ADR-010 to 014 with measured numbers. `FAILURE_MODES.md`: join fan-out (+5.02% GMV), late reviews beyond the lookback, status regression, spine growth at scale. `CHANGELOG.md`: v0.3.0.

**Ownership checkpoint: Phase 3**
1. State the grain of each fact table in one sentence. Why three facts instead of one?
2. Orders join 1:N to items, reviews and payments. How did you prevent fan-out, and how wrong is GMV if you don't?
3. Walk through the incremental branch of `fct_seller_orders`. A review answered three days ago arrives for an order purchased last month: trace it to the monthly fact. The same review arrives 17 months late: what happens, and what corrects it?
4. Why a 2-month lookback? What would 3 months buy, and what would it cost?
5. Why store numerators and denominators instead of rates?
6. How does the churn definition handle right-censoring?
7. Why `customer_unique_id` and not `customer_id` for `dim_customers`?

`PHASE 3 COMPLETE. Halting execution. Awaiting human code review and Ownership Checkpoint clearance before proceeding.`

---

## Phase 4: CI/CD (GitHub Actions)

**Objective.** No change reaches `main` without lint, Python tests and a dbt build of everything it touches, in an isolated schema, at minimal credit cost.
**Effort.** 2 days. **Entry.** Phase 3 cleared; marts built in prod schemas. **Exit.** Workflows green on a real PR; branch protection on; failure drills captured; ADR-015 and 016 recorded.

**4.1 Environments and credentials** (ADR-016)
- Targets: `dev` (personal schemas), `ci` (per-PR schemas) and `prod` (`main`).
- A CI service user with key-pair authentication and a role limited to reading RAW and creating and dropping CI schemas. The private key is stored as a GitHub Actions secret, never as a file in the repository.
- The CI job writes its own profile from environment variables at runtime, so the repository holds no credentials.
- Acceptance: the CI role fails when it tries to write to a prod schema.

**4.2 Pull-request build: `.github/workflows/dbt_ci.yml`** (ADR-015)
- Trigger: pull requests to `main` that touch `dbt/**`.
- Steps: checkout; Python 3.11 with a dependency cache; pinned install; `dbt deps`; download the latest prod manifest; `dbt build --select state:modified+ --defer --state <prod manifest>` into the PR schema, falling back to a full build when no manifest exists; upload `run_results.json` and logs.
- One concurrency group per PR so superseded runs are cancelled, plus a job timeout.
- Acceptance: a PR that touches one staging model builds only that model and its descendants; duration and credits are recorded.

**4.3 Python tests: `.github/workflows/pyspark_ci.yml`**
- Trigger: changes under `pyspark_ingestion/**`. Runs Java 17 (Temurin), Python 3.11 and pytest on fixtures, with no Snowflake access needed.
- Acceptance: the hazard tests from step 1.9 run on every relevant PR.

**4.4 Lint**
- SQLFluff (Snowflake dialect) on models; Ruff on Python. SQLFluff's Jinja templater keeps CI fast and offline, and the dbt compile inside step 4.2 catches the template errors it can't.
- Acceptance: lint failures block the PR.

**4.5 Deploy on merge: `.github/workflows/dbt_deploy.yml`**
- Trigger: push to `main`. Builds against prod, publishes `manifest.json` as the artifact slim CI defers to, and generates dbt docs and publishes them to GitHub Pages for the README's lineage link.
- The full refresh (ADR-013) is a manual-dispatch workflow while the source is static; a cron schedule would spend credits on unchanged data.
- Acceptance: a merge produces a manifest artifact and an updated docs site.

**4.6 CI schema cleanup**
- When a PR closes, drop its CI schemas through a dbt run-operation.
- Acceptance: no CI schema outlives its PR.

**4.7 Branch protection and PR template**
- `main` requires the dbt CI, PySpark CI and lint checks, and rejects direct and force pushes. The scaffolding commit is the only direct push.
- PR template checklist: grain stated, tests added, ADR linked, docs updated, evidence attached.
- Acceptance: a direct push to `main` is rejected.

**4.8 Failure drills** (interview evidence)
- Drill A: a PR injects an order item with an unknown `seller_id` into the CI schema. The relationship test errors and the PR is blocked.
- Drill B: a PR raises a known-anomaly count above baseline but below 2x. The build passes with a warning.
- Drill C: a PR breaks the churn rule. A unit test fails.
- Acceptance: the three runs are linked from `INTERVIEW_NOTES.md`.

**4.9 Phase documentation**
- `DECISIONS.md`: ADR-015 and 016. `FAILURE_MODES.md`: an expired key, a missing manifest, orphaned CI schemas. `CHANGELOG.md`: v0.4.0.

**Ownership checkpoint: Phase 4**
1. An order arrives with an unrecognized `seller_id`. Walk through what the pipeline does, step by step.
2. How do you decide whether a failing test warns or breaks the build?
3. What exactly does `state:modified+` select, and what does `--defer` do for a parent that wasn't rebuilt?
4. Where do the credentials live, and what happens when the key expires?
5. What does one PR cost in credits, and how do you know?

`PHASE 4 COMPLETE. Halting execution. Awaiting human code review and Ownership Checkpoint clearance before proceeding.`

---

## Phase 5: Documentation and interview preparation

**Objective.** A reviewer understands the problem, the design and the evidence in under two minutes, and the owner can defend any line in a technical interview.
**Effort.** 3 days. **Entry.** Phase 4 cleared. **Exit.** All documents complete; final checkpoint passed without notes; `v1.0.0` tagged.

**5.1 `README.md`**
- First screen: the problem, what was built, headline evidence (measured numbers only), an architecture image, and links to the dbt docs and CI status. Then a quickstart to reproduce everything end to end, the KPI table and a repository map.
- Acceptance: a reader new to the project can state the grain of the monthly fact and the SLA definition after one read.

**5.2 `ARCHITECTURE.md`**
- Lineage from CSV to marts; each layer's responsibility; why Snowflake separates storage and compute (independent scaling, per-second billing, no cost for a suspended warehouse); the role model; cost per full build and per PR.

**5.3 `DATA_DICTIONARY.md`**
- Finalized from YAML: a grain sentence, columns, types, nullability, and primary and foreign keys for every mart.

**5.4 `DECISIONS.md`**
- ADR-001 to 016, each with context, decision, alternatives, trade-offs and a 2–3 sentence interview defense that cites the number behind the decision.

**5.5 `FAILURE_MODES.md`**
- Detection, resolution and prevention for each failure found or drilled.

**5.6 `INTERVIEW_NOTES.md`**
- Sections: Snowflake architecture (warehouse sizing, pruning, spill, micro-partitions); dbt modeling (the DAG, ref/source, materializations, incremental logic); data quality and CI/CD; advanced SQL (window functions, deduplication, fan-out).
- A "numbers to know" table: 99,224 reviews vs. 104,720 lines; +5.02% fan-out; 99.52% lookback coverage; 473 predicted drift orders; 90.55% SLA; 14.54% ODR; 13.4% churn.
- Answers are written by the owner, not generated.

**5.7 Diagrams** (`docs/diagrams/`)
- The dbt lineage graph, a star-schema ERD, and a replay timeline showing which batch carries which events.

**5.8 `PROJECT_MAP.md` and `CHANGELOG.md`**
- A directory inventory with component ownership, and v1.0.0 release notes. Tag the release.

**5.9 Interview drills**
- A five-minute walkthrough: problem → architecture → one hard decision (the lookback) → evidence → what you would change.
- A live SQL defense: reproduce the fan-out bug and fix it, explain latest-version deduplication, and walk through the incremental branch.
- The scale scenario (from 100k to 100M rows a day). Points to prepare:
  - The full-refresh sweep breaks first; replace it with affected-key recompute from load watermarks.
  - `merge` target scans grow; answer with date clustering, tighter predicates, or microbatch by day.
  - Batch PUT + COPY gives way to Snowpipe or streaming ingestion.
  - Local Spark moves to a managed cluster.
  - Warehouse sizing: scale up for spill, multi-cluster for concurrency.
  - Spine growth: stop densifying N months after churn.

**5.10 Final ownership checkpoint**
- Answer every checkpoint question from Phases 1–4 without notes. A miss reopens the step it came from.

`PHASE 5 COMPLETE. Halting execution. Awaiting human code review and Ownership Checkpoint clearance before proceeding.`

---

## Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|:-:|:-:|---|
| Snowflake trial ends before Phase 4 | Medium | High | PySpark work done locally first, trial opened at step 1.11; every object defined as code; evidence exported as you go |
| Toolchain incompatibility (Java 19, Python 3.14) | High | Medium | Step 1.2 before any code |
| Silent CSV corruption (line breaks, BOM, leading zeros) | Medium | High | Fail-fast parsing; reconciliation against the frozen baseline (steps 1.10, 1.16) |
| Join fan-out inflates GMV | Medium | High | Pre-aggregation rule; row-count and GMV reconciliation tests (steps 3.3, 3.12) |
| Scope creep (dashboards, NLP, SCD Type 2) | High | Medium | Out-of-scope list in PROJECT_CONTEXT.md §5; any addition needs an ADR |
| Credentials committed | Low | High | Git-ignored profiles, a CI profile generated at runtime, key-pair auth, secret scanning |
| "Spark for 120 MB is over-engineering" critique | Medium | Medium | ADR-001 states the trade-off and the condition for removing Spark |
| Owner can't defend generated code | Medium | High | Ownership checkpoints; no phase advances on unexplained code |

## Effort baseline

| Phase | Focused days |
|---|:-:|
| 1. PySpark ELT and Snowflake ingestion | 4–5 |
| 2. dbt staging | 2–3 |
| 3. Intermediate and marts | 5–6 |
| 4. CI/CD | 2 |
| 5. Documentation and interview prep | 3 |
| **Total** | **16–19** |
