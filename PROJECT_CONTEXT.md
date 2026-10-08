# Project Context: Cartographer

**Marketplace Supply Analytics Engine: Seller Health Data Mart**

| | |
|---|---|
| **Domain** | E-commerce marketplace: partner (seller) economics and operations |
| **Deliverable** | Seller Health Data Mart: Kimball star schema in Snowflake, modeled and tested in dbt, fed by a PySpark → Parquet ingestion layer, gated by GitHub Actions |
| **Source** | Olist Brazilian E-Commerce public dataset: 9 CSV extracts, 1,550,922 rows, ~120 MB, orders purchased 2016-09-04 to 2018-10-17 |
| **Status** | Planning complete. KPI definitions in §4 are proposals pending owner sign-off (§9) |
| **Execution plan** | [EXECUTION_PLAYBOOK.md](EXECUTION_PLAYBOOK.md) |

All figures in this document were profiled from the source files on 2026-10-08. They are the baseline that every ingestion and reconciliation test checks against.

---

## 1. Business problem

A marketplace does not control its supply. GMV depends on ~3,000 independent sellers handing parcels to carriers on time, and on customers who rate the result. The signals a business usually sees first, lost GMV or a seller who has stopped selling, arrive after the damage is done.

The source data shows where to focus:

| Finding | Evidence |
|---|---|
| Late handoff predicts late delivery | Seller-orders handed to the carrier after the shipping deadline reached the customer late 20.7% of the time, vs. 5.2% when handed over on time (3.9x; 2017-01 to 2018-08) |
| Late delivery predicts defects | Orders delivered after the promised date received a 1–2★ review 62.5% of the time (3,986 of 6,381), vs. 9.3% when on time (8,324 of 89,443) (6.7x; lifetime) |
| Supply is concentrated | The top 10% of sellers (305 of 3,053) carry 67.4% of GMV; the top 100 carry 45.3%. Median lifetime seller GMV is R$825 |
| Attrition is steady | On the definition in §4.4, 13.4% of a month's active sellers make no sale in the following three months (range 10.5–19.1%). At 2018-08, 1,230 of the 3,029 sellers active in the window (40.6%) were churned or dormant |

These are associations, not causal estimates. They show where to look, not how much an intervention returns.

**Objective.** Give Supply Operations a monthly, seller-level view of fulfillment reliability, customer defects, GMV and churn risk. Every number must reproduce from source, pass automated tests, and trace back to source rows.

**Value logic.** The carrier-handoff SLA is the lead indicator a seller controls; defect rate and churn are lagging outcomes. Focusing account-management effort on SLA misses among high-GMV sellers covers most of the revenue at risk while touching a fraction of the seller base.

## 2. Business questions

The mart is done when each question is answerable with one query against the marts schema.

1. Which sellers missed the carrier-handoff SLA this month, and how much GMV do they carry?
2. Whose defect rate is rising, and does the rise coincide with late handoffs?
3. How many sellers churned this month, with what trailing GMV, in which categories and states?
4. Which high-GMV sellers are at risk (1–2 inactive months) and still recoverable?
5. How are GMV and active-seller counts trending by category and state?

## 3. Stakeholders

| Stakeholder | Decision supported | Cadence | Primary KPIs |
|---|---|---|---|
| Supply Operations (seller account management) | Which sellers to coach, warn or escalate; where account-manager hours go | Weekly drill-through, monthly scorecard | Fulfillment SLA, Order Defect Rate, churn-risk status |
| Category Managers | Where supply is thinning; where to recruit sellers | Monthly | GMV, active sellers, churn by category |
| Executive Leadership | Whether supply health is improving; concentration risk | Monthly, quarterly | GMV, active sellers, churn rate, SLA trend |
| Analytics Engineering (build owner) | Data contracts, test thresholds, platform cost | Per release | Test pass rate, reconciliation, incremental drift, credits |

## 4. KPI definitions (proposed)

### Common rules

- **Unit of fulfillment: the seller-order.** A seller-order is one seller's share of one customer order: 100,010 seller-orders from the 98,666 orders that have items. The shipping deadline (`shipping_limit_date`) is set per seller per order and is constant within every seller-order (0 exceptions). A three-item parcel is one fulfillment event, not three, so measuring per item would over-weight multi-item orders.
- **Time attribution: purchase month** (`order_purchase_timestamp`). A month's numbers stay provisional while its orders progress (§4.5).
- **Active seller-month:** at least one seller-order purchased in the month, excluding `canceled` and `unavailable` orders.
- **Analysis window: 2017-01 to 2018-08** (20 complete months). 2016 holds only 329 orders, with none at all in 2016-11; 2018-09 and 2018-10 hold 20. Fact tables keep every row; the window applies only to KPI reporting.
- **Rates are stored as numerator and denominator**, never as pre-computed percentages, so they aggregate correctly across sellers, categories and months.

### 4.1 Monthly GMV

| | |
|---|---|
| Definition | Sum of item `price` on a seller's seller-orders purchased in the month, excluding `canceled` and `unavailable` orders |
| Excludes | Freight (`freight_value`, R$2.25M lifetime). It is a logistics pass-through and is reported separately as Freight Value |
| Attribution | Each item carries its `seller_id`, so attribution is exact, including on the 1,278 multi-seller orders. No allocation is needed |
| Quantity | The source has no quantity column; quantity appears as repeated item rows (6,968 orders). Summing price already reflects it |
| Baseline | R$13,494,400.74 lifetime, after excluding R$97,242.96 of canceled/unavailable GMV (gross: R$13,591,643.70) |
| Why not `payment_value` | Payments are recorded per order, so they cannot be attributed to a seller. They also include installment interest and voucher splits: 380 orders (0.39%) do not reconcile to price + freight within R$0.01 |
| Edge case | A cancellation recorded after month close restates that month. The incremental lookback (§4.5) reprocesses it |

### 4.2 Seller Fulfillment SLA (carrier-handoff on-time rate)

| | |
|---|---|
| Definition | Share of a seller's seller-orders handed to the carrier on or before `shipping_limit_date` |
| Numerator | Seller-orders with `order_delivered_carrier_date` ≤ `shipping_limit_date` |
| Denominator | Seller-orders not `canceled`/`unavailable` that were handed over, plus seller-orders past their deadline with no handoff, which count as misses |
| Baseline (window) | 90.55% (89,705 of 99,069), including 599 overdue, unshipped seller-orders counted as misses |
| Why handoff, not delivery | The seller controls pick, pack and handoff; transit belongs to the carrier. Customer-facing punctuality is tracked separately as Delivery Promise Hit Rate (§4.6), a shared metric |
| Impossible sequences | 166 orders (176 seller-orders) record handoff before purchase. Excluded from numerator and denominator; monitored by a warn-level test |
| Sequence noise | 1,359 orders record handoff before payment approval. Approval time is not part of the formula, so they stay in; monitored |
| Shared handoff time | Handoff is recorded per order, so every seller on a multi-seller order (1.30% of orders, 1.98% of GMV) inherits the same timestamp. Accepted; the impact is bounded |
| Bad deadlines | 4 items carry a 2020 shipping deadline, up to 1,056 days after purchase. Treated as a source error; warn-level test, immaterial |

### 4.3 Order Defect Rate (ODR)

| | |
|---|---|
| Definition | Share of a seller's seller-orders whose order received a 1–2★ review |
| Numerator | Seller-orders where the worst review score on the order is ≤ 2 |
| Denominator | All seller-orders purchased in the month, any status. A canceled order with a 1★ review is still a customer-facing failure |
| Baseline (window) | 14.54% (14,496 of 99,693); 14.25% if canceled/unavailable orders are excluded |
| Multiple reviews | 547 orders carry 2–3 reviews, and 202 of them disagree on score. The worst score wins. Using the latest score instead would change the defect flag on 39 orders (0.04%), so the conservative rule costs almost nothing |
| Multi-seller orders | Reviews are order-level, so every seller on the order inherits the review. The source offers no basis for splitting it |
| Unreviewed | 758 seller-orders (0.76%) have no review and count as non-defective |
| Not a defect | Cancellation. Olist does not record who canceled, so Cancellation Rate (0.45% of seller-orders) is reported separately and not charged to the seller |

### 4.4 Seller Churn

| | |
|---|---|
| Active | At least one seller-order in the month, excluding `canceled`/`unavailable` |
| At risk | 1–2 consecutive inactive months |
| Churn event | The third consecutive inactive month after at least one active month. The seller's status is `churned` in that month, `dormant` after it, and `reactivated` on their next sale |
| Churn rate (month M) | Sellers with a churn event in M ÷ sellers active in M−3, the only cohort that can produce that event |
| Baseline | Mean 13.4% per month (2017-04 to 2018-08), range 10.5–19.1% |
| Threshold calibration | Share of inactivity spells that later ended in a sale: 54.9% after 1 month, 37.5% after 2, 25.9% after 3, 17.8% after 4, 7.7% after 6. These are lower bounds, because spells still open at 2018-08 count as non-returns. Three months trades earlier intervention against a one-in-four false-alarm rate |
| Right-censoring | The data ends at 2018-08. A seller whose last sale falls in 2018-06 to 2018-08 can only be `active` or `at_risk`; any later churn is unobserved, not absent |

### 4.5 Metric maturity and late-arriving data

An order's metrics keep changing until its last lifecycle event: approval, carrier handoff, delivery or review answer. The month that last event lands in, measured from the purchase month:

| Last event lands in | Cumulative share of orders |
|---|---:|
| Purchase month (M) | 51.80% |
| M+1 | 97.33% |
| M+2 | 99.52% |
| M+3 | 99.73% |
| Longest tail | 528 days (a review answered ~17 months after purchase) |

Implications:

- A month's SLA, ODR and churn status are provisional for two months. Each seller-month carries an `is_final` flag, set once two full months have closed after it.
- Incremental models reprocess the trailing three purchase months on every run, which catches 99.52% of late events. The remaining 473 orders (0.48%) are corrected by a scheduled full refresh. The trade-off is recorded as ADR-012 and ADR-013 in the playbook.

### 4.6 Supporting metrics

| Metric | Definition | Baseline |
|---|---|---|
| Delivery Promise Hit Rate | Delivered orders with delivery date ≤ estimated delivery date, compared at **date** grain | 93.23% (lifetime). The estimate is stored as midnight, so comparing raw timestamps reports 91.89% and misclassifies 1,292 on-time orders as late |
| Cancellation Rate | Canceled seller-orders ÷ seller-orders | 0.45% (448 of 99,693, window) |
| Freight Value | Sum of `freight_value` | R$2,251,909.54 lifetime |
| Health tier | Red / Amber / Green from trailing-three-month SLA and ODR | Thresholds set by the owner after the first build. Assigned only at ≥10 seller-orders in the trailing three months; below that, `insufficient_volume` |

Why the volume floor: the median active seller-month has 2 seller-orders, so a single bad review reads as a 50% defect rate. A floor of 10 seller-orders over three months classifies 36.9% of active seller-months, which carry 72.6% of GMV, and avoids publishing rates that are mostly noise.

## 5. Scope

### In scope (v1)

| Layer | Scope |
|---|---|
| Ingestion | All 9 files → typed Parquet (PySpark) → Snowflake internal stage → RAW via COPY INTO, reconciled to source row counts and money totals |
| Late-data simulation | Monthly as-of replay batches derived from lifecycle timestamps, so incremental models process real status changes and late reviews (ADR-003) |
| Staging | 8 dbt views, one per source except geolocation: renames, casts, typo fixes, latest version per key |
| Intermediate | Reviews rolled up to order grain; items enriched; items aggregated to seller-orders; seller-month spine |
| Marts | 4 conformed dimensions (date, seller, product, customer); 3 facts (order items, seller orders, seller monthly performance) |
| Quality | Generic, singular and unit tests; reconciliation tests; a documented severity policy (error vs. warn) |
| CI/CD | PR checks (lint, PySpark tests, slim dbt build in an isolated schema); deploy on merge; published dbt docs |
| Documentation | README, ARCHITECTURE, DECISIONS (ADRs), DATA_DICTIONARY, FAILURE_MODES, INTERVIEW_NOTES, CHANGELOG, PROJECT_MAP |

### Out of scope (deferred)

| Item | Reason | Revisit when |
|---|---|---|
| Geolocation modeling (seller-to-customer distance) | 1,000,163 rows, 26.2% exact duplicates, up to 1,146 coordinates per zip prefix, and no v1 KPI needs distance. Landed in RAW only | v2: distance as a driver of late delivery |
| SCD Type 2 seller history | The source is a single snapshot with no attribute history, so Type 2 rows would be fabricated | A source with change history exists |
| Streaming ingestion (Snowpipe, Kafka) | Decisions run weekly or monthly; no latency requirement justifies the cost | Daily SLA alerting becomes a requirement |
| BI dashboard | The mart is the product; any BI tool can sit on top of it | KPI definitions are signed off |
| Review text analytics | 40,977 Portuguese comments; a separate skill set and scope | v2 |
| Payment analytics | Order-level with no seller attribution; used only for reconciliation | A finance use case appears |
| Orchestrator (Airflow, Dagster) | Static source; GitHub Actions covers build and deploy | A recurring source feed exists |
| Churn prediction model | Needs the labeled history this mart creates first | v2, built on `fct_seller_monthly_performance` |

## 6. Source data profile

| File | Rows | Natural key | Note |
|---|---:|---|---|
| `olist_orders_dataset.csv` | 99,441 | `order_id` | 8 statuses; 96,478 delivered |
| `olist_order_items_dataset.csv` | 112,650 | `order_id`, `order_item_id` | 0 duplicate keys, 0 orphans |
| `olist_order_reviews_dataset.csv` | 99,224 | `review_id`, `order_id` | `review_id` alone is not unique |
| `olist_order_payments_dataset.csv` | 103,886 | `order_id`, `payment_sequential` | 1 order has no payment |
| `olist_customers_dataset.csv` | 99,441 | `customer_id` | One `customer_id` per order; 96,096 distinct people |
| `olist_sellers_dataset.csv` | 3,095 | `seller_id` | 23 states; 1,849 sellers in SP |
| `olist_products_dataset.csv` | 32,951 | `product_id` | 73 categories; 610 products uncategorized |
| `product_category_name_translation.csv` | 71 | `product_category_name` | 2 categories missing |
| `olist_geolocation_dataset.csv` | 1,000,163 | none | 19,015 zip prefixes |

Reconciliation totals: Σ`price` R$13,591,643.70 · Σ`freight_value` R$2,251,909.54 · Σ`payment_value` R$16,008,872.12.

### Findings that shape the design

| # | Finding | Evidence | Design response |
|---|---|---|---|
| F1 | Line breaks inside review comments | 104,720 physical lines but 99,224 records; 3,852 comments contain line breaks; CRLF line endings; quoting is inconsistent within files | Quote-aware, multi-line parsing; acceptance requires exactly 99,224 rows |
| F2 | UTF-8 BOM on the translation file | Without BOM handling the first header reads `﻿product_category_name`; the file also uses CRLF and has no trailing newline | BOM-safe read; dedicated unit-test fixture |
| F3 | Leading zeros in zip prefixes | 1,027 seller prefixes (33.2%) and 23,995 customer prefixes (24.1%) start with 0 | Explicit string types; schema inference banned |
| F4 | Non-ASCII text | 22,658 review comments and 73,442 geolocation city values | UTF-8 end to end |
| F5 | `review_id` is not a key | 789 `review_id`s span 2+ orders (1,603 rows); 547 orders have 2–3 reviews, 202 of them with conflicting scores | Key = (`review_id`, `order_id`); roll up to order grain before any join |
| F6 | Naive joins inflate GMV | Joining items to payments overstates GMV by 4.54%; items × reviews × payments by 5.02% (R$682,056) | Aggregate every 1:N child to its parent's grain before joining; payments excluded from GMV |
| F7 | `customer_id` is order-scoped | 99,441 `customer_id`s for 99,441 orders; 96,096 `customer_unique_id`s; 2,997 repeat buyers (3.1%) | `dim_customers` at `customer_unique_id` grain |
| F8 | Orders without items | 775 orders (603 unavailable, 164 canceled, 8 other) have no items and therefore no seller | Excluded from seller metrics; tracked by a data-quality test |
| F9 | Impossible event sequences | Handoff before purchase: 166 orders. Handoff before approval: 1,359. Delivery before handoff: 23. Review created before purchase: 74 | Warn-level tests with baseline thresholds; SLA exclusions per §4.2 |
| F10 | Date-only promise field | `order_estimated_delivery_date` is always 00:00:00 | Lateness compared at date grain (a 1,292-order difference) |
| F11 | Stale in-transit orders | 1,107 orders are still `shipped`; 1,001 of them were purchased more than 60 days before 2018-09-01, while P99 purchase-to-delivery time is 46 days | Flagged `stuck_in_transit` after 60 days |
| F12 | Dirty attributes and typos | Columns `product_name_lenght` and `product_description_lenght`; 17 malformed `seller_city` values (e.g. `04482255`, `sp / sp`); 2 untranslated categories covering 13 products | Renames in staging; `seller_state` as the geography of record; seed file for the 2 missing translations |
| F13 | No change history | One snapshot with no `updated_at`; lifecycle timestamps imply state over time | An as-of replay generates realistic incremental batches |
| F14 | No duplicate order items | 0 duplicate (`order_id`, `order_item_id`) keys and 0 exact duplicate rows | The uniqueness test stays as a guard. No dedup logic is written for a problem the source doesn't have; replay versions are handled in staging |

## 7. Constraints and assumptions

| Constraint | Implication |
|---|---|
| Static extract: no CDC, no `updated_at` | Incremental behavior is exercised through replay batches (ADR-003) |
| Timestamps carry no time zone | Assumed to be Brazil local time; stored as `TIMESTAMP_NTZ`; no conversion |
| Single currency (BRL) | No FX logic |
| Snowflake trial credits are time-boxed | Build the PySpark layer locally first and open the trial at Phase 1C. X-Small warehouse, 60-second auto-suspend, resource-monitor hard cap |
| Local toolchain is Python 3.14 and Java 19 | Pin Python 3.11 and Java 17 for the project; Java 19 is outside Spark's supported versions |
| Raw data stays out of Git | 120 MB of CSVs, including one 58 MB file above GitHub's 50 MB warning limit. `Datasets/` is git-ignored, and the README documents the download |

## 8. Success criteria

| Criterion | Measure |
|---|---|
| Ingestion fidelity | RAW row counts equal §6 for all 9 files; price, freight and payment totals match to R$0.01 |
| Grain integrity | Every mart declares its grain and enforces it with a uniqueness test; 0 failures on `main` |
| Referential integrity | Every fact foreign key resolves to a dimension row |
| GMV reconciliation | Mart GMV equals staging GMV for the window to R$0.01 |
| Incremental correctness | After the full replay, drift between incremental and full-refresh builds is measured against the predicted 473 orders, and a full refresh restores zero differences |
| CI | Every PR to `main` runs lint, PySpark tests and a slim dbt build in an isolated schema; `main` is branch-protected |
| Cost | Total spend stays under the resource-monitor cap; no query spills to remote storage |
| Defensibility | Every KPI maps to a documented column; every design choice has an ADR with at least one rejected alternative |

## 9. Decisions for owner sign-off (before Phase 3)

| ID | Decision | Recommendation | What it moves |
|---|---|---|---|
| D1 | GMV basis | Net of canceled/unavailable; freight excluded; purchase-month recognition | R$97,243 vs. gross |
| D2 | Overdue, unshipped seller-orders in the SLA | Count as misses | 599 seller-orders. Without this, a seller who never ships scores 100% |
| D3 | Multiple reviews per order | Worst score | 39 orders vs. latest score |
| D4 | ODR denominator | All statuses | 14.54% vs. 14.25% |
| D5 | Churn threshold | 3 consecutive inactive months | False-alarm rate: 37.5% at 2 months, 25.9% at 3, 17.8% at 4 |
| D6 | Analysis window | 2017-01 to 2018-08 | Excludes 349 orders (0.35%) |
| D7 | Health-tier volume floor | ≥10 seller-orders over the trailing three months | Classifies 36.9% of active seller-months, covering 72.6% of GMV |
| D8 | Health-tier thresholds | Set from the first build's distribution | Size of the Red list Supply Ops works each month |

## 10. Transferability

The model carries over to any two-sided marketplace whose supply partners have a fulfillment obligation: sellers shipping on time, hotels honoring confirmed bookings, restaurants meeting prep times. The partner-order and partner-month grains, numerator/denominator storage and late-data handling all apply unchanged.
