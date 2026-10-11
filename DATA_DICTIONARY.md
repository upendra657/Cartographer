# Cartographer Data Dictionary: Marts Layer

**Version**: `v1.0.0`  
**Target Warehouse**: Snowflake (`CARTOGRAPHER_DB.MARTS` / `STAGING`)  
**Specification Reference**: [`PROJECT_CONTEXT.md` §4](file:///Users/sharmaupendra657/Desktop/Projects/Cartographer/PROJECT_CONTEXT.md) & [`EXECUTION_PLAYBOOK.md` Phase 3](file:///Users/sharmaupendra657/Desktop/Projects/Cartographer/EXECUTION_PLAYBOOK.md)

---

## 1. Dimensional Models (`models/marts/core/`)

### 1.1 `dim_date`
* **Description**: Daily calendar dimension supporting historical cohorting, day-of-week seasonality, and business day calculations.
* **Grain**: One row per calendar day (`date_day`).
* **Row Count**: 852 rows (2016-09-01 to 2018-12-31).

| Column Name | Data Type | Nullable | Key Type | Description / Logic |
|---|---|---|---|---|
| `date_key` | VARCHAR(32) | NO | PK | `MD5(date_day::VARCHAR)` surrogate primary key |
| `date_day` | DATE | NO | Natural | Calendar date |
| `year_number` | NUMBER | NO | Attribute | Four-digit calendar year (e.g., 2017) |
| `quarter_number` | NUMBER | NO | Attribute | Quarter (1–4) |
| `month_number` | NUMBER | NO | Attribute | Month (1–12) |
| `month_name` | VARCHAR | NO | Attribute | Month name abbreviation ('Jan' through 'Dec') |
| `year_month` | VARCHAR(7) | NO | Partition | 'YYYY-MM' format |
| `day_of_month` | NUMBER | NO | Attribute | Day of month (1–31) |
| `day_of_week` | NUMBER | NO | Attribute | Day of week index |
| `day_name` | VARCHAR | NO | Attribute | Three-letter day abbreviation ('Mon'–'Sun') |
| `is_weekend` | BOOLEAN | NO | Indicator | TRUE for Saturday/Sunday |
| `is_month_end` | BOOLEAN | NO | Indicator | TRUE if last day of the calendar month |

---

### 1.2 `dim_sellers`
* **Description**: Conformed partner dimension capturing seller operational location and platform tenure.
* **Grain**: One row per distinct seller (`seller_id`).
* **Row Count**: 3,095 rows.

| Column Name | Data Type | Nullable | Key Type | Description / Logic |
|---|---|---|---|---|
| `seller_key` | VARCHAR(32) | NO | PK | `MD5(seller_id)` surrogate primary key |
| `seller_id` | VARCHAR(32) | NO | Natural | Raw alphanumeric seller UUID |
| `seller_zip_code_prefix`| VARCHAR(5) | NO | Attribute | Sanitized 5-digit Brazilian CEP postal code |
| `seller_city` | VARCHAR | YES | Attribute | Cleaned Portuguese city name |
| `seller_state` | VARCHAR(2) | YES | Attribute | 2-letter Brazilian state code (UF) |
| `first_active_month` | DATE | YES | Attribute | Month of first active (non-canceled) order |
| `last_active_month` | DATE | YES | Attribute | Month of most recent active order |
| `is_tenured_seller` | BOOLEAN | NO | Indicator | TRUE if tenure (`last_active_month - first_active_month`) ≥ 6 months |

---

### 1.3 `dim_products`
* **Description**: Product catalog dimension enriched with English translations and physical package specs.
* **Grain**: One row per distinct product (`product_id`).
* **Row Count**: 32,951 rows.

| Column Name | Data Type | Nullable | Key Type | Description / Logic |
|---|---|---|---|---|
| `product_key` | VARCHAR(32) | NO | PK | `MD5(product_id)` surrogate primary key |
| `product_id` | VARCHAR(32) | NO | Natural | Raw alphanumeric product UUID |
| `product_category_name`| VARCHAR | YES | Natural | Original Portuguese category name |
| `product_category_name_english` | VARCHAR | YES | Attribute | English translation with manual seed fallback (F12) |
| `product_name_length` | NUMBER | YES | Attribute | Character count of product title |
| `product_description_length` | NUMBER | YES | Attribute | Character count of product listing body |
| `product_photos_qty` | NUMBER | YES | Attribute | Number of published listing images |
| `product_weight_g` | NUMBER | YES | Metric | Physical shipping weight in grams |
| `product_volume_cm3` | NUMBER | YES | Metric | Calculated volumetric size (`length * height * width`) in cm³ |

---

### 1.4 `dim_customers`
* **Description**: Conformed customer master entity resolved at the individual person grain (`customer_unique_id`), breaking the transaction transaction token anti-pattern.
* **Grain**: One row per real-world unique customer (`customer_unique_id`).
* **Row Count**: 96,096 rows.

| Column Name | Data Type | Nullable | Key Type | Description / Logic |
|---|---|---|---|---|
| `customer_key` | VARCHAR(32) | NO | PK | `MD5(customer_unique_id)` surrogate primary key |
| `customer_unique_id`| VARCHAR(32) | NO | Natural | Persistent real-world customer UUID |
| `customer_zip_code_prefix` | VARCHAR(5) | YES | Attribute | Customer primary CEP code |
| `customer_city` | VARCHAR | YES | Attribute | Sanitized customer city |
| `customer_state` | VARCHAR(2) | YES | Attribute | 2-letter customer state code (UF) |
| `lifetime_orders_count` | NUMBER | NO | Metric | Total distinct orders placed by person |
| `first_order_timestamp` | TIMESTAMP_NTZ | YES | Attribute | Earliest purchase timestamp |
| `last_order_timestamp` | TIMESTAMP_NTZ | YES | Attribute | Latest purchase timestamp |
| `is_repeat_buyer` | BOOLEAN | NO | Indicator | TRUE if `lifetime_orders_count > 1` |

---

## 2. Fact Models (`models/marts/core/`)

### 2.1 `fct_order_items` (Transaction Fact)
* **Description**: Atomic transaction fact capturing item-level marketplace economics. Joins 1:1 against order items to prevent join fan-out.
* **Grain**: One row per line item within an order (`order_id`, `order_item_id`).
* **Row Count**: 112,650 rows.
* **Incremental Strategy**: Merge lookback (`order_purchase_timestamp >= DATEADD('month', -2, current_max)`).
* **KPI Rules Reference**: [`PROJECT_CONTEXT.md` §4.1 (GMV & Lifetime Financials)](file:///Users/sharmaupendra657/Desktop/Projects/Cartographer/PROJECT_CONTEXT.md#41-gross-merchandise-value-gmv)

| Column Name | Data Type | Nullable | Key Type | Description / Logic |
|---|---|---|---|---|
| `order_item_key` | VARCHAR(32) | NO | PK | `MD5(order_id \|\| '-' \|\| order_item_id)` |
| `order_id` | VARCHAR(32) | NO | Degenerate | Natural order UUID |
| `order_item_id` | NUMBER | NO | Natural | Line item sequential index (1, 2, 3...) |
| `purchase_date_key` | VARCHAR(32) | NO | FK | Foreign key to `dim_date.date_key` |
| `seller_key` | VARCHAR(32) | NO | FK | Foreign key to `dim_sellers.seller_key` |
| `product_key` | VARCHAR(32) | NO | FK | Foreign key to `dim_products.product_key` |
| `customer_key` | VARCHAR(32) | NO | FK | Foreign key to `dim_customers.customer_key` |
| `order_purchase_timestamp` | TIMESTAMP_NTZ | NO | Partition | Exact order placement timestamp |
| `shipping_limit_date` | TIMESTAMP_NTZ | NO | Milestone | SLA merchant handoff deadline |
| `price` | NUMBER(12, 2) | NO | Measure | Gross merchandise value (GMV). Total: R$ 13,591,643.70 |
| `freight_value` | NUMBER(12, 2) | NO | Measure | Logistics pass-through freight fee. Total: R$ 2,251,909.54 |
| `total_item_value` | NUMBER(12, 2) | NO | Measure | Total buyer charge (`price + freight_value`) |
| `order_status` | VARCHAR | NO | Degenerate | Current order state ('delivered', 'shipped', 'canceled', etc.) |
| `is_canceled` | BOOLEAN | NO | Indicator | TRUE if `order_status = 'canceled'` |
| `is_unavailable` | BOOLEAN | NO | Indicator | TRUE if `order_status = 'unavailable'` |

---

### 2.2 `fct_seller_orders` (Accumulating Snapshot Fact)
* **Description**: Accumulating snapshot tracking operational milestones, delivery lags, fulfillment SLA compliance, and customer satisfaction at the partner-order grain.
* **Grain**: One row per seller per order (`seller_id`, `order_id`).
* **Row Count**: 100,010 rows.
* **Incremental Strategy**: Merge lookback (`order_purchase_timestamp >= DATEADD('month', -2, current_max)`).
* **KPI Rules Reference**: [`PROJECT_CONTEXT.md` §4.2 (SLA)](file:///Users/sharmaupendra657/Desktop/Projects/Cartographer/PROJECT_CONTEXT.md#42-seller-fulfillment-sla-carrier-handoff-on-time-rate) & [`PROJECT_CONTEXT.md` §4.3 (ODR)](file:///Users/sharmaupendra657/Desktop/Projects/Cartographer/PROJECT_CONTEXT.md#43-order-defect-rate-odr)

| Column Name | Data Type | Nullable | Key Type | Description / Logic |
|---|---|---|---|---|
| `seller_order_key` | VARCHAR(32) | NO | PK | `MD5(seller_id \|\| '-' \|\| order_id)` |
| `order_id` | VARCHAR(32) | NO | Degenerate | Natural order UUID |
| `seller_id` | VARCHAR(32) | NO | FK | Natural seller UUID |
| `seller_key` | VARCHAR(32) | NO | FK | Foreign key to `dim_sellers.seller_key` |
| `purchase_date_key` | VARCHAR(32) | NO | FK | Foreign key to `dim_date.date_key` |
| `order_purchase_timestamp` | TIMESTAMP_NTZ | NO | Milestone | Order placement timestamp |
| `order_approved_at` | TIMESTAMP_NTZ | YES | Milestone | Payment authorization timestamp |
| `order_delivered_carrier_date` | TIMESTAMP_NTZ | YES | Milestone | Carrier physical scan timestamp |
| `order_delivered_customer_date` | TIMESTAMP_NTZ | YES | Milestone | Customer receipt timestamp |
| `order_estimated_delivery_date` | TIMESTAMP_NTZ | NO | Milestone | Customer promise deadline |
| `shipping_limit_date` | TIMESTAMP_NTZ | NO | Milestone | Strict fulfillment SLA deadline |
| `review_answer_timestamp` | TIMESTAMP_NTZ | YES | Milestone | Date/time review was submitted |
| `item_count` | NUMBER | NO | Metric | Quantity of items fulfilled by this seller on order |
| `seller_order_gmv` | NUMBER(12, 2) | NO | Metric | Seller-attributed item revenue |
| `seller_order_freight` | NUMBER(12, 2) | NO | Metric | Seller-attributed shipping fee |
| `hours_to_carrier` | NUMBER(10, 2) | YES | Lag | Decimal hours from purchase to carrier scan |
| `days_to_delivery` | NUMBER(10, 2) | YES | Lag | Decimal days from purchase to customer delivery |
| `delivery_delay_days` | NUMBER(10, 2) | YES | Lag | Days delivered past promise (`customer_date - estimated_date`) |
| `is_sla_eligible` | BOOLEAN | NO | Flag | Handed over or deadline passed; excludes canceled & impossible sequences |
| `is_sla_hit` | BOOLEAN | NO | Flag | TRUE if `carrier_date <= shipping_limit_date` |
| `is_delivered_late` | BOOLEAN | NO | Flag | Evaluated at calendar DATE grain (ADR-004) |
| `is_order_defect` | BOOLEAN | NO | Flag | TRUE if worst order review score ≤ 2★ |
| `worst_review_score` | NUMBER | YES | Metric | Minimum review score given on the order |
| `is_canceled` | BOOLEAN | NO | Flag | TRUE if `order_status = 'canceled'` |
| `is_unavailable` | BOOLEAN | NO | Flag | TRUE if `order_status = 'unavailable'` |

---

### 2.3 `fct_seller_monthly_performance` (Periodic Snapshot Fact)
* **Description**: Longitudinal monthly snapshot tracking partner activity, churn transitions, fulfillment rates, and executive health tiers. Built on a dense Cartesian spine.
* **Grain**: One row per seller per calendar month (`seller_id`, `month`).
* **Row Count**: 33,484 rows (spanned from each seller's first active sale through 2018-08-01).
* **Incremental Strategy**: Re-evaluated over dense calendar periods.
* **KPI Rules Reference**: [`PROJECT_CONTEXT.md` §4.4 (Seller Churn)](file:///Users/sharmaupendra657/Desktop/Projects/Cartographer/PROJECT_CONTEXT.md#44-seller-churn) & [`PROJECT_CONTEXT.md` §4.6 (Health Tiers)](file:///Users/sharmaupendra657/Desktop/Projects/Cartographer/PROJECT_CONTEXT.md#46-supporting-metrics)

| Column Name | Data Type | Nullable | Key Type | Description / Logic |
|---|---|---|---|---|
| `seller_monthly_key` | VARCHAR(32) | NO | PK | `MD5(seller_id \|\| '-' \|\| month::VARCHAR)` |
| `seller_id` | VARCHAR(32) | NO | FK | Natural seller UUID |
| `seller_key` | VARCHAR(32) | NO | FK | Foreign key to `dim_sellers.seller_key` |
| `month` | DATE | NO | Natural | First day of reporting month ('YYYY-MM-01') |
| `seller_orders_count` | NUMBER | NO | Metric | Count of seller-orders received in month |
| `items_count` | NUMBER | NO | Metric | Total item units sold |
| `gmv` | NUMBER(12, 2) | NO | Metric | Realized monthly GMV (excludes cancellations) |
| `freight` | NUMBER(12, 2) | NO | Metric | Realized monthly freight pass-through |
| `sla_hits` | NUMBER | NO | Numerator | Number of on-time carrier handoffs |
| `sla_eligible` | NUMBER | NO | Denominator | Eligible fulfillment orders |
| `defects_count` | NUMBER | NO | Numerator | Orders with worst review score ≤ 2★ |
| `defect_eligible` | NUMBER | NO | Denominator | All received orders in month |
| `is_active_month` | BOOLEAN | NO | Indicator | TRUE if `seller_orders_count > 0` |
| `consecutive_inactive_months` | NUMBER | NO | Metric | Counter tracking consecutive inactive months |
| `seller_lifecycle_status` | VARCHAR | NO | State | State machine value: `new`, `active`, `at_risk`, `churned`, `dormant`, `reactivated` |
| `rolling_3m_seller_orders` | NUMBER | NO | Window Metric | Trailing 3-month volume window |
| `rolling_3m_sla_rate` | NUMBER(6, 4) | YES | Window Metric | Trailing 3-month fulfillment SLA rate |
| `rolling_3m_defect_rate` | NUMBER(6, 4) | YES | Window Metric | Trailing 3-month Order Defect Rate (ODR) |
| `health_tier` | VARCHAR | NO | Classification | `Green` (SLA ≥90% & ODR ≤10%), `Amber`, `Red`, or `insufficient_volume` (<10 orders) |
| `is_final` | BOOLEAN | NO | Indicator | TRUE if reporting month is fully mature (closed + 2 calendar months) |
