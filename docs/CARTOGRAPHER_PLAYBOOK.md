# The Cartographer Playbook: An ELI5 Guide to Modern Data Engineering

> *"If you can't explain it to a six-year-old, you don't understand it yourself."* — Albert Einstein

Welcome to **Cartographer**. If you are new to data engineering, data warehouses, or business intelligence, modern tech stacks can feel like an intimidating bowl of alphabet soup: *PySpark, Snappy Parquet, Snowflake, Medallion Architecture, dbt, Kimball Modeling, CI/CD, Streamlit*.

To make every concept crystal clear, we are going to tell a story. We will explain this entire world-class data system using one consistent, real-world analogy: **A Three-Michelin-Star Restaurant Kitchen**.

---

## Table of Contents
1. [The Big Picture: Chaos vs. Haute Cuisine](#1-the-big-picture-chaos-vs-haute-cuisine)
2. [Phase 1: The Delivery Truck (PySpark & Snowflake RAW)](#2-phase-1-the-delivery-truck-pyspark--snowflake-raw)
3. [Phase 2: Washing the Vegetables (Staging & Deduplication)](#3-phase-2-washing-the-vegetables-staging--deduplication)
4. [Phase 3: The Secret Cookbook (Kimball Dimensional Modeling)](#4-phase-3-the-secret-cookbook-kimball-dimensional-modeling)
   - [Nouns vs. Verbs (Dimensions vs. Facts)](#nouns-vs-verbs-dimensions-vs-facts)
   - [The Three Fact Archetypes](#the-three-fact-archetypes)
5. [Phase 4: The Dining Room (Streamlit & RBAC Security)](#5-phase-4-the-dining-room-streamlit--rbac-security)
6. [Summary Kitchen Glossary](#6-summary-kitchen-glossary)

---

## 1. The Big Picture: Chaos vs. Haute Cuisine

Imagine you run an ultra-exclusive, Three-Michelin-Star restaurant named **Le Cartographer**.

Every evening, your dining room is filled with high-profile guests: the CEO, the Chief Operating Officer, and the VP of Logistics. They sit at linen-covered tables and ask simple, high-stakes questions:
- *"Are our food suppliers delivering on time?"*
- *"Which merchant partners are failing our quality standards?"*
- *"Are we making a profit, or are we quietly bleeding money?"*

If you answer these questions by pointing to a mountain of dirt-covered carrots, half-broken egg cartons, and scribbled delivery notes dumped in the back alley, you will be fired on the spot.

```
       RAW INGESTION                     PREP STATION                      THE DINING ROOM
 ┌──────────────────────┐          ┌──────────────────────┐          ┌──────────────────────┐
 │ Dirty Farm Delivery  │ ───────► │ Washed, Sliced, Prep │ ───────► │ Michelin-Star Dish   │
 │   (1.55M Raw CSVs)   │          │ (Deduplicated Views) │          │  (Streamlit Portal)  │
 └──────────────────────┘          └──────────────────────┘          └──────────────────────┘
     [ Back Alley ]                     [ The Kitchen ]                   [ The Executives ]
```

That chaotic back alley is **Raw E-Commerce Data**. When millions of people buy shoes, watches, and guitars online, their computers generate millions of messy transaction rows:
- Some reviews have broken Portuguese sentences with messy line breaks.
- Some merchants send duplicate notification pings.
- Some postal codes lose their leading zeros.
- Payment amounts can suffer from rounding errors.

Our job as **Analytics Engineers** is not just to store data. Our job is to take raw, chaotic farm deliveries, wash them, chop them, cook them using precise recipes, and serve a stunning, Michelin-star culinary masterpiece to the dining room—accurate to the single cent.

---

## 2. Phase 1: The Delivery Truck (PySpark & Snowflake RAW)

### The Analogy: Loose Veggies vs. Vacuum-Sealed Crates
Imagine the local farmers drive up to the back of the restaurant in a pickup truck.
- **The CSV Approach**: The farmers simply dump 10,000 loose, dirt-covered tomatoes, unbundled carrots, and leaking milk cartons directly into your kitchen floor. The tomatoes roll under the stove. The milk leaks into the floor drain. You don't know which tomato came from which farm, and counting them takes two hours of bending over with a clipboard.
- **The Parquet Approach**: The farmers vacuum-seal the vegetables in clear, sterile, temperature-controlled, barcoded plastic containers. Every box has an immutable label: *"Grade-A Roma Tomatoes, Exactly 50 Count, Picked at 06:00 AM, Weight: 5.00 kg"*.

```
   LOOSE CSVs (Messy & Row-Oriented)          SNAPPY PARQUET (Vacuum-Sealed & Columnar)
 ┌──────────────────────────────────┐        ┌──────────────────────────────────┐
 │ "01037","São Paulo","19.90"\n    │        │ [Metadata Header: Schema & Types]│
 │ "1037","RJ","NULL"\n             │  ───►  │ [Column 1: Zip (String)]         │
 │ "Broken line...                  │        │ [Column 2: State (Fixed Length)] │
 └──────────────────────────────────┘        │ [Column 3: Price (Decimal 12,2)] │
                                             └──────────────────────────────────┘
```

### What We Actually Built
1. **PySpark 3.5 Engine**:
   Before anything enters our cloud warehouse (Snowflake), our automated delivery forklift (**PySpark**) inspects every single file. We write an ironclad contract called a `StructType` schema. If a supplier tries to send us a text string where a financial decimal belongs, PySpark halts the forklift immediately (`FAILFAST`). We refuse to let contaminated ingredients into the building.
2. **Snappy Parquet Serialization**:
   We convert 122 megabytes of raw CSVs into **Snappy Parquet** files (shrinking them down to 60 MB—a 51% reduction!).
   - *Why Snappy?* It's like shrink-wrap. It compresses the ingredients so they take up half the shelf space, but it unzips in milliseconds without making the chef wait.
   - *Columnar Storage*: If the pastry chef only needs sugar, she doesn't have to unpack the entire truck of flour and onions. She grabs just the sugar crate. That is how Snowflake reads Parquet columns without scanning entire tables.
3. **The Walk-in Cooler (`CARTOGRAPHER_DB.RAW`)**:
   We slide these sealed crates into our restaurant's cold storage unit using an internal named stage (`@CARTOGRAPHER_STAGE`). The crates sit on the shelves untouched, in their pure, original form.

---

## 3. Phase 2: Washing the Vegetables (Staging & Deduplication)

### The Analogy: The Prep Station & The Bruised Tomatoes
Now our sous chefs grab the crates from cold storage and take them to the stainless-steel prep counter (**The Staging Layer**).

Here is a strange secret about e-commerce: **Orders change their status over time.**
When a customer buys a coffee maker on Monday, the system creates a record: *"Status: Purchased"*.
On Wednesday, the merchant hands it to the courier: *"Status: Shipped"*.
On Friday, it arrives at the customer's doorstep: *"Status: Delivered"*.
Two weeks later, the customer writes a review: *"Status: Reviewed"*.

Because our temporal replay engine simulates 22 months of operations, our delivery truck dropped off **136,549 order status snapshots**. But how many actual coffee makers were bought? Exactly **99,441 unique orders**.

If our chef served 136,549 meals instead of 99,441, the restaurant would cook 37,108 extra steaks and go bankrupt!

```
 RAW ORDERS INGESTED (136,549 Rows)               STAGING DEDUPLICATION (QUALIFY)
 ┌────────────────────────────────────────┐      ┌───────────────────────────────┐
 │ Order 101 | 2017-01-05 | Purchased     │      │                               │
 │ Order 101 | 2017-01-07 | Shipped       │ ───► │ Order 101 | Delivered (Latest)│
 │ Order 101 | 2017-01-10 | Delivered     │      │                               │
 └────────────────────────────────────────┘      └───────────────────────────────┘
                                                  [ Only 99,441 Unique Orders! ]
```

### The Magic of Snowflake `QUALIFY ROW_NUMBER()`
How does the prep cook keep only the freshest vegetable?
- In traditional cooking (legacy SQL), the cook puts all tomatoes in a giant bowl, writes numbers on sticky notes for every tomato, creates a second bowl, moves the highest-numbered tomato into the second bowl, and throws the first bowl away (this is an expensive **CTE Subquery**).
- In our modern kitchen, we use Snowflake's native **`QUALIFY ROW_NUMBER()`** clause:
  ```sql
  SELECT *
  FROM raw_orders
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY order_id 
      ORDER BY order_purchase_timestamp DESC, _loaded_at DESC
  ) = 1
  ```
  The prep cook inspects the tomatoes as they slide down the conveyor belt. The instant a fresher version of `Order 101` slides past, the older one is discarded automatically on the fly. No extra bowls, no wasted memory, no redundant storage.

---

## 4. Phase 3: The Secret Cookbook (Kimball Dimensional Modeling)

Now that the ingredients are washed, peeled, and prepped, it is time to cook. But in a Michelin-star kitchen, you don't just throw steak, chocolate sauce, and oysters into one giant blender. That would taste disgusting.

In data engineering, throwing all data into one giant table is called **The One-Big-Table (OBT) Trap**. It creates fan-out bugs and multiplies financial metrics. Instead, we use the legendary **Kimball Dimensional Modeling** technique.

### Nouns vs. Verbs (Dimensions vs. Facts)
Think of your data warehouse as basic English grammar:
- **Dimensions are the NOUNS (People, Places, Things)**:
  - *Who cooked it?* `dim_sellers` (The merchant partner, their tenure, their city).
  - *Who ate it?* `dim_customers` (The unique human customer who placed the order).
  - *What ingredient was used?* `dim_products` (The product category, weight, dimensions).
  - *When was it served?* `dim_date` (The day of the week, fiscal quarter, holiday).
- **Facts are the VERBS (Events, Actions, Measurements)**:
  - Facts capture numbers, dollars, seconds, and transactions that happened at a specific moment.

```
       DIMENSIONS (The Nouns)                           FACTS (The Verbs)
 ┌───────────────────────────────────┐        ┌───────────────────────────────────┐
 │ • dim_sellers   (The Merchant)    │        │ • fct_order_items (Sold an Item!) │
 │ • dim_customers (The Buyer)       │  ───►  │ • fct_seller_orders (Fulfilled!)  │
 │ • dim_products  (The Product)     │        │ • fct_seller_monthly (Evaluated!) │
 │ • dim_date      (The Calendar)    │        └───────────────────────────────────┘
 └───────────────────────────────────┘             [ Measurable Business Events ]
```

---

### The Three Fact Archetypes

To keep our kitchen running with mathematical precision, we built three distinct types of fact tables:

#### 1. The Transaction Fact: `fct_order_items` (The Ticket Prints)
- **The Kitchen Analogy**: A customer orders two ribeye steaks and a glass of wine. The printer in the kitchen goes *chzz-chzz-chzz* and prints out the order slip.
- **The Data Reality**: Every line on the receipt is an immutable, additive record:
  - Line 1: Ribeye Steak #1 $\to$ R$ 120.00
  - Line 2: Ribeye Steak #2 $\to$ R$ 120.00
  - Line 3: Cabernet Sauvignon $\to$ R$ 45.00
- **Why It Matters**: This table contains exactly **112,650 line items**. If you add up the `price` column, it equals **R$ 13,591,643.70** to the exact penny. It never changes, and it never duplicates.

#### 2. The Accumulating Snapshot Fact: `fct_seller_orders` (The Ticket Travels)
- **The Kitchen Analogy**: The order slip doesn't just sit on the printer. The head chef clips it to the order rail. As the dish progresses through the kitchen, the cooks stamp the ticket with times:
  - 7:02 PM: Ticket received at the grill.
  - 7:15 PM: Steaks seared to medium-rare.
  - 7:20 PM: Plated with roasted vegetables.
  - 7:22 PM: Food runner delivers the plate to Table 4.
  - 7:45 PM: Diner tells the waiter, *"That was delicious! 5 stars!"*
- **The Data Reality**: This fact table follows an order across multiple asynchronous milestones: `purchased` $\to$ `approved` $\to$ `carrier handoff` $\to$ `delivered` $\to$ `reviewed`.
- **Why It Matters**: It calculates operational latency: *How many hours did it take the merchant to hand the box to the carrier (`hours_to_carrier`)? Did it arrive before the promised delivery date (`is_on_time_delivery`)? Did the customer leave a 1-star review (`is_order_defect`)?*

#### 3. The Periodic Snapshot Fact: `fct_seller_monthly_performance` (The Chef's Monthly Review)
- **The Kitchen Analogy**: At the end of every month, the Executive Chef sits down in her office with a cup of coffee and evaluates each line cook's monthly performance:
  - *Did line cook Marco cook consistently, or did he take three weeks off?*
  - *What was Marco's on-time rate this month?*
  - *How many steaks did customers send back to the kitchen?*
- **The Data Reality**: This table samples seller performance at uniform, monthly intervals across a dense calendar spine of **33,484 seller-months**.
- **The Churn State Machine**: It tracks the life cycle of every merchant:
  - Month 1: First sale ever $\to$ `new`
  - Month 2: Selling actively $\to$ `active`
  - Month 3: Zero sales for 30 days $\to$ `at_risk`
  - Month 5: Zero sales for 90 days $\to$ `churned`
  - Month 8: Sells a product again $\to$ `reactivated`!

---

## 5. Phase 4: The Dining Room (Streamlit & RBAC Security)

### The Analogy: The Waiter vs. The Walk-in Freezer
The dishes are cooked, seasoned, and plated. Now, how do we serve them to the dining room?

Imagine if a waiter walked a guest into the kitchen, opened the raw meat freezer, handed them a butcher knife, and said: *"Go ahead, slice off whatever you want and figure out what it costs."* That restaurant would be shut down by the health department in five minutes!

In modern data architecture, this is called **Role-Based Access Control (RBAC)**.

```
 [ Executive / Waiter ]           [ Sous Chef / dbt Core ]          [ Delivery Supplier ]
          │                                  │                                │
          ▼                                  ▼                                ▼
    ROLE: REPORTER                   ROLE: TRANSFORMER                   ROLE: LOADER
          │                                  │                                │
          ▼                                  ▼                                ▼
 ┌───────────────────┐              ┌───────────────────┐            ┌───────────────────┐
 │ The Dining Room   │              │ The Prep Kitchen  │            │ The Loading Dock  │
 │  (MARTS Views)    │              │ (STAGING & MARTS) │            │   (RAW Tables)    │
 └───────────────────┘              └───────────────────┘            └───────────────────┘
   [ Clean & Plated ]                [ Prep & Cooking ]               [ Frozen & Raw ]
```

### How RBAC Works in Cartographer
We created three distinct culinary roles in Snowflake:
1. **`ROLE LOADER` (The Delivery Driver)**:
   - Allowed only at the loading dock (`CARTOGRAPHER_DB.RAW`).
   - Can drop off crates of Parquet files, but is forbidden from stepping into the kitchen or touching the prepared food.
2. **`ROLE TRANSFORMER` (The Sous Chef / dbt Core)**:
   - Allowed in the raw storage area to read ingredients, and has full control of the prep counter (`STAGING`) and cooking stoves (`MARTS`).
   - Can chop, boil, simmer, and plate dishes.
3. **`ROLE REPORTER` (The Waiter / Streamlit Application)**:
   - Allowed only in the dining room (`CARTOGRAPHER_DB.MARTS`).
   - Can carry the finished plates (`rpt_seller_health_scorecard`, `rpt_executive_kpis_monthly`) to the executive guests.
   - **Strictly locked out of the raw freezer**: If the waiter tries to peek into `RAW.RAW_ORDERS` (which contains unmasked customer home addresses and raw transaction IDs), Snowflake slaps their hand with an error:
     `SQL compilation error: Object 'RAW.RAW_ORDERS' does not exist or not authorized.`

---

## 6. Summary Kitchen Glossary

| Data Engineering Term | The Kitchen Analogy | What It Actually Does |
| :--- | :--- | :--- |
| **Raw CSV Datasets** | Dirty, loose farm produce dumped in the alley. | Unvalidated text files prone to formatting bugs and floating-point drift. |
| **PySpark StructType** | The strict supplier contract. | Enforces data types before data ever touches cloud infrastructure. |
| **Snappy Parquet** | Vacuum-sealed, labeled plastic crates. | Columnar binary format that cuts storage by 51% and accelerates queries. |
| **Snowflake RAW** | The walk-in cold storage unit. | Immutable, append-only landing zone for raw historical records. |
| **Staging Layer** | The stainless-steel prep station. | Cleans, renames, and types columns without storing duplicate physical data. |
| **`QUALIFY ROW_NUMBER()`**| Discarding wilted lettuce on the conveyor belt. | Deduplicates streaming and replay batches directly inside window calculations. |
| **Kimball Dimension** | The ingredients list & chef profiles (Nouns). | Contextual entities (`dim_sellers`, `dim_customers`, `dim_products`). |
| **Transaction Fact** | The printed receipt slip. | Line-item additive financial events (`fct_order_items`, R$ 13.59M GMV). |
| **Accumulating Snapshot** | The order ticket stamped at each cooking station. | Tracks fulfillment latency milestones (`hours_to_carrier`, `days_to_delivery`). |
| **Periodic Snapshot** | The Head Chef's monthly line-cook evaluation. | Samples retention, churn state machines, and SLA compliance over time. |
| **Streamlit BI App** | The beautifully lit, linen-table dining room. | Interactive 3-tab visual portal delivering executive supply intelligence. |
| **Role `REPORTER`** | The front-of-house waiter. | Least-privilege consumer locked out of raw PII and intermediate prep schemas. |

---

*Congratulations! You now understand the complete architecture of Project Cartographer. When you look at a complex data pipeline, never see scary code—always see a high-end kitchen striving for culinary perfection.*
