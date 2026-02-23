# 🏭 FMCG Lakehouse ETL — Databricks End-to-End Data Engineering

> **End-to-end ETL pipeline** consolidating FMCG retail data from two post-acquisition companies into a unified **Medallion Lakehouse** using Databricks, PySpark, Delta Lake & AWS S3.


## 📌 Business Problem

**Atlön** (large FMCG retailer) acquired **Sports Bar** (startup). The COO needed unified analytics across both companies — but the data couldn't talk to each other:

| Company | Data Status | Issues |
|---|---|---|
| **Atlön** | ✅ Clean & structured | Already in Gold layer |
| **Sports Bar** | ❌ Messy & inconsistent | Nulls, negatives, spelling errors, format mismatches, multiple sources |

**Goal:** Build a reliable ETL pipeline that cleans Sports Bar's data and consolidates both companies into a **single analytics layer** for unified reporting.

### ✅ Success Criteria (COO Requirements)
1. Aggregated analytics for both companies in **one reliable dashboard**
2. **Low learning curve** for new data hires
3. **Scalable, long-term** automated solution

---

## 🏗️ Architecture — Medallion Lakehouse

```
Source Systems           AWS S3            Databricks
(CSVs, APIs,      →   (Data Lake)   →   Bronze → Silver → Gold → Dashboard
 WhatsApp exports)                        Raw     Cleaned  Analytics  + Genie AI
```

### Layer Responsibilities

| Layer | Purpose | Write Mode |
|---|---|---|
| 🟤 **Bronze** | Raw ingestion from S3. Untouched. Audit metadata added. | OVERWRITE (dims) / APPEND (facts) |
| ⚪ **Silver** | PySpark cleaning: types, nulls, spelling, surrogate keys | Transform & validate |
| 🟡 **Gold** | Delta MERGE upserts. Single source of truth. BI-ready. | MERGE (dims) / APPEND (facts) |

---

## 🗂️ Project Structure

```
fmcg-lakehouse-etl-databricks/
│
├── notebooks/
│   ├── setup/
│   │   ├── 01_setup_catalog.ipynb          # Create FMCG catalog + schemas
│   │   └── 02_s3_connection.ipynb          # IAM role-based S3 connection
│   │
│   ├── bronze/
│   │   ├── 01_ingest_dimensions.ipynb      # Load dim CSVs from S3 → Bronze
│   │   └── 02_ingest_facts.ipynb           # Load fact CSVs from S3 → Bronze
│   │
│   ├── silver/
│   │   ├── 01_customers_processing.ipynb   # Clean & transform dim_customers
│   │   ├── 02_products_processing.ipynb    # Clean, regex fix, variant extract
│   │   └── 03_facts_processing.ipynb       # Type cast, filter, derive columns
│   │
│   ├── gold/
│   │   ├── 01_dim_upserts.ipynb            # Delta MERGE for all dimensions
│   │   ├── 02_full_load_fact.ipynb         # Historical 5-month batch load
│   │   ├── 03_incremental_load_fact.ipynb  # Daily incremental S3 → Gold
│   │   └── 04_fact_sales_view.ipynb        # Denormalized view for BI + Genie
│   │
│   └── orchestration/
│       └── pipeline_job_config.json        # Databricks Jobs configuration
│
├── docs/
│   ├── architecture_diagram.png
│   ├── data_model.png
│   └── dashboard_screenshot.png
│
├── data_model/
│   └── star_schema.md
│
├── README.md
└── requirements.txt
```

---

## 📊 Data Model — Star Schema

```
                    ┌─────────────────┐
                    │  dim_customers  │
                    │  customer_id PK │
                    └────────┬────────┘
                             │
┌──────────────┐    ┌────────▼────────┐    ┌──────────────────┐
│ dim_products │    │   fact_orders   │    │  dim_gross_price │
│ product_id PK├────│ date            ├────│  product_id FK   │
│ category     │    │ product_id FK   │    │  fiscal_year     │
│ division     │    │ customer_id FK  │    │  gross_price     │
│ variant      │    │ sold_quantity   │    └──────────────────┘
└──────────────┘    │ source_company  │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   fact_sales    │  ← Denormalized Gold view
                    │  (All dims +    │     for Dashboard & Genie AI
                    │   fact joined)  │
                    └─────────────────┘
```

---

## ⚙️ Key Technical Implementations

### 1. Surrogate Key Generation (SHA-256)
```python
from pyspark.sql.functions import sha2, concat_ws

# Prevents key collisions between Atlön and Sports Bar IDs
df = df.withColumn(
    "customer_id",
    sha2(concat_ws("_", col("customer_code"), col("source_company")), 256)
)
```

### 2. Data Quality Fixes (Silver Layer)
```python
from pyspark.sql.functions import regexp_replace, when, col

# Fix spelling errors
df = df.withColumn("category",
    regexp_replace(col("category"), "(?i)beverege", "Beverage"))

# Remove invalid records
df = df.filter(col("sold_quantity") > 0)

# Handle nulls
df = df.withColumn("category",
    when(col("category").isNull(), "Unknown").otherwise(col("category")))
```

### 3. Gold Layer MERGE (Upsert)
```python
from delta.tables import DeltaTable

gold = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
gold.alias("gold") \
    .merge(df_silver.alias("src"), "gold.customer_id = src.customer_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

### 4. Incremental Load — Exactly Once
```python
import boto3

s3 = boto3.client("s3")
new_files = s3.list_objects_v2(Bucket="bucket", Prefix="new_data/")["Contents"]

for file in new_files:
    df = spark.read.option("header", True).csv(f"s3://bucket/{file['Key']}")
    df.write.format("delta").mode("append").saveAsTable("fmcg.bronze.fact_orders")
    # Archive to prevent reprocessing
    s3.copy_object(Bucket="bucket",
                   CopySource=f"bucket/{file['Key']}",
                   Key=file['Key'].replace("new_data/", "processed/"))
    s3.delete_object(Bucket="bucket", Key=file['Key'])
```

---

## 🔄 Pipeline Modes

### Batch — Historical Load (5 Months)
- One-time backfill of Sports Bar historical sales data
- All CSVs processed through Bronze → Silver → Gold
- Fact table: **APPEND** mode to preserve all historical records

### Incremental — Daily Load (From Nov 30 onwards)
- New daily files land in `s3://bucket/new_data/`
- Archived to `s3://bucket/processed/` after ingestion (**exactly-once**)
- Scheduled via **Databricks Jobs** at 11 PM daily (`0 23 * * *`)

---

## 🛠️ Tech Stack

| Tool | Role |
|---|---|
| **Databricks Free Edition** | Unified platform — notebooks, jobs, dashboard, Genie |
| **Apache Spark / PySpark** | Distributed data transformation |
| **Delta Lake** | ACID transactions, MERGE upsert, time travel |
| **AWS S3** | Raw data lake storage |
| **AWS IAM Roles** | Credential-free, least-privilege S3 access |
| **Python + boto3** | Pipeline scripting & S3 file operations |
| **SQL** | DDL, Gold view creation, validation |
| **Databricks Jobs** | Orchestration & cron scheduling |
| **Databricks Genie** | AI natural-language querying on Gold layer |
| **Databricks Dashboard** | Atlön BI 360 — unified BI reporting |
| **SHA-256 Hashing** | Surrogate key generation |
| **Regex** | Spelling correction & variant extraction |

---

## 📈 Dashboard — Atlön BI 360

Built on `fmcg.gold.fact_sales` denormalized view:

- 📊 **Counter KPIs** — Total Revenue, Total Quantity Sold
- 📅 **Date & Category Filters** — Dynamic slicing
- 📊 **Bar Chart** — Top products by revenue
- 🥧 **Pie Chart** — Revenue share by sales channel
- 🤖 **Genie AI** — Ask questions like *"Top 5 customers by revenue last quarter"*

---

## 🚀 How to Run

**Prerequisites:** Databricks Free Edition + AWS Free Tier account

```bash
# 1. Clone repo
git clone https://github.com/pawan-111/fmcg-lakehouse-etl-databricks.git

# 2. Import notebooks into Databricks workspace

# 3. Set up S3 bucket + IAM role (see notebooks/setup/)

# 4. Upload raw CSVs to S3 bucket

# 5. Run notebooks in order:
#    setup/ → bronze/ → silver/ → gold/

# 6. Schedule Databricks Job using pipeline_job_config.json
```

---

## 🔑 Concepts Demonstrated

`Medallion Architecture` `Delta Lake MERGE` `Idempotent Pipeline` `Batch + Incremental Load` `SHA-256 Surrogate Keys` `Star Schema` `IAM Security` `Databricks Jobs` `Genie AI` `Denormalized Gold View` `PySpark Transformations` `Data Quality Engineering`

---

## 📚 Reference

- 📹 Full walkthrough: [YouTube Tutorial](https://youtu.be/U6ZUKWdfSLY)
- 🧱 [Databricks Free Edition](https://www.databricks.com/try-databricks)
- 📖 [Delta Lake Docs](https://delta.io)

---

## 👤 Author

**Pawan** · Data Engineer · [GitHub @pawan-111](https://github.com/pawan-111)

> ⭐ Star this repo if it helped you!
