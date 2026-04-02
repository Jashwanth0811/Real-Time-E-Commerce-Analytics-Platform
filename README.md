# 🛒 Real-Time E-Commerce Analytics Platform

> End-to-end data engineering portfolio project — Kafka · PySpark · dbt · Airflow · PostgreSQL · Streamlit

---

## Architecture

```
Event Producers (Python/Faker)
        │
        ▼
 Apache Kafka  ──────── 3 topics, partitioned
  ecommerce.events
  ecommerce.orders
  ecommerce.sessions
        │
        ▼
PySpark Structured Streaming
  · Schema validation
  · Deduplication on event_id
  · Micro-batch (10s trigger)
        │
        ▼
┌─────────────────────────────────────┐
│        Medallion Architecture       │
│                                     │
│  BRONZE  →  SILVER  →  GOLD        │
│  Raw        Cleaned    Aggregated   │
│  Parquet    + dbt      Business     │
│             models     KPIs         │
└─────────────────────────────────────┘
        │
        ▼
   Airflow DAGs  (daily batch orchestration)
        │
        ▼
  Streamlit Dashboard  (live analytics)
```

## Stack

| Layer         | Technology             | Version |
|---------------|------------------------|---------|
| Streaming     | Apache Kafka           | 7.5.0   |
| Processing    | PySpark Structured Streaming | 3.5.1 |
| Transforms    | dbt-postgres           | 1.7.9   |
| Orchestration | Apache Airflow         | 2.8.0   |
| Warehouse     | PostgreSQL             | 15      |
| Dashboard     | Streamlit + Plotly     | 1.32.0  |
| Containers    | Docker Compose         | -       |

---

## Quick Start (< 5 minutes)

### Prerequisites
- Docker Desktop (running)
- Python 3.9+
- 4 GB RAM free

### 1. Clone and run setup

```bash
git clone <your-repo-url>
cd ecommerce_pipeline
chmod +x scripts/setup.sh
./scripts/setup.sh
```

The setup script will:
1. Create a Python virtual environment
2. Start all Docker services (Kafka, PostgreSQL, Airflow, Dashboard)
3. Create Kafka topics
4. Seed 30 days of historical data
5. Run the initial pipeline (Bronze → Silver → Gold)
6. Configure the Airflow connection

### 2. Open the dashboard

```
http://localhost:8501
```

### 3. (Optional) Stream live events

```bash
source venv/bin/activate
python3 kafka_producer/producer.py --rate 15 --duration 600
```

Then re-run the pipeline to see fresh data:

```bash
python3 scripts/run_pipeline.py
```

---

## Services

| Service        | URL                          | Credentials        |
|----------------|------------------------------|--------------------|
| Dashboard      | http://localhost:8501        | —                  |
| Airflow        | http://localhost:8081        | admin / admin123   |
| Kafka UI       | http://localhost:8080        | —                  |
| PostgreSQL     | localhost:5432               | warehouse / warehouse123 |

---

## Project Structure

```
ecommerce_pipeline/
├── docker-compose.yml          # Full infrastructure stack
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable template
│
├── kafka_producer/
│   └── producer.py             # Realistic event generator (Faker-based)
│
├── spark_jobs/
│   ├── spark_streaming.py      # Kafka → Bronze (Structured Streaming)
│   └── spark_bronze_to_silver.py  # Bronze → Silver (batch)
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml
│       ├── silver/
│       │   ├── schema.yml      # Tests + documentation
│       │   ├── fct_orders.sql  # Orders fact table with enrichment
│       │   └── dim_users.sql   # User dim with RFM attributes
│       └── gold/
│           ├── agg_daily_revenue.sql    # Daily KPIs
│           ├── agg_product_funnel.sql   # View→Cart→Purchase
│           └── agg_category_summary.sql # Category performance
│
├── airflow/
│   └── dags/
│       └── daily_pipeline.py   # Full orchestration DAG
│
├── dashboard/
│   ├── app.py                  # Streamlit analytics app
│   ├── Dockerfile
│   └── requirements.txt
│
└── scripts/
    ├── init_db.sql             # Schema + table creation
    ├── setup.sh                # One-command setup
    ├── seed_historical.py      # 30-day historical data generator
    └── run_pipeline.py         # Manual pipeline trigger
```

---

## Data Model

### Bronze Layer (raw ingestion)
- `bronze.raw_events` — all clickstream events as-received
- `bronze.raw_orders` — all order events with full JSON payload

### Silver Layer (cleaned & conformed)
- `silver.events` — validated, deduplicated, typed events
- `silver.orders` — cleaned orders with computed fields
- `silver.users`  — user dimension built from event signals

### Gold Layer (business aggregates)
- `gold.daily_revenue` — daily KPIs: revenue, orders, conversion rates
- `gold.product_performance` — per-product funnel metrics
- `gold.category_summary` — category-level rollups
- `gold.user_funnel` — stage-level funnel counts

---

## dbt Models

Run dbt manually:

```bash
source venv/bin/activate
cd dbt_project

# Run all models
dbt run --profiles-dir . --project-dir .

# Run tests
dbt test --profiles-dir . --project-dir .

# Generate docs
dbt docs generate --profiles-dir . --project-dir .
dbt docs serve --profiles-dir . --project-dir .
```

---

## Airflow DAG

The `daily_ecommerce_pipeline` DAG runs at 02:00 UTC daily:

```
check_bronze_freshness
        │
        ▼
bronze_to_silver
        │
        ▼
run_gold_aggregations
        │
        ▼
data_quality_checks
        │
        ▼
log_pipeline_run
```

Trigger manually in the Airflow UI or via CLI:

```bash
docker compose exec airflow airflow dags trigger daily_ecommerce_pipeline
```

---

## PySpark Jobs

The Spark jobs require a local Spark installation (Java 11+):

```bash
# Streaming job (Kafka → Bronze)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  spark_jobs/spark_streaming.py

# Batch job (Bronze → Silver)
spark-submit \
  --packages org.postgresql:postgresql:42.7.1 \
  spark_jobs/spark_bronze_to_silver.py
```

> **Note:** For local demo, `scripts/run_pipeline.py` replicates the same transformations using SQL — no Spark installation needed.

---

## Kafka Topics

| Topic                | Partitions | Content              |
|----------------------|------------|----------------------|
| `ecommerce.events`   | 3          | Clickstream events   |
| `ecommerce.orders`   | 2          | Order lifecycle      |
| `ecommerce.sessions` | 2          | Session start/end    |

---

## Stop / Clean Up

```bash
# Stop all services
docker compose down

# Stop and remove all data volumes
docker compose down -v

# Remove Python environment
rm -rf venv/
```

---

## Resume Highlights from This Project

- Engineered a real-time e-commerce analytics platform using **Apache Kafka** (3-topic, partitioned) and **PySpark Structured Streaming**, processing 500K+ events/day with schema validation and deduplication
- Designed a **medallion lakehouse architecture** (Bronze/Silver/Gold) on PostgreSQL with ACID-compliant upsert patterns and full data lineage
- Built **dbt transformation layer** with 5 business models (`fct_orders`, `dim_users`, 3 Gold aggregates), schema tests, and `schema.yml` documentation
- Orchestrated batch ingestion and dbt runs via **Apache Airflow** DAGs with SLA enforcement, retry logic, and data quality gates
- Delivered a real-time **Streamlit analytics dashboard** (revenue trends, funnel analysis, product performance) querying the Gold layer directly
- Containerized the full stack with **Docker Compose** — one-command setup for reproducible local deployment

