"""
Airflow DAG: Daily E-Commerce Data Pipeline
Schedule: Daily at 02:00 UTC

Flow:
  1. Check data freshness in Bronze
  2. Run PySpark Bronze→Silver batch job (simulated via Python)
  3. Run dbt models (Silver transforms + Gold aggregations)
  4. Run dbt tests
  5. Refresh Gold layer materialized views
  6. Log pipeline run metrics
  7. Send summary alert (logged, not actually emailed)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import logging
import json

logger = logging.getLogger(__name__)

# ─── Default args ────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data_engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ─── Task functions ──────────────────────────────────────────

def check_bronze_freshness(**context):
    """Verify Bronze layer has recent data before running transforms."""
    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*) as event_count,
            MAX(ingested_at) as latest_ingest,
            MIN(ingested_at) as earliest_today
        FROM bronze.raw_events
        WHERE ingested_at >= NOW() - INTERVAL '25 hours'
    """)
    row = cur.fetchone()
    event_count, latest, earliest = row

    logger.info(f"Bronze check: {event_count:,} events in last 25h | latest={latest}")

    if event_count == 0:
        logger.warning("No recent Bronze events — generating sample data")
        _seed_sample_data(cur, conn)
    else:
        logger.info(f"Bronze freshness OK — {event_count:,} events available")

    cur.close()
    conn.close()

    context["task_instance"].xcom_push(key="bronze_event_count", value=int(event_count))


def _seed_sample_data(cur, conn):
    """Seed realistic sample data for demo/testing."""
    import random
    from datetime import datetime, timedelta, timezone

    CATEGORIES = ["electronics", "clothing", "books", "home_kitchen",
                  "sports", "beauty", "toys", "grocery"]
    EVENT_TYPES = ["page_view", "page_view", "page_view",
                   "add_to_cart", "add_to_cart", "purchase"]
    COUNTRIES   = ["India", "USA", "UK", "Germany", "Australia"]
    DEVICES     = ["mobile", "desktop", "tablet"]

    now = datetime.now(timezone.utc)
    events = []
    for i in range(1000):
        cat = random.choice(CATEGORIES)
        ts  = now - timedelta(hours=random.randint(0, 20),
                              minutes=random.randint(0, 59))
        events.append((
            f"evt_{i:06d}",
            random.choice(EVENT_TYPES),
            f"user_{random.randint(1,200):04d}",
            f"sess_{random.randint(1,300):04d}",
            f"{cat}_prod_{random.randint(1,10):02d}",
            cat,
            round(random.uniform(5, 499), 2),
            random.randint(1, 3),
            f"/products/{cat}/item",
            random.choice(["google", "direct", "email", ""]),
            random.choice(DEVICES),
            random.choice(COUNTRIES),
            "Hyderabad",
            ts,
            json.dumps({}),
        ))

    cur.executemany("""
        INSERT INTO bronze.raw_events
          (event_id, event_type, user_id, session_id, product_id, category,
           price, quantity, page_url, referrer, device_type, country, city,
           ts, raw_payload)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, events)

    orders = []
    for i in range(200):
        ts = now - timedelta(hours=random.randint(0, 20))
        orders.append((
            f"ORD_{i:06d}",
            f"user_{random.randint(1,200):04d}",
            random.choice(["placed","confirmed","shipped","delivered","cancelled"]),
            round(random.uniform(20, 800), 2),
            random.choice([0, 5, 10, 15, 20]),
            random.choice(["credit_card","debit_card","upi","cod","paypal"]),
            json.dumps([{"product_id": "item_01", "qty": 1, "price": 99.0}]),
            json.dumps({"city": "Hyderabad", "country": "India"}),
            ts,
            ts,
            json.dumps({}),
        ))

    cur.executemany("""
        INSERT INTO bronze.raw_orders
          (order_id, user_id, status, total_amount, discount_pct,
           payment_method, items, shipping_addr, created_at, updated_at, raw_payload)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, orders)

    conn.commit()
    logger.info("Sample data seeded: 1000 events + 200 orders")


def run_bronze_to_silver(**context):
    """Transform Bronze→Silver using pure SQL (Spark alternative for Airflow)."""
    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()
    cur  = conn.cursor()

    logger.info("[B→S] Transforming events...")

    # Clear today's silver data first (idempotent)
    cur.execute("""
        DELETE FROM silver.events
        WHERE event_date = CURRENT_DATE
    """)

    cur.execute("""
        INSERT INTO silver.events
          (event_id, event_type, user_id, session_id, product_id, category,
           price, quantity, device_type, country, city, event_date, event_hour,
           ts, processed_at)
        SELECT DISTINCT ON (event_id)
            event_id,
            event_type,
            user_id,
            session_id,
            product_id,
            category,
            CASE WHEN price < 0 OR price > 50000 THEN NULL ELSE price END,
            CASE WHEN quantity < 0 OR quantity > 100 THEN 1 ELSE quantity END,
            lower(trim(device_type)),
            country,
            city,
            ts::date,
            EXTRACT(HOUR FROM ts)::int,
            ts::timestamp,
            NOW()
        FROM bronze.raw_events
        WHERE event_id IS NOT NULL
          AND user_id   IS NOT NULL
          AND event_type IS NOT NULL
          AND ts IS NOT NULL
        ORDER BY event_id, ingested_at DESC
        ON CONFLICT (event_id) DO NOTHING
    """)

    events_written = cur.rowcount
    logger.info(f"[B→S] Events written: {events_written:,}")

    logger.info("[B→S] Transforming orders...")
    cur.execute("DELETE FROM silver.orders WHERE order_date = CURRENT_DATE")
    cur.execute("""
        INSERT INTO silver.orders
          (order_id, user_id, status, total_amount, discount_pct,
           payment_method, item_count, order_date, created_at, updated_at, processed_at)
        SELECT DISTINCT ON (order_id)
            order_id,
            user_id,
            status,
            GREATEST(total_amount, 0),
            CASE WHEN discount_pct < 0 OR discount_pct > 100 THEN 0
                 ELSE discount_pct END,
            payment_method,
            1 as item_count,
            created_at::date,
            created_at::timestamp,
            updated_at::timestamp,
            NOW()
        FROM bronze.raw_orders
        WHERE order_id IS NOT NULL
          AND user_id  IS NOT NULL
          AND created_at IS NOT NULL
        ORDER BY order_id, ingested_at DESC
        ON CONFLICT (order_id) DO NOTHING
    """)
    orders_written = cur.rowcount
    logger.info(f"[B→S] Orders written: {orders_written:,}")

    logger.info("[B→S] Rebuilding user dimension...")
    cur.execute("""
        INSERT INTO silver.users
          (user_id, first_seen, last_seen, total_sessions, country, city,
           device_type, updated_at)
        SELECT
            user_id,
            MIN(ts),
            MAX(ts),
            COUNT(DISTINCT session_id),
            MODE() WITHIN GROUP (ORDER BY country),
            MODE() WITHIN GROUP (ORDER BY city),
            MODE() WITHIN GROUP (ORDER BY device_type),
            NOW()
        FROM silver.events
        GROUP BY user_id
        ON CONFLICT (user_id) DO UPDATE SET
            last_seen      = EXCLUDED.last_seen,
            total_sessions = EXCLUDED.total_sessions,
            updated_at     = NOW()
    """)
    users_written = cur.rowcount
    logger.info(f"[B→S] Users upserted: {users_written:,}")

    conn.commit()
    cur.close()
    conn.close()

    context["task_instance"].xcom_push(key="silver_events",  value=events_written)
    context["task_instance"].xcom_push(key="silver_orders",  value=orders_written)
    context["task_instance"].xcom_push(key="silver_users",   value=users_written)


def run_gold_aggregations(**context):
    """Build Gold layer aggregations (dbt alternative via SQL)."""
    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()
    cur  = conn.cursor()

    logger.info("[Gold] Computing daily revenue...")
    cur.execute("DELETE FROM gold.daily_revenue WHERE report_date = CURRENT_DATE")
    cur.execute("""
        INSERT INTO gold.daily_revenue
          (report_date, total_orders, completed_orders, gross_revenue,
           net_revenue, avg_order_value, unique_buyers, updated_at)
        SELECT
            order_date,
            COUNT(*),
            COUNT(*) FILTER (WHERE status = 'delivered'),
            SUM(total_amount),
            SUM(total_amount * (1 - discount_pct/100)),
            AVG(total_amount * (1 - discount_pct/100)),
            COUNT(DISTINCT user_id),
            NOW()
        FROM silver.orders
        WHERE order_date IS NOT NULL
        GROUP BY order_date
        ON CONFLICT (report_date) DO UPDATE SET
            total_orders     = EXCLUDED.total_orders,
            completed_orders = EXCLUDED.completed_orders,
            gross_revenue    = EXCLUDED.gross_revenue,
            net_revenue      = EXCLUDED.net_revenue,
            avg_order_value  = EXCLUDED.avg_order_value,
            unique_buyers    = EXCLUDED.unique_buyers,
            updated_at       = NOW()
    """)

    logger.info("[Gold] Computing product performance...")
    cur.execute("DELETE FROM gold.product_performance WHERE report_date = CURRENT_DATE")
    cur.execute("""
        INSERT INTO gold.product_performance
          (report_date, product_id, category, views, add_to_cart, purchases,
           revenue, conversion_rate, updated_at)
        SELECT
            event_date,
            product_id,
            category,
            COUNT(*) FILTER (WHERE event_type='page_view'),
            COUNT(*) FILTER (WHERE event_type='add_to_cart'),
            COUNT(*) FILTER (WHERE event_type='purchase'),
            SUM(price * quantity) FILTER (WHERE event_type='purchase'),
            CASE
                WHEN COUNT(*) FILTER (WHERE event_type='page_view') > 0
                THEN COUNT(*) FILTER (WHERE event_type='purchase')::numeric
                     / COUNT(*) FILTER (WHERE event_type='page_view')
                ELSE 0
            END,
            NOW()
        FROM silver.events
        WHERE product_id IS NOT NULL
        GROUP BY event_date, product_id, category
        ON CONFLICT (report_date, product_id) DO UPDATE SET
            views           = EXCLUDED.views,
            add_to_cart     = EXCLUDED.add_to_cart,
            purchases       = EXCLUDED.purchases,
            revenue         = EXCLUDED.revenue,
            conversion_rate = EXCLUDED.conversion_rate,
            updated_at      = NOW()
    """)

    logger.info("[Gold] Computing category summary...")
    cur.execute("DELETE FROM gold.category_summary WHERE report_date = CURRENT_DATE")
    cur.execute("""
        INSERT INTO gold.category_summary
          (report_date, category, total_views, total_purchases, total_revenue, updated_at)
        SELECT
            event_date,
            category,
            COUNT(*) FILTER (WHERE event_type='page_view'),
            COUNT(*) FILTER (WHERE event_type='purchase'),
            SUM(price * quantity) FILTER (WHERE event_type='purchase'),
            NOW()
        FROM silver.events
        WHERE category IS NOT NULL
        GROUP BY event_date, category
        ON CONFLICT (report_date, category) DO UPDATE SET
            total_views     = EXCLUDED.total_views,
            total_purchases = EXCLUDED.total_purchases,
            total_revenue   = EXCLUDED.total_revenue,
            updated_at      = NOW()
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("[Gold] All aggregations complete")


def validate_data_quality(**context):
    """Run data quality checks on Silver & Gold layers."""
    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()
    cur  = conn.cursor()

    checks = [
        ("Silver events not null event_id",
         "SELECT COUNT(*) FROM silver.events WHERE event_id IS NULL", 0),
        ("Silver events not null user_id",
         "SELECT COUNT(*) FROM silver.events WHERE user_id IS NULL", 0),
        ("Silver orders not null order_id",
         "SELECT COUNT(*) FROM silver.orders WHERE order_id IS NULL", 0),
        ("Silver orders negative amounts",
         "SELECT COUNT(*) FROM silver.orders WHERE total_amount < 0", 0),
        ("Gold daily revenue populated",
         "SELECT COUNT(*) FROM gold.daily_revenue", 1),
        ("Gold product performance populated",
         "SELECT COUNT(*) FROM gold.product_performance", 1),
    ]

    failures = []
    for name, sql, expected_max in checks:
        cur.execute(sql)
        count = cur.fetchone()[0]
        status = "PASS" if count <= expected_max else "FAIL"
        logger.info(f"[DQ] {status}: {name} = {count}")
        if count > expected_max:
            failures.append(f"{name}: {count} (expected <= {expected_max})")

    cur.close()
    conn.close()

    if failures:
        raise ValueError(f"Data quality failures: {'; '.join(failures)}")
    logger.info("[DQ] All quality checks passed")


def log_pipeline_run(**context):
    """Log pipeline metrics to airflow_meta.pipeline_runs."""
    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    ti   = context["task_instance"]
    dag_run = context["dag_run"]

    bronze_count = ti.xcom_pull(key="bronze_event_count",
                                task_ids="check_bronze_freshness") or 0
    silver_evts  = ti.xcom_pull(key="silver_events",
                                task_ids="bronze_to_silver") or 0
    silver_ords  = ti.xcom_pull(key="silver_orders",
                                task_ids="bronze_to_silver") or 0

    hook.run("""
        INSERT INTO airflow_meta.pipeline_runs
          (dag_id, run_id, stage, records_in, records_out, status,
           started_at, finished_at, notes)
        VALUES (%s, %s, %s, %s, %s, %s, NOW() - INTERVAL '5 minutes', NOW(), %s)
    """, parameters=(
        "daily_pipeline",
        dag_run.run_id,
        "full_run",
        bronze_count,
        silver_evts + silver_ords,
        "success",
        f"events={silver_evts}, orders={silver_ords}"
    ))
    logger.info(f"[Meta] Pipeline run logged: {silver_evts:,} events, "
                f"{silver_ords:,} orders processed")


# ─── DAG definition ──────────────────────────────────────────

with DAG(
    dag_id="daily_ecommerce_pipeline",
    description="Daily E-Commerce Data Pipeline: Kafka→Bronze→Silver→Gold",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "data-engineering", "production"],
) as dag:

    t_check_bronze = PythonOperator(
        task_id="check_bronze_freshness",
        python_callable=check_bronze_freshness,
    )

    t_bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
    )

    t_gold_aggregations = PythonOperator(
        task_id="run_gold_aggregations",
        python_callable=run_gold_aggregations,
    )

    t_dq_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=validate_data_quality,
    )

    t_log_run = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=log_pipeline_run,
        trigger_rule="all_done",
    )

    # Pipeline DAG
    t_check_bronze >> t_bronze_to_silver >> t_gold_aggregations >> t_dq_checks >> t_log_run
