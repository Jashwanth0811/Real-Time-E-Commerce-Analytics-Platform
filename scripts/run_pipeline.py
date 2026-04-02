"""
Pipeline Runner — Bronze → Silver → Gold
Pure-SQL implementation (no Spark required for local demo).
Mirrors exactly what the PySpark jobs and Airflow DAG do.

Run: python3 scripts/run_pipeline.py
"""

import psycopg2
import json
import time
from datetime import datetime

DB_CONFIG = dict(
    host="localhost", port=5432,
    dbname="ecommerce_dw",
    user="warehouse", password="warehouse123",
)


def run(sql: str, cur, label: str):
    t = time.time()
    cur.execute(sql)
    rows = cur.rowcount
    elapsed = time.time() - t
    print(f"  ✓ {label}: {rows:,} rows  ({elapsed:.2f}s)")
    return rows


def pipeline():
    print("\n" + "="*55)
    print("  E-Commerce Pipeline: Bronze → Silver → Gold")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*55)

    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    total_start = time.time()

    # ── Stage 1: Bronze → Silver Events ──────────────────────
    print("\n[Stage 1] Bronze → Silver Events")

    run("""
        INSERT INTO silver.events
          (event_id, event_type, user_id, session_id, product_id, category,
           price, quantity, device_type, country, city,
           event_date, event_hour, ts, processed_at)
        SELECT DISTINCT ON (event_id)
            event_id,
            event_type,
            user_id,
            session_id,
            product_id,
            category,
            CASE WHEN price < 0 OR price > 50000 THEN NULL ELSE price END,
            CASE WHEN quantity < 0 OR quantity > 100 THEN 1 ELSE quantity END,
            lower(trim(coalesce(device_type, 'unknown'))),
            coalesce(country, 'Unknown'),
            coalesce(city,    'Unknown'),
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
        ON CONFLICT (event_id) DO UPDATE SET
            event_type   = EXCLUDED.event_type,
            processed_at = NOW()
    """, cur, "Events → silver.events")

    # ── Stage 2: Bronze → Silver Orders ──────────────────────
    print("\n[Stage 2] Bronze → Silver Orders")

    run("""
        INSERT INTO silver.orders
          (order_id, user_id, status, total_amount, discount_pct,
           payment_method, item_count, order_date, created_at, updated_at, processed_at)
        SELECT DISTINCT ON (order_id)
            order_id,
            user_id,
            status,
            GREATEST(coalesce(total_amount, 0), 0),
            CASE WHEN discount_pct < 0 OR discount_pct > 100 THEN 0
                 ELSE coalesce(discount_pct, 0) END,
            coalesce(payment_method, 'unknown'),
            1,
            created_at::date,
            created_at::timestamp,
            updated_at::timestamp,
            NOW()
        FROM bronze.raw_orders
        WHERE order_id IS NOT NULL
          AND user_id  IS NOT NULL
          AND created_at IS NOT NULL
        ORDER BY order_id, ingested_at DESC
        ON CONFLICT (order_id) DO UPDATE SET
            status       = EXCLUDED.status,
            updated_at   = EXCLUDED.updated_at,
            processed_at = NOW()
    """, cur, "Orders → silver.orders")

    # ── Stage 3: Build User Dimension ────────────────────────
    print("\n[Stage 3] Build silver.users dimension")

    run("""
        INSERT INTO silver.users
          (user_id, first_seen, last_seen, total_sessions,
           country, city, device_type, updated_at)
        SELECT
            user_id,
            MIN(ts)                              AS first_seen,
            MAX(ts)                              AS last_seen,
            COUNT(DISTINCT session_id)           AS total_sessions,
            MODE() WITHIN GROUP (ORDER BY country) AS country,
            MODE() WITHIN GROUP (ORDER BY city)    AS city,
            MODE() WITHIN GROUP (ORDER BY device_type) AS device_type,
            NOW()
        FROM silver.events
        GROUP BY user_id
        ON CONFLICT (user_id) DO UPDATE SET
            last_seen      = EXCLUDED.last_seen,
            total_sessions = EXCLUDED.total_sessions,
            updated_at     = NOW()
    """, cur, "Users → silver.users")

    conn.commit()

    # ── Stage 4: Gold — Daily Revenue ────────────────────────
    print("\n[Stage 4] Gold — Daily Revenue")

    run("""
        INSERT INTO gold.daily_revenue
          (report_date, total_orders, completed_orders, gross_revenue,
           net_revenue, avg_order_value, unique_buyers, updated_at)
        SELECT
            order_date,
            COUNT(*)                                           AS total_orders,
            COUNT(*) FILTER (WHERE status='delivered')        AS completed_orders,
            COALESCE(SUM(total_amount), 0)                    AS gross_revenue,
            COALESCE(SUM(total_amount*(1-discount_pct/100)),0) AS net_revenue,
            COALESCE(AVG(total_amount*(1-discount_pct/100)),0) AS avg_order_value,
            COUNT(DISTINCT user_id)                           AS unique_buyers,
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
    """, cur, "Daily revenue → gold.daily_revenue")

    # ── Stage 5: Gold — Product Performance ──────────────────
    print("\n[Stage 5] Gold — Product Performance")

    run("""
        INSERT INTO gold.product_performance
          (report_date, product_id, category, views, add_to_cart,
           purchases, revenue, conversion_rate, updated_at)
        SELECT
            event_date,
            product_id,
            category,
            COUNT(*) FILTER (WHERE event_type='page_view'),
            COUNT(*) FILTER (WHERE event_type='add_to_cart'),
            COUNT(*) FILTER (WHERE event_type='purchase'),
            COALESCE(SUM(price*quantity) FILTER (WHERE event_type='purchase'), 0),
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
    """, cur, "Product performance → gold.product_performance")

    # ── Stage 6: Gold — Category Summary ─────────────────────
    print("\n[Stage 6] Gold — Category Summary")

    run("""
        INSERT INTO gold.category_summary
          (report_date, category, total_views, total_purchases, total_revenue, updated_at)
        SELECT
            event_date,
            category,
            COUNT(*) FILTER (WHERE event_type='page_view'),
            COUNT(*) FILTER (WHERE event_type='purchase'),
            COALESCE(SUM(price*quantity) FILTER (WHERE event_type='purchase'), 0),
            NOW()
        FROM silver.events
        WHERE category IS NOT NULL
        GROUP BY event_date, category
        ON CONFLICT (report_date, category) DO UPDATE SET
            total_views     = EXCLUDED.total_views,
            total_purchases = EXCLUDED.total_purchases,
            total_revenue   = EXCLUDED.total_revenue,
            updated_at      = NOW()
    """, cur, "Category summary → gold.category_summary")

    # ── Stage 7: Gold — User Funnel ──────────────────────────
    print("\n[Stage 7] Gold — User Funnel")

    run("""
        INSERT INTO gold.user_funnel
          (report_date, stage, users, drop_off_rate, updated_at)
        SELECT
            event_date,
            event_type AS stage,
            COUNT(DISTINCT user_id) AS users,
            0 AS drop_off_rate,
            NOW()
        FROM silver.events
        WHERE event_type IN ('page_view','add_to_cart','purchase')
        GROUP BY event_date, event_type
        ON CONFLICT (report_date, stage) DO UPDATE SET
            users      = EXCLUDED.users,
            updated_at = NOW()
    """, cur, "User funnel → gold.user_funnel")

    conn.commit()

    # ── Stage 8: Log pipeline run ─────────────────────────────
    cur.execute("""
        INSERT INTO airflow_meta.pipeline_runs
          (dag_id, run_id, stage, records_in, records_out,
           status, started_at, finished_at, notes)
        SELECT
            'manual_run',
            'run_' || to_char(NOW(), 'YYYYMMDD_HH24MI'),
            'full_pipeline',
            (SELECT COUNT(*) FROM bronze.raw_events),
            (SELECT COUNT(*) FROM silver.events),
            'success',
            NOW() - INTERVAL '1 minute',
            NOW(),
            'Initial seed pipeline run'
    """)
    conn.commit()

    cur.close()
    conn.close()

    elapsed = time.time() - total_start
    print("\n" + "="*55)
    print(f"  ✓ Pipeline complete in {elapsed:.1f}s")
    print("  → Dashboard: http://localhost:8501")
    print("="*55 + "\n")


if __name__ == "__main__":
    pipeline()
