"""
E-Commerce Analytics Dashboard — Streamlit
Reads directly from PostgreSQL Gold layer tables.
"""

import os
import time
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

# ─── Page config ─────────────────────────────────────────────

st.set_page_config(
    page_title="E-Commerce Analytics Platform",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .metric-card {
        background: #1e2130;
        border-radius: 10px;
        padding: 20px;
        border-left: 4px solid #4f8bf9;
    }
    .stMetric label { font-size: 0.85rem; color: #aaa; }
</style>
""", unsafe_allow_html=True)

# ─── DB connection ────────────────────────────────────────────

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host     = os.getenv("DB_HOST",     "localhost"),
        port     = int(os.getenv("DB_PORT", "5432")),
        dbname   = os.getenv("DB_NAME",     "ecommerce_dw"),
        user     = os.getenv("DB_USER",     "warehouse"),
        password = os.getenv("DB_PASSWORD", "warehouse123"),
    )


def run_query(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = get_connection()
        if conn.closed:
            st.cache_resource.clear()
            conn = get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


# ─── Sidebar ─────────────────────────────────────────────────

st.sidebar.image("https://img.icons8.com/fluency/96/shopping-cart.png", width=60)
st.sidebar.title("🛒 Analytics Platform")
st.sidebar.markdown("---")

date_range = st.sidebar.selectbox(
    "Date range",
    ["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
    index=1,
)

RANGE_MAP = {
    "Last 7 days":  7,
    "Last 30 days": 30,
    "Last 90 days": 90,
    "All time":     3650,
}
days_back = RANGE_MAP[date_range]
since_date = (datetime.now() - timedelta(days=days_back)).date()

auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
if auto_refresh:
    time.sleep(30)
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption("Medallion Architecture\nKafka → Spark → dbt → Gold")

# ─── Header ──────────────────────────────────────────────────

st.title("📊 E-Commerce Analytics Dashboard")
st.caption(f"Gold layer data · Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
st.divider()

# ─── KPI Row ─────────────────────────────────────────────────

kpi_df = run_query("""
    SELECT
        COALESCE(SUM(net_revenue), 0)         AS total_revenue,
        COALESCE(SUM(total_orders), 0)        AS total_orders,
        COALESCE(AVG(avg_order_value), 0)     AS avg_order_value,
        COALESCE(AVG(visitor_to_buyer_rate)*100, 0) AS conversion_rate,
        COALESCE(SUM(unique_buyers), 0)       AS total_buyers,
        COALESCE(SUM(total_sessions), 0)      AS total_sessions
    FROM gold.daily_revenue
    WHERE report_date >= %s
""", (since_date,))

if not kpi_df.empty:
    row = kpi_df.iloc[0]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("💰 Net Revenue",    f"₹{row['total_revenue']:,.0f}")
    c2.metric("🛍️ Total Orders",   f"{int(row['total_orders']):,}")
    c3.metric("🧾 Avg Order Value", f"₹{row['avg_order_value']:.2f}")
    c4.metric("🔄 Conversion",      f"{row['conversion_rate']:.2f}%")
    c5.metric("👥 Unique Buyers",   f"{int(row['total_buyers']):,}")
    c6.metric("🌐 Sessions",        f"{int(row['total_sessions']):,}")

st.divider()

# ─── Revenue trend + Orders by status ────────────────────────

col1, col2 = st.columns([3, 2])

with col1:
    st.subheader("📈 Daily Revenue Trend")
    rev_df = run_query("""
        SELECT report_date, gross_revenue, net_revenue, total_orders
        FROM gold.daily_revenue
        WHERE report_date >= %s
        ORDER BY report_date
    """, (since_date,))

    if not rev_df.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(
            x=rev_df["report_date"], y=rev_df["gross_revenue"],
            name="Gross Revenue", line=dict(color="#4f8bf9", width=2),
            fill="tozeroy", fillcolor="rgba(79,139,249,0.1)"
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=rev_df["report_date"], y=rev_df["net_revenue"],
            name="Net Revenue", line=dict(color="#00d4aa", width=2)
        ), secondary_y=False)
        fig.add_trace(go.Bar(
            x=rev_df["report_date"], y=rev_df["total_orders"],
            name="Orders", marker_color="rgba(255,165,0,0.4)", opacity=0.6
        ), secondary_y=True)
        fig.update_layout(
            height=320, template="plotly_dark", showlegend=True,
            legend=dict(orientation="h", y=1.1),
            margin=dict(l=0, r=0, t=30, b=0),
        )
        fig.update_yaxes(title_text="Revenue (₹)", secondary_y=False)
        fig.update_yaxes(title_text="Orders", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No revenue data yet — run the pipeline or start the producer.")

with col2:
    st.subheader("🏆 Top Categories by Revenue")
    cat_df = run_query("""
        SELECT category, SUM(total_revenue) AS revenue,
               SUM(total_purchases) AS purchases
        FROM gold.category_summary
        WHERE report_date >= %s AND category IS NOT NULL
        GROUP BY category
        ORDER BY revenue DESC NULLS LAST
        LIMIT 9
    """, (since_date,))

    if not cat_df.empty:
        fig = px.bar(
            cat_df, x="revenue", y="category", orientation="h",
            color="revenue", color_continuous_scale="Blues",
            labels={"revenue": "Revenue (₹)", "category": ""},
            height=320,
        )
        fig.update_layout(
            template="plotly_dark", showlegend=False,
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No category data yet.")

st.divider()

# ─── Funnel + Top Products ────────────────────────────────────

col3, col4 = st.columns([2, 3])

with col3:
    st.subheader("🔽 Conversion Funnel")
    funnel_df = run_query("""
        SELECT
            SUM(total_sessions)           AS sessions,
            SUM(unique_visitors)          AS visitors,
            SUM(users_who_added_to_cart)  AS carted,
            SUM(unique_buyers)            AS buyers
        FROM gold.daily_revenue
        WHERE report_date >= %s
    """, (since_date,))

    if not funnel_df.empty:
        row = funnel_df.iloc[0]
        stages  = ["Sessions", "Unique Visitors", "Added to Cart", "Buyers"]
        values  = [int(row["sessions"] or 0), int(row["visitors"] or 0),
                   int(row["carted"] or 0),   int(row["buyers"] or 0)]
        colors  = ["#4f8bf9", "#00d4aa", "#f4a261", "#e76f51"]

        fig = go.Figure(go.Funnel(
            y=stages, x=values,
            marker=dict(color=colors),
            textinfo="value+percent initial",
        ))
        fig.update_layout(
            height=300, template="plotly_dark",
            margin=dict(l=0, r=0, t=10, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("📦 Top Products by Revenue")
    prod_df = run_query("""
        SELECT product_id, category,
               SUM(revenue)      AS total_revenue,
               SUM(purchases)    AS total_purchases,
               SUM(views)        AS total_views,
               ROUND(AVG(overall_conversion_rate)*100, 2) AS conv_pct
        FROM gold.product_performance
        WHERE report_date >= %s AND product_id IS NOT NULL
        GROUP BY product_id, category
        ORDER BY total_revenue DESC NULLS LAST
        LIMIT 10
    """, (since_date,))

    if not prod_df.empty:
        prod_df.columns = ["Product", "Category", "Revenue (₹)",
                           "Purchases", "Views", "Conv %"]
        prod_df["Revenue (₹)"] = prod_df["Revenue (₹)"].apply(
            lambda x: f"₹{x:,.2f}" if x else "₹0.00")
        st.dataframe(prod_df, use_container_width=True, height=270)
    else:
        st.info("No product data yet.")

st.divider()

# ─── Device + Payment breakdown ──────────────────────────────

col5, col6 = st.columns(2)

with col5:
    st.subheader("📱 Sessions by Device Type")
    dev_df = run_query("""
        SELECT device_type, COUNT(*) AS cnt
        FROM silver.events
        WHERE event_date >= %s AND device_type IS NOT NULL
        GROUP BY device_type
        ORDER BY cnt DESC
    """, (since_date,))

    if not dev_df.empty:
        fig = px.pie(dev_df, names="device_type", values="cnt",
                     color_discrete_sequence=px.colors.qualitative.Set2,
                     hole=0.4, height=280)
        fig.update_layout(template="plotly_dark",
                          margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

with col6:
    st.subheader("💳 Orders by Payment Method")
    pay_df = run_query("""
        SELECT payment_method, COUNT(*) AS orders,
               SUM(total_amount) AS revenue
        FROM silver.orders
        WHERE order_date >= %s AND payment_method IS NOT NULL
        GROUP BY payment_method
        ORDER BY revenue DESC NULLS LAST
    """, (since_date,))

    if not pay_df.empty:
        fig = px.bar(pay_df, x="payment_method", y="revenue",
                     color="payment_method", text="orders",
                     labels={"revenue": "Revenue (₹)", "payment_method": "Method"},
                     color_discrete_sequence=px.colors.qualitative.Pastel,
                     height=280)
        fig.update_layout(template="plotly_dark", showlegend=False,
                          margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ─── Pipeline health ─────────────────────────────────────────

st.subheader("⚙️ Pipeline Health")
c1, c2, c3, c4 = st.columns(4)

bronze_cnt = run_query("SELECT COUNT(*) AS cnt FROM bronze.raw_events").iloc[0]["cnt"]
silver_cnt = run_query("SELECT COUNT(*) AS cnt FROM silver.events").iloc[0]["cnt"]
order_cnt  = run_query("SELECT COUNT(*) AS cnt FROM silver.orders").iloc[0]["cnt"]
gold_cnt   = run_query("SELECT COUNT(*) AS cnt FROM gold.daily_revenue").iloc[0]["cnt"]

c1.metric("🟠 Bronze Events",  f"{int(bronze_cnt):,}")
c2.metric("🔵 Silver Events",  f"{int(silver_cnt):,}")
c3.metric("🟣 Silver Orders",  f"{int(order_cnt):,}")
c4.metric("🟢 Gold Records",   f"{int(gold_cnt):,}")

runs_df = run_query("""
    SELECT dag_id, run_id, stage, records_in, records_out,
           status, started_at, notes
    FROM airflow_meta.pipeline_runs
    ORDER BY started_at DESC
    LIMIT 10
""")

if not runs_df.empty:
    with st.expander("📋 Recent Pipeline Runs"):
        st.dataframe(runs_df, use_container_width=True)

st.caption("Built with: Apache Kafka · PySpark · dbt · Airflow · PostgreSQL · Streamlit")
