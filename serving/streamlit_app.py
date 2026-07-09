"""VeloShelf — Streamlit business dashboard.

Business-facing live view reading from Postgres windowed_features + alerts.
Designed for a supply-chain manager or ops lead, not an engineer.

Run:
    streamlit run serving/streamlit_app.py
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="VeloShelf — Dark Store Intelligence",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://veloshelf:veloshelf@localhost:5432/veloshelf",
)
REPORTS_PATH = Path(os.getenv("REPORTS_PATH", "data/reports"))
REFRESH_S    = int(os.getenv("DASHBOARD_REFRESH_S", "30"))

# Alert lookback window (minutes) — alerts table has no 'resolved' column
ALERT_WINDOW_MIN = 15

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
@st.cache_resource
def get_connection():
    import psycopg
    return psycopg.connect(POSTGRES_DSN, autocommit=True)


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        conn = get_connection()
        return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Data fetchers — column names match actual windowed_features + alerts schema
# ---------------------------------------------------------------------------

def fetch_freshness() -> dict:
    df = query("SELECT MAX(window_end) AS latest FROM windowed_features")
    if df.empty or df["latest"].isna().all():
        return {"lag_s": None, "status": "no_data"}
    latest = pd.to_datetime(df["latest"].iloc[0], utc=True)
    lag_s  = (datetime.now(tz=UTC) - latest).total_seconds()
    status = "healthy" if lag_s < 120 else "stale" if lag_s < 300 else "down"
    return {"lag_s": lag_s, "latest": latest, "status": status}


def fetch_alert_counts() -> dict[str, int]:
    df = query(
        """
        SELECT alert_type, COUNT(*) AS cnt
        FROM alerts
        WHERE resolved = FALSE
        GROUP BY alert_type
        """
    )
    if df.empty:
        return {"stockout": 0, "surge": 0}
    counts = dict(zip(df["alert_type"], df["cnt"].astype(int)))
    return {"stockout": counts.get("stockout_risk", 0), "surge": counts.get("surge", 0)}


def fetch_stockout_risks(threshold: int) -> pd.DataFrame:
    return query(
        """
        SELECT store_id, sku_id,
               on_hand_est, depletion_vel, order_rate, demand_momentum,
               window_end
        FROM windowed_features
        WHERE on_hand_est <= %s
        ORDER BY depletion_vel DESC
        LIMIT 50
        """,
        (threshold,),
    )


def fetch_surge_alerts() -> pd.DataFrame:
    return query(
        """
        SELECT a.store_id, a.sku_id, a.metric_value, a.threshold,
               a.triggered_at,
               f.order_rate, f.demand_momentum, f.on_hand_est
        FROM alerts a
        LEFT JOIN LATERAL (
            SELECT order_rate, demand_momentum, on_hand_est
            FROM windowed_features f2
            WHERE f2.store_id = a.store_id AND f2.sku_id = a.sku_id
            ORDER BY window_end DESC
            LIMIT 1
        ) f ON true
        WHERE a.alert_type = 'surge'
          AND a.resolved = FALSE
        ORDER BY a.metric_value DESC
        LIMIT 30
        """
    )


def fetch_category_velocity() -> pd.DataFrame:
    """Top SKUs by order rate per store (last 10 min)."""
    return query(
        """
        SELECT store_id, sku_id,
               ROUND(AVG(order_rate)::numeric, 3)     AS avg_order_rate,
               ROUND(AVG(depletion_vel)::numeric, 3)  AS avg_depletion
        FROM windowed_features
        WHERE window_end > NOW() - INTERVAL '10 minutes'
        GROUP BY store_id, sku_id
        ORDER BY avg_order_rate DESC
        LIMIT 60
        """
    )


def fetch_store_health() -> pd.DataFrame:
    """Per-store aggregate from the most recent window per SKU."""
    return query(
        """
        SELECT
            wf.store_id,
            ROUND(AVG(wf.on_hand_est)::numeric, 1)         AS avg_on_hand,
            ROUND(AVG(wf.order_rate)::numeric, 3)           AS avg_order_rate,
            ROUND(AVG(wf.demand_momentum)::numeric, 2)      AS avg_momentum,
            COUNT(DISTINCT wf.sku_id)                        AS active_skus,
            COALESCE(a.alert_cnt, 0)                         AS recent_alerts
        FROM windowed_features wf
        LEFT JOIN (
            SELECT store_id, COUNT(*) AS alert_cnt
            FROM alerts
            WHERE triggered_at > NOW() - INTERVAL '%s minutes'
            GROUP BY store_id
        ) a ON a.store_id = wf.store_id
        GROUP BY wf.store_id, a.alert_cnt
        ORDER BY recent_alerts DESC
        """,
        (ALERT_WINDOW_MIN,),
    )


def fetch_recent_alerts_log() -> pd.DataFrame:
    return query(
        """
        SELECT alert_type, store_id, sku_id, metric_value, threshold,
               resolved, triggered_at
        FROM alerts
        ORDER BY triggered_at DESC
        LIMIT 100
        """
    )


def load_latest_drift_report() -> Path | None:
    files = sorted(REPORTS_PATH.glob("drift_*.html"), reverse=True)
    return files[0] if files else None


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

STATUS_MAP = {
    "healthy": ("🟢", "Healthy"),
    "stale":   ("🟡", "Stale"),
    "down":    ("🔴", "Down"),
    "no_data": ("⚫", "No data"),
}

SEVERITY_COLOUR = {"high": "🔴", "medium": "🟡", "low": "🟢"}


def _fmt_lag(lag_s: float | None) -> str:
    if lag_s is None:
        return "—"
    if lag_s < 60:
        return f"{lag_s:.0f}s ago"
    return f"{lag_s / 60:.1f}m ago"


def _colour_on_hand(val: float, low: int = 20, mid: int = 50) -> str:
    if val <= low:
        return "background-color: #ffcccc"
    if val <= mid:
        return "background-color: #fff3cc"
    return ""


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def render_sidebar() -> int:
    st.sidebar.header("Controls")
    threshold = st.sidebar.slider(
        "Stockout reorder threshold (on-hand units)",
        min_value=5, max_value=100, value=40, step=5,
    )
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        f"**Alert lookback:** last {ALERT_WINDOW_MIN} min  \n"
        f"**Auto-refresh:** every {REFRESH_S}s"
    )
    st.sidebar.markdown("---")
    st.sidebar.caption("PSI thresholds")
    st.sidebar.markdown(
        "🟢 < 0.10 — stable  \n"
        "🟡 0.10–0.25 — moderate  \n"
        "🔴 > 0.25 — significant drift"
    )
    return threshold


# ---------------------------------------------------------------------------
# Panels
# ---------------------------------------------------------------------------

def render_header(freshness: dict, alert_counts: dict) -> None:
    icon, label = STATUS_MAP.get(freshness["status"], ("⚫", "Unknown"))
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Pipeline", f"{icon} {label}")
    c2.metric("Last Window", _fmt_lag(freshness.get("lag_s")))
    c3.metric("Stockout Alerts", alert_counts["stockout"], delta=None)
    c4.metric("Surge Alerts", alert_counts["surge"], delta=None)


def render_stockout(threshold: int) -> None:
    df = fetch_stockout_risks(threshold)
    if df.empty:
        st.success(f"No SKUs below {threshold} units. All stores healthy.")
        return

    st.caption(f"{len(df)} SKU(s) at risk · sorted by depletion rate")

    # Bar chart — on_hand_est per SKU (worst first)
    chart_df = (
        df[["sku_id", "store_id", "on_hand_est"]]
        .head(20)
        .copy()
    )
    chart_df["label"] = chart_df["sku_id"] + " / " + chart_df["store_id"]
    st.bar_chart(
        chart_df.set_index("label")["on_hand_est"],
        height=220,
        color="#e63946",
    )

    display = df.rename(columns={
        "store_id": "Store", "sku_id": "SKU",
        "on_hand_est": "On Hand", "depletion_vel": "Depletion (u/min)",
        "order_rate": "Order Rate", "demand_momentum": "Momentum",
        "window_end": "Window End",
    })
    st.dataframe(
        display.style.map(
            _colour_on_hand, subset=["On Hand"]
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_surges() -> None:
    df = fetch_surge_alerts()
    if df.empty:
        st.info(f"No surge alerts in the last {ALERT_WINDOW_MIN} minutes.")
        return

    st.caption(f"{len(df)} active surge(s) · sorted by metric value")

    # metric_value bar chart
    chart_df = df[["sku_id", "store_id", "metric_value"]].head(15).copy()
    chart_df["label"] = chart_df["sku_id"] + " / " + chart_df["store_id"]
    st.bar_chart(
        chart_df.set_index("label")["metric_value"],
        height=200,
        color="#f4a261",
    )

    st.dataframe(
        df.rename(columns={
            "store_id": "Store", "sku_id": "SKU",
            "metric_value": "Momentum", "threshold": "Threshold",
            "order_rate": "Order Rate", "demand_momentum": "Demand Momentum",
            "on_hand_est": "On Hand", "triggered_at": "Triggered At",
        }),
        use_container_width=True,
        hide_index=True,
    )


def render_velocity() -> None:
    df = fetch_category_velocity()
    if df.empty:
        st.info("No feature data in the last 10 minutes.")
        return

    left, right = st.columns(2)

    with left:
        st.markdown("**Order Rate (orders/min) — SKU × store**")
        pivot = df.pivot_table(
            index="sku_id", columns="store_id",
            values="avg_order_rate", aggfunc="mean",
        ).fillna(0).round(3)
        st.dataframe(pivot, use_container_width=True)

    with right:
        st.markdown("**Avg Depletion Velocity (units/min)**")
        pivot2 = df.pivot_table(
            index="sku_id", columns="store_id",
            values="avg_depletion", aggfunc="mean",
        ).fillna(0).round(3)
        st.dataframe(pivot2, use_container_width=True)

    # Bar chart — top SKUs by total order rate across stores
    sku_totals = df.groupby("sku_id")["avg_order_rate"].sum().sort_values(ascending=False).head(20)
    st.markdown("**Top 20 SKUs by order rate**")
    st.bar_chart(sku_totals, height=220, color="#2a9d8f")


def render_store_health() -> None:
    df = fetch_store_health()
    if df.empty:
        st.info("No store data available.")
        return

    # Alert count bar
    if "recent_alerts" in df.columns:
        st.bar_chart(
            df.set_index("store_id")["recent_alerts"],
            height=180,
            color="#e76f51",
        )

    st.dataframe(
        df.rename(columns={
            "store_id": "Store",
            "avg_on_hand": "Avg On Hand",
            "avg_order_rate": "Avg Order Rate",
            "avg_momentum": "Avg Momentum",
            "active_skus": "Active SKUs",
            "recent_alerts": f"Alerts (last {ALERT_WINDOW_MIN}m)",
        }),
        use_container_width=True,
        hide_index=True,
    )

    # Recent alert log
    log = fetch_recent_alerts_log()
    if not log.empty:
        with st.expander(f"Alert log — last {len(log)} events"):
            st.dataframe(
                log.rename(columns={
                    "alert_type": "Type", "store_id": "Store", "sku_id": "SKU",
                    "metric_value": "Value", "threshold": "Threshold",
                    "resolved": "Resolved", "triggered_at": "Time",
                }),
                use_container_width=True,
                hide_index=True,
            )


def render_drift() -> None:
    report = load_latest_drift_report()

    col1, col2, col3 = st.columns(3)
    col1.markdown("**PSI < 0.10**  \n🟢 Stable")
    col2.markdown("**PSI 0.10–0.25**  \n🟡 Moderate shift")
    col3.markdown("**PSI > 0.25**  \n🔴 Significant — retrain triggered")

    st.divider()

    if report:
        st.success(f"Latest report: `{report}`")
        try:
            with open(report, "rb") as f:
                st.download_button(
                    "Download Evidently report (HTML)",
                    data=f,
                    file_name=report.name,
                    mime="text/html",
                )
        except OSError:
            pass
        st.info(
            "The Evidently report contains interactive PSI / KS / JS distributions "
            "per feature. Open it in a browser for the full analysis."
        )
        # List all available reports
        all_reports = sorted(REPORTS_PATH.glob("drift_*.html"), reverse=True)
        if len(all_reports) > 1:
            with st.expander(f"All reports ({len(all_reports)})"):
                for r in all_reports:
                    st.caption(str(r))
    else:
        st.warning(
            "No drift reports yet.  \n"
            "Run: `python -m observability.drift_job`  \n"
            "Or wait for the Dagster `drift_check_schedule` (every 2h)."
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # Auto-refresh
    st.markdown(
        f"<meta http-equiv='refresh' content='{REFRESH_S}'>",
        unsafe_allow_html=True,
    )

    threshold = render_sidebar()

    st.title("🛒 VeloShelf — Dark Store Intelligence")
    st.caption(
        f"Last render: {datetime.now(tz=UTC).strftime('%H:%M:%S UTC')} · "
        f"auto-refreshes every {REFRESH_S}s"
    )

    freshness    = fetch_freshness()
    alert_counts = fetch_alert_counts()
    render_header(freshness, alert_counts)

    st.divider()

    tabs = st.tabs([
        "⚠️ Stockout Risk",
        "🚀 Demand Surges",
        "📦 Category Velocity",
        "🏪 Store Health",
        "📊 Drift",
    ])

    with tabs[0]:
        st.subheader(f"SKUs below {threshold} units on hand")
        render_stockout(threshold)

    with tabs[1]:
        st.subheader(f"Active surge alerts — last {ALERT_WINDOW_MIN} min")
        render_surges()

    with tabs[2]:
        st.subheader("Live order velocity by SKU and store")
        render_velocity()

    with tabs[3]:
        st.subheader("Per-store health summary")
        render_store_health()

    with tabs[4]:
        st.subheader("Data drift — Evidently reports")
        render_drift()


if __name__ == "__main__":
    main()
