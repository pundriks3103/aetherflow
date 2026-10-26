#!/usr/bin/env python3
"""A read-only Streamlit view of AetherFlow's hourly crypto analytics."""

import base64
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from aetherflow_config import load_config
from crypto_market_analytics import spark_session
from metrics_calculations import calculate_partition_offsets, timestamp_freshness
from operational_metrics import _data_probe


CONFIG = load_config(context="compose")
REFRESH_SECONDS = CONFIG["dashboard"]["refresh_seconds"]
METRICS_REFRESH_SECONDS = CONFIG["dashboard"]["metrics_refresh_seconds"]
STALE_AFTER_SECONDS = CONFIG["dashboard"]["freshness_stale_after_seconds"]
RECENT_WINDOWS = CONFIG["dashboard"]["recent_windows"]


@st.cache_data(ttl=METRICS_REFRESH_SECONDS, show_spinner=False)
def operational_snapshot():
    """Reuse the read-only Kafka/Bronze/Silver/quarantine metrics probe."""
    try:
        snapshot = _data_probe()
        snapshot["dashboard_measured_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        return snapshot
    except Exception as exc:
        return {"kafka": {"status": "unavailable", "partitions": {}}, "bronze": {"status": "unavailable"},
                "silver": {"status": "unavailable"}, "analytics": {"status": "unavailable"},
                "quarantine": {"status": "unavailable"}, "error": f"{type(exc).__name__}: {exc}"}


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def analytics_snapshot():
    """Read the existing Delta aggregate table through the configured S3A helper."""
    spark = None
    try:
        spark = spark_session("local[2]", CONFIG, app_name="AetherFlow-Dashboard-Read")
        spark.sparkContext.setLogLevel("ERROR")
        table = spark.read.format("delta").load(CONFIG["analytics"]["output"])
        expected = {
            "symbol", "window_start", "window_end", "trade_count", "base_volume", "quote_notional",
            "vwap", "first_price", "last_price", "high_price", "low_price",
        }
        missing = sorted(expected - set(table.columns))
        if missing:
            return {"status": "unavailable", "error": f"Analytics table is missing columns: {', '.join(missing)}", "rows": []}
        rows = table.orderBy("window_start", "symbol").limit(500).collect()
        data = [{
            "symbol": row["symbol"],
            "window_start": row["window_start"].replace(tzinfo=timezone.utc),
            "window_end": row["window_end"].replace(tzinfo=timezone.utc),
            "trade_count": int(row["trade_count"]),
            "base_volume": str(row["base_volume"]),
            "quote_notional": str(row["quote_notional"]),
            "vwap": str(row["vwap"]),
            "first_price": str(row["first_price"]),
            "last_price": str(row["last_price"]),
            "high_price": str(row["high_price"]),
            "low_price": str(row["low_price"]),
        } for row in rows]
        return {"status": "available", "row_count": table.count(), "rows": data, "path": CONFIG["analytics"]["output"]}
    except Exception as exc:
        return {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}", "rows": []}
    finally:
        if spark is not None:
            spark.stop()


def latest_successful_silver_task():
    """Get Airflow's actual successful task end time using its REST API."""
    username = os.environ.get("AIRFLOW_ADMIN_USER")
    password = os.environ.get("AIRFLOW_ADMIN_PASSWORD")
    if not username or not password:
        return {"status": "unavailable", "error": "Airflow API credentials are not configured"}
    root = "http://airflow-webserver:8080/api/v1"
    dag_id = quote(CONFIG["airflow"]["dag_id"], safe="")
    auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")

    def get_json(path):
        request = Request(f"{root}{path}", headers={"Authorization": f"Basic {auth}", "Accept": "application/json"})
        with urlopen(request, timeout=8) as response:
            return json.loads(response.read().decode("utf-8"))

    try:
        runs = get_json(f"/dags/{dag_id}/dagRuns?state=success&order_by=-end_date&limit=25").get("dag_runs", [])
        for run in runs:
            run_id = run.get("dag_run_id")
            if not run_id:
                continue
            task_id = quote("silver_transform", safe="")
            encoded_run = quote(run_id, safe="")
            task = get_json(f"/dags/{dag_id}/dagRuns/{encoded_run}/taskInstances/{task_id}")
            if task.get("state") == "success" and task.get("end_date"):
                return {"status": "available", "run_id": run_id, "end_time_utc": task["end_date"]}
        return {"status": "no_successful_task", "end_time_utc": None}
    except HTTPError as exc:
        return {"status": "unavailable", "error": f"Airflow API returned HTTP {exc.code}"}
    except (URLError, TimeoutError, ValueError, OSError) as exc:
        return {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}"}


def _money(value, scale=8):
    if value is None:
        return "—"
    try:
        number = Decimal(str(value))
        return f"{number:,.{scale}f}"
    except Exception:
        return "—"


def _utc(value):
    if value is None:
        return "Unknown"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return "Unknown"


def _age_label(metric):
    if metric["status"] == "future_clock_skew":
        return f"Clock skew · {metric['clock_skew_seconds']:.0f}s ahead"
    age = metric.get("age_seconds")
    if age is None:
        return metric["status"].replace("_", " ").title()
    if age < 3600:
        return f"{age / 60:.0f} min ago"
    return f"{age / 3600:.1f} hr ago"


def _freshness_status(metric):
    if metric["status"] != "available":
        return "unknown"
    return "stale" if metric["age_seconds"] > STALE_AFTER_SECONDS else "fresh"


def _partition_lag(snapshot):
    kafka = snapshot.get("kafka", {})
    kafka_offsets = {
        int(partition): offsets for partition, offsets in kafka.get("partitions", {}).items()
    }
    bronze_offsets = {
        int(partition): value for partition, value in snapshot.get("bronze", {}).get("offsets", {}).items()
    }
    return calculate_partition_offsets(kafka_offsets, bronze_offsets)


def _plot_price(rows):
    x = [row["window_start"] for row in rows]
    high = [float(row["high_price"]) for row in rows]
    low = [float(row["low_price"]) for row in rows]
    vwap = [float(row["vwap"]) for row in rows]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=high, mode="lines", line={"width": 0}, hoverinfo="skip", showlegend=False))
    fig.add_trace(go.Scatter(x=x, y=low, mode="lines", line={"width": 0}, fill="tonexty",
                             fillcolor="rgba(98,230,199,0.13)", name="Hourly range", hovertemplate="Low %{y:,.8f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=x, y=vwap, mode="lines+markers", name="VWAP",
                             line={"color": "#62e6c7", "width": 3}, marker={"size": 6},
                             hovertemplate="VWAP %{y:,.8f}<extra></extra>"))
    fig.update_layout(height=330, margin={"l": 8, "r": 8, "t": 14, "b": 8},
                      legend={"orientation": "h", "y": 1.12}, hovermode="x unified",
                      xaxis_title="Window start · UTC", yaxis_title="Quote currency")
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(160,180,190,.12)")
    return fig


def _plot_activity(rows):
    x = [row["window_start"] for row in rows]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=x, y=[row["trade_count"] for row in rows], name="Trades",
                         marker_color="rgba(98,230,199,.65)", hovertemplate="%{y:,} trades<extra></extra>"), secondary_y=False)
    fig.add_trace(go.Scatter(x=x, y=[float(row["base_volume"]) for row in rows], name="Base volume",
                             mode="lines+markers", line={"color": "#82aaff", "width": 2},
                             hovertemplate="%{y:,.6f} base<extra></extra>"), secondary_y=True)
    fig.update_layout(height=330, margin={"l": 8, "r": 8, "t": 14, "b": 8},
                      legend={"orientation": "h", "y": 1.12}, hovermode="x unified",
                      xaxis_title="Window start · UTC", barmode="overlay")
    fig.update_yaxes(title_text="Trade count", secondary_y=False, gridcolor="rgba(160,180,190,.12)")
    fig.update_yaxes(title_text="Base volume", secondary_y=True, showgrid=False)
    fig.update_xaxes(showgrid=False)
    return fig


def _status_label(status):
    return {"available": "Available", "unavailable": "Unavailable", "empty_table": "Empty", "no_valid_events": "No valid events"}.get(status, status.replace("_", " ").title())


def _render_dashboard():
    now = datetime.now(timezone.utc)
    snapshot = operational_snapshot()
    market = analytics_snapshot()
    airflow = latest_successful_silver_task()
    records = market.get("rows", [])
    recent = records[-RECENT_WINDOWS:]
    latest = records[-1] if records else None
    bronze_freshness = timestamp_freshness(
        snapshot.get("bronze", {}).get("latest_valid_exchange_event_time_utc"), now,
        snapshot.get("bronze", {}).get("freshness_source_status", "available"),
    )
    silver_freshness = timestamp_freshness(
        snapshot.get("silver", {}).get("latest_valid_exchange_event_time_utc"), now,
        snapshot.get("silver", {}).get("freshness_source_status", "available"),
    )
    lag = _partition_lag(snapshot)
    lag_values = [row["offset_position_lag"] for row in lag.values() if row["offset_position_lag"] is not None]
    all_lag_known = bool(lag) and len(lag_values) == len(lag)
    total_lag = sum(lag_values) if all_lag_known else None
    retention_gap = any(row["retention_gap"] is True for row in lag.values())

    st.markdown("<div class='eyebrow'>AETHERFLOW · MARKET INTELLIGENCE</div>", unsafe_allow_html=True)
    st.title("Crypto market overview")
    if latest:
        st.caption(f"{latest['symbol'].upper()} · hourly Delta snapshot · latest window {_utc(latest['window_start'])} — {_utc(latest['window_end'])}")
    else:
        st.caption("Hourly analytics from the existing Silver trade table")

    if market.get("status") != "available":
        st.error(f"Analytics table unavailable: {market.get('error', 'No table data returned')}")
    elif not latest:
        st.info("The analytics table is available but has no hourly windows yet. Run `make crypto-analytics` after Silver has trades.")

    if latest:
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Last recorded price", _money(latest["last_price"]), help="Last trade price in the latest hourly window; this is not a live quote.")
        kpi2.metric("VWAP · latest hour", _money(latest["vwap"]))
        kpi3.metric("Base volume · latest hour", _money(latest["base_volume"]))
        kpi4.metric("Quote notional · latest hour", _money(latest["quote_notional"], 2))
        st.caption("The latest recorded price comes from the most recent aggregate window. It may be stale and is not a live exchange price.")

    left, right = st.columns([1.4, 1])
    with left:
        st.subheader("Price movement")
        if recent:
            st.plotly_chart(_plot_price(recent), use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("Price history will appear when hourly aggregates are available.")
    with right:
        st.subheader("Market activity")
        if recent:
            st.plotly_chart(_plot_activity(recent), use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("Trade count and volume will appear when hourly aggregates are available.")

    st.subheader("Recent hourly aggregates")
    if recent:
        display_rows = [{
            "Symbol": row["symbol"].upper(), "Window start (UTC)": _utc(row["window_start"]),
            "Trades": row["trade_count"], "Base volume": row["base_volume"],
            "Quote notional": row["quote_notional"], "VWAP": row["vwap"],
            "First": row["first_price"], "Last": row["last_price"],
            "Low": row["low_price"], "High": row["high_price"],
        } for row in reversed(recent)]
        st.dataframe(display_rows, use_container_width=True, hide_index=True)
    else:
        st.info("No aggregate rows to display.")

    st.markdown("### Pipeline health")
    health1, health2, health3, health4, health5 = st.columns(5)
    bronze_freshness_status = _freshness_status(bronze_freshness)
    health1.metric("Bronze feed freshness", _age_label(bronze_freshness), delta=bronze_freshness_status,
                   delta_color="normal" if bronze_freshness_status == "fresh" else "inverse")
    silver_freshness_status = _freshness_status(silver_freshness)
    health2.metric("Silver feed freshness", _age_label(silver_freshness), delta=silver_freshness_status,
                   delta_color="normal" if silver_freshness_status == "fresh" else "inverse")
    lag_value = f"{total_lag:,}" if total_lag is not None else "Unknown"
    lag_delta = "Retention gap" if retention_gap else f"{len(lag)} partitions" if lag else "Unavailable"
    health3.metric("Kafka offset-position lag", lag_value, delta=lag_delta,
                   delta_color="inverse" if retention_gap or (total_lag not in (None, 0)) else "normal")
    silver = snapshot.get("silver", {})
    silver_status = _status_label(silver.get("status", "unavailable"))
    silver_value = f"{silver.get('row_count'):,} rows" if silver.get("row_count") is not None else silver_status
    health4.metric("Silver table", silver_value, delta=silver_status)
    analytics_status = market.get("status", "unavailable")
    analytics_value = f"{market.get('row_count', 0):,} windows" if analytics_status == "available" else _status_label(analytics_status)
    health5.metric("Analytics table", analytics_value, delta=_status_label(analytics_status))
    st.caption(
        f"Freshness source: Binance exchange notification timestamp (E) · "
        f"stale after {STALE_AFTER_SECONDS // 60} minutes. "
        f"Operational probes last checked {_utc(snapshot.get('dashboard_measured_at_utc'))}. "
        "Offset-position lag is a position difference, not an exact count of unprocessed records."
    )

    airflow_end = airflow.get("end_time_utc")
    airflow_freshness = timestamp_freshness(airflow_end, now, "no_successful_run" if airflow.get("status") == "no_successful_task" else "available")
    if airflow.get("status") == "available":
        st.caption(f"Last successful Silver task: {_utc(airflow_end)} · {_age_label(airflow_freshness)} · run `{airflow.get('run_id', 'unknown')}`")
    elif airflow.get("status") == "no_successful_task":
        st.caption("Last successful Silver task: no successful task run found.")
    else:
        st.caption(f"Last successful Silver task: unavailable · {airflow.get('error', 'Airflow API unavailable')}")

    with st.expander("Partition offsets and table details"):
        st.write(f"Analytics Delta path: `{CONFIG['analytics']['output']}`")
        st.write(f"Bronze status: {_status_label(snapshot.get('bronze', {}).get('status', 'unavailable'))}")
        st.write(f"Quarantine: {_status_label(snapshot.get('quarantine', {}).get('status', 'unavailable'))} · {snapshot.get('quarantine', {}).get('total_records', 'unknown')} records")
        if lag:
            st.dataframe([{"Partition": part, **metric} for part, metric in sorted(lag.items())], use_container_width=True, hide_index=True)
        if market.get("error"):
            st.warning(market["error"])
        if snapshot.get("error"):
            st.warning(snapshot["error"])


def render_app():
    st.set_page_config(page_title="AetherFlow Crypto Analytics", page_icon="◈", layout="wide")
    st.markdown("""
    <style>
    .stApp { background: radial-gradient(ellipse at 12% 0%, rgba(39, 86, 88, .18), transparent 35%), #0b1117; }
    .block-container { max-width: 1500px; padding-top: 2.2rem; padding-bottom: 3rem; }
    .eyebrow { color: #62e6c7; letter-spacing: .18em; font-size: .72rem; font-weight: 700; margin-bottom: -.5rem; }
    h1, h2, h3 { letter-spacing: -.025em; }
    [data-testid="stMetric"] { background: linear-gradient(145deg, rgba(22,34,43,.96), rgba(14,23,31,.96)); border: 1px solid rgba(144,177,185,.13); padding: 1rem 1.1rem; border-radius: 14px; }
    [data-testid="stMetricValue"] { color: #f1f7f7; font-variant-numeric: tabular-nums; }
    [data-testid="stMetricLabel"] { color: #9eafb7; }
    [data-testid="stDataFrame"] { border: 1px solid rgba(144,177,185,.13); border-radius: 12px; overflow: hidden; }
    </style>
    """, unsafe_allow_html=True)
    with st.sidebar:
        st.markdown("## ◈ AetherFlow")
        st.caption("Crypto market analytics")
        st.markdown(f"**Refresh interval**  ·  {REFRESH_SECONDS}s")
        st.caption(f"Kafka and pipeline checks refresh every {METRICS_REFRESH_SECONDS // 60} min.")
        st.caption("Read-only snapshot. Analytics is not rebuilt by this page.")
        if st.button("↻  Refresh now", use_container_width=True):
            operational_snapshot.clear()
            analytics_snapshot.clear()
            st.rerun()
        st.divider()
        st.caption(f"Measured at {_utc(datetime.now(timezone.utc))}")

    st.fragment(run_every=f"{REFRESH_SECONDS}s")(_render_dashboard)()


def smoke_check():
    operational = operational_snapshot()
    market = analytics_snapshot()
    airflow = latest_successful_silver_task()
    result = {
        "analytics_status": market.get("status"),
        "analytics_rows": market.get("row_count"),
        "sample_symbol": market.get("rows", [{}])[-1].get("symbol") if market.get("rows") else None,
        "silver_status": operational.get("silver", {}).get("status"),
        "kafka_status": operational.get("kafka", {}).get("status"),
        "airflow_status": airflow.get("status"),
    }
    print("AETHERFLOW_DASHBOARD_SMOKE=" + json.dumps(result, sort_keys=True))
    return 0 if result["analytics_status"] == "available" and (result["analytics_rows"] or 0) > 0 else 1


if __name__ == "__main__":
    if "--smoke" in sys.argv:
        sys.exit(smoke_check())
    render_app()
