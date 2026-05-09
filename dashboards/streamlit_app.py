from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

from dashboards import queries
from streaming.spark_session import create_spark_session


st.set_page_config(
    page_title="Crypto Market Intelligence",
    page_icon="",
    layout="wide",
)


@st.cache_resource
def get_spark():
    os.environ.setdefault("SPARK_UI_PORT", os.getenv("DASHBOARD_SPARK_UI_PORT", "4080"))
    spark = create_spark_session("crypto-market-dashboard")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


@st.cache_data(ttl=15)
def load_dashboard_data(row_limit: int, alert_limit: int):
    spark = get_spark()
    return {
        "overview": queries.overview_metrics(spark),
        "watchlist": queries.latest_watchlist(spark, row_limit),
        "ohlc": queries.latest_ohlc(spark, row_limit),
        "volume_spikes": queries.latest_volume_spikes(spark, row_limit),
        "volatility": queries.latest_volatility(spark, row_limit),
        "price_alerts": queries.latest_price_alerts(spark, row_limit),
        "alert_topic": queries.alert_topic_messages(
            spark,
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("TOPIC_SIGNALS_ALERTS", "market.signals.alerts"),
            limit=alert_limit,
        ),
        "health": queries.table_health(spark),
    }


def render_dataframe(df: pd.DataFrame, height: int = 420) -> None:
    st.dataframe(df, use_container_width=True, height=height, hide_index=True)


def render_overview(data: dict[str, object]) -> None:
    metrics = data["overview"]
    cols = st.columns(5)
    cols[0].metric("Symbols", metrics["symbols"])
    cols[1].metric("High Volume Spikes", metrics["high_volume_spikes"])
    cols[2].metric("Price Alerts", metrics["price_alerts"])
    cols[3].metric("Freshness", metrics["freshness"])
    cols[4].metric("Latest Gold Time", str(metrics["latest_gold_time"]) if metrics["latest_gold_time"] else "n/a")

    watchlist = data["watchlist"]
    if not watchlist.empty:
        chart_df = watchlist[["symbol", "price_change_5m_pct"]].set_index("symbol")
        st.bar_chart(chart_df)
    render_dataframe(watchlist)


def render_watchlist(data: dict[str, object]) -> None:
    render_dataframe(data["watchlist"])
    st.divider()
    render_dataframe(data["ohlc"])


def render_volume_spikes(data: dict[str, object]) -> None:
    spikes = data["volume_spikes"]
    if not spikes.empty:
        chart_df = spikes[["symbol", "volume_spike_ratio"]].head(20).set_index("symbol")
        st.bar_chart(chart_df)
    render_dataframe(spikes)


def render_volatility(data: dict[str, object]) -> None:
    volatility = data["volatility"]
    if not volatility.empty:
        chart_df = volatility[["symbol", "price_change_pct"]].head(20).set_index("symbol")
        st.bar_chart(chart_df)
    render_dataframe(volatility)


def render_alerts(data: dict[str, object]) -> None:
    left, right = st.columns(2)
    with left:
        st.subheader("Gold Price Movement Alerts")
        render_dataframe(data["price_alerts"], height=360)
    with right:
        st.subheader("Kafka Alert Topic")
        render_dataframe(data["alert_topic"], height=360)


def render_health(data: dict[str, object]) -> None:
    health = data["health"].copy()
    if not health.empty:
        health["freshness"] = health["latest_time"].apply(queries.freshness_label)
    render_dataframe(health)


def main() -> None:
    st.title("Crypto Market Intelligence")

    with st.sidebar:
        row_limit = st.slider("Rows", min_value=10, max_value=200, value=50, step=10)
        alert_limit = st.slider("Alert Rows", min_value=10, max_value=200, value=50, step=10)
        st.caption(str(Path.cwd()))
        if st.button("Refresh"):
            st.cache_data.clear()

    data = load_dashboard_data(row_limit=row_limit, alert_limit=alert_limit)

    tabs = st.tabs(
        [
            "Market Overview",
            "Symbol Watchlist",
            "Volume Spikes",
            "Volatility",
            "Alerts",
            "Pipeline Health",
        ]
    )
    with tabs[0]:
        render_overview(data)
    with tabs[1]:
        render_watchlist(data)
    with tabs[2]:
        render_volume_spikes(data)
    with tabs[3]:
        render_volatility(data)
    with tabs[4]:
        render_alerts(data)
    with tabs[5]:
        render_health(data)


if __name__ == "__main__":
    main()
