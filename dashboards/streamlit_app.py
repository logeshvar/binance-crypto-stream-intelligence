from __future__ import annotations

import os

import pandas as pd
import streamlit as st

from dashboards import queries
from streaming.spark_session import create_spark_session


ALERT_DISPLAY_COLUMNS = [
    "symbol",
    "alert_type",
    "severity",
    "metric_value",
    "description",
    "window_start",
    "window_end",
    "created_at",
]


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
def load_market_data(row_limit: int, alert_limit: int, time_range_key: str):
    spark = get_spark()
    start_time = queries.time_range_start(time_range_key)
    snapshot = queries.market_snapshot(spark, row_limit, start_time=start_time)
    volume_spikes = queries.latest_volume_spikes(spark, row_limit, start_time=start_time)
    price_alerts = queries.latest_price_alerts(spark, row_limit, start_time=start_time)
    health = queries.table_health(spark, start_time=start_time)

    return {
        "overview": queries.build_overview_metrics(snapshot, volume_spikes, price_alerts, health),
        "snapshot": snapshot,
        "price_alerts": price_alerts,
        "published_alerts": queries.alert_topic_messages(
            spark,
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("TOPIC_SIGNALS_ALERTS", "market.signals.alerts"),
            limit=alert_limit,
            start_time=start_time,
        ),
        "health": health,
        "time_range_caption": queries.time_range_caption(time_range_key, start_time),
    }


@st.cache_data(ttl=15)
def load_symbol_data(symbol: str, time_range_key: str):
    spark = get_spark()
    start_time = queries.time_range_start(time_range_key)
    return {
        "ohlc": queries.symbol_ohlc_history(spark, symbol, start_time=start_time),
        "trade_summary": queries.symbol_trade_summary_history(spark, symbol, start_time=start_time),
        "spikes": queries.symbol_spike_history(spark, symbol, start_time=start_time),
    }


def numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(df[column], errors="coerce")


def render_dataframe(df: pd.DataFrame, height: int = 420) -> None:
    st.dataframe(df, width="stretch", height=height, hide_index=True)


def display_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    return df[[column for column in columns if column in df.columns]]


def metric_value(value: object, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.2f}{suffix}"
    return f"{value}{suffix}"


def safe_float(value: object) -> float | None:
    converted = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(converted) else float(converted)


def top_row(df: pd.DataFrame, column: str) -> pd.Series | None:
    if df.empty or column not in df:
        return None
    ranked = df.assign(_rank=pd.to_numeric(df[column], errors="coerce")).sort_values("_rank", ascending=False)
    return ranked.iloc[0] if not ranked.empty else None


def render_command_center(data: dict[str, object]) -> None:
    metrics = data["overview"]
    snapshot = data["snapshot"]
    st.caption(data["time_range_caption"])

    strongest_spike = top_row(snapshot, "volume_spike_ratio")
    biggest_move = None
    if not snapshot.empty:
        move_rank = snapshot.assign(_abs_move=numeric_series(snapshot, "price_change_5m_pct").abs()).sort_values(
            "_abs_move", ascending=False
        )
        biggest_move = move_rank.iloc[0]

    cols = st.columns(5)
    cols[0].metric("Active Symbols", metrics["symbols"])
    cols[1].metric("Strongest Spike", metric_value(metrics["strongest_spike"], "x"))
    cols[2].metric("Biggest 5m Move", metric_value(metrics["biggest_abs_move"], "%"))
    cols[3].metric("High Spike Windows", metrics["high_volume_spikes"])
    cols[4].metric("Gold Freshness", metrics["freshness"])

    left, right = st.columns([1.1, 1])
    with left:
        st.subheader("Attention Ranked Symbols")
        display_cols = [
            "symbol",
            "attention_score",
            "latest_price",
            "price_change_5m_pct",
            "volume_5m",
            "volume_spike_ratio",
            "signal_strength",
            "volatility_level",
            "market_direction",
            "attention_reason",
            "last_updated_time",
        ]
        render_dataframe(snapshot[[col for col in display_cols if col in snapshot.columns]], height=430)

    with right:
        st.subheader("Current Leaders")
        leader_rows = []
        if strongest_spike is not None:
            leader_rows.append(
                {
                    "signal": "Strongest volume spike",
                    "symbol": strongest_spike["symbol"],
                    "value": metric_value(safe_float(strongest_spike.get("volume_spike_ratio")), "x"),
                }
            )
        if biggest_move is not None:
            leader_rows.append(
                {
                    "signal": "Largest 5m price move",
                    "symbol": biggest_move["symbol"],
                    "value": metric_value(abs(safe_float(biggest_move.get("price_change_5m_pct")) or 0.0), "%"),
                }
            )
        render_dataframe(pd.DataFrame(leader_rows), height=150)

        if not snapshot.empty:
            spike_chart = snapshot[["symbol", "volume_spike_ratio"]].dropna().head(12).copy()
            spike_chart["volume_spike_ratio"] = numeric_series(spike_chart, "volume_spike_ratio")
            move_chart = snapshot[["symbol", "price_change_5m_pct"]].dropna().head(12).copy()
            move_chart["price_change_5m_pct"] = numeric_series(move_chart, "price_change_5m_pct")
            spike_chart = spike_chart.set_index("symbol")
            move_chart = move_chart.set_index("symbol")
            st.bar_chart(spike_chart)
            st.bar_chart(move_chart)


def render_symbol_drilldown(snapshot: pd.DataFrame, symbol_data: dict[str, pd.DataFrame], selected_symbol: str) -> None:
    st.caption(f"{selected_symbol} | filtered to the selected dashboard time range")
    current = snapshot[snapshot["symbol"] == selected_symbol]
    if not current.empty:
        row = current.iloc[0]
        cols = st.columns(5)
        cols[0].metric("Latest Price", metric_value(safe_float(row.get("latest_price"))))
        cols[1].metric("5m Move", metric_value(safe_float(row.get("price_change_5m_pct")), "%"))
        cols[2].metric("5m Volume", metric_value(safe_float(row.get("volume_5m"))))
        cols[3].metric("Spike Ratio", metric_value(safe_float(row.get("volume_spike_ratio")), "x"))
        cols[4].metric("Volatility", row.get("volatility_level", "n/a"))

    ohlc = symbol_data["ohlc"]
    summary = symbol_data["trade_summary"]
    spikes = symbol_data["spikes"]

    left, right = st.columns([1.15, 1])
    with left:
        st.subheader("1m Close Price")
        if not ohlc.empty:
            chart_df = ohlc.copy()
            chart_df["close_price"] = numeric_series(chart_df, "close_price")
            st.line_chart(chart_df.set_index("window_end")[["close_price"]])
        render_dataframe(ohlc.tail(30), height=300)

    with right:
        st.subheader("5m Volume And Spike Ratio")
        if not summary.empty:
            volume_df = summary.copy()
            volume_df["total_volume"] = numeric_series(volume_df, "total_volume")
            st.bar_chart(volume_df.set_index("window_end")[["total_volume"]])
        if not spikes.empty:
            spike_df = spikes.copy()
            spike_df["volume_spike_ratio"] = numeric_series(spike_df, "volume_spike_ratio")
            st.line_chart(spike_df.set_index("window_end")[["volume_spike_ratio"]])
        render_dataframe(spikes.tail(30), height=300)


def render_alerts(data: dict[str, object]) -> None:
    published_alerts = display_columns(data["published_alerts"], ALERT_DISPLAY_COLUMNS)
    price_alerts = data["price_alerts"]
    st.caption(data["time_range_caption"])

    if not published_alerts.empty:
        cols = st.columns(4)
        cols[0].metric("Published Alerts", len(published_alerts))
        cols[1].metric("Symbols Alerted", published_alerts["symbol"].nunique())
        cols[2].metric("High Severity", int((published_alerts["severity"] == "HIGH").sum()))
        cols[3].metric("Latest Alert", str(published_alerts["created_at"].max()))

    left, right = st.columns([1.1, 1])
    with left:
        st.subheader("Published Alert History")
        render_dataframe(published_alerts, height=440)
    with right:
        st.subheader("Gold Price Alerts")
        render_dataframe(price_alerts, height=440)


def render_health(data: dict[str, object]) -> None:
    health = data["health"].copy()
    st.caption(data["time_range_caption"])
    if not health.empty:
        health["freshness"] = health["latest_time"].apply(queries.freshness_label)
        chart_df = health[["table", "row_count"]].set_index("table")
        st.bar_chart(chart_df)
    render_dataframe(health)


def main() -> None:
    st.title("Crypto Market Intelligence")

    with st.sidebar:
        time_range_key = st.selectbox(
            "Time Range",
            list(queries.TIME_RANGE_LABELS.keys()),
            format_func=lambda value: queries.TIME_RANGE_LABELS[value],
        )
        row_limit = st.slider("Rows", min_value=25, max_value=200, value=100, step=25)
        alert_limit = st.slider("Alert Rows", min_value=25, max_value=200, value=100, step=25)
        if st.button("Refresh"):
            st.cache_data.clear()
            st.rerun()

    data = load_market_data(row_limit=row_limit, alert_limit=alert_limit, time_range_key=time_range_key)
    snapshot = data["snapshot"]
    symbols = sorted(snapshot["symbol"].dropna().unique().tolist()) if not snapshot.empty else []

    with st.sidebar:
        selected_symbol = st.selectbox("Symbol", symbols) if symbols else None

    symbol_data = (
        load_symbol_data(selected_symbol, time_range_key)
        if selected_symbol
        else {"ohlc": pd.DataFrame(), "trade_summary": pd.DataFrame(), "spikes": pd.DataFrame()}
    )

    tabs = st.tabs(["Command Center", "Symbol Drilldown", "Alerts", "Pipeline Health"])
    with tabs[0]:
        render_command_center(data)
    with tabs[1]:
        if selected_symbol:
            render_symbol_drilldown(snapshot, symbol_data, selected_symbol)
        else:
            st.info("No symbols available yet.")
    with tabs[2]:
        render_alerts(data)
    with tabs[3]:
        render_health(data)


if __name__ == "__main__":
    main()
