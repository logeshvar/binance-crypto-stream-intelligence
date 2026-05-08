from __future__ import annotations

from streaming.gold.gold_trade_summary_5min import build_trade_summary_5min
from streaming.gold.gold_volatility_5min import build_volatility_5min
from streaming.gold.gold_watchlist_summary import build_watchlist_summary
from tests.silver_trade_fixtures import silver_trades_df, trade_row


def test_trade_summary_5min_calculates_window_metrics(spark_session):
    trades_df = silver_trades_df(
        spark_session,
        [
            trade_row(event_time="2026-04-30T10:00:00Z", trade_id=1, price="100.00", quantity="1.00"),
            trade_row(event_time="2026-04-30T10:02:00Z", trade_id=2, price="120.00", quantity="2.00"),
            trade_row(event_time="2026-04-30T10:04:00Z", trade_id=3, price="90.00", quantity="3.00"),
        ],
    )

    row = build_trade_summary_5min(trades_df, "10 minutes").collect()[0]

    assert row.number_of_trades == 3
    assert float(row.total_volume) == 6.0
    assert round(float(row.average_trade_value), 2) == 203.33
    assert float(row.max_trade_value) == 270.0
    assert float(row.min_trade_value) == 100.0


def test_volatility_5min_classifies_high_price_movement(spark_session):
    trades_df = silver_trades_df(
        spark_session,
        [
            trade_row(event_time="2026-04-30T10:00:00Z", trade_id=1, price="100.00", quantity="1.00"),
            trade_row(event_time="2026-04-30T10:04:59Z", trade_id=2, price="103.00", quantity="1.00"),
        ],
    )

    row = build_volatility_5min(trades_df, "10 minutes").collect()[0]

    assert round(row.price_change_pct, 2) == 3.00
    assert row.volatility_level == "HIGH"


def test_watchlist_summary_emits_latest_symbol_snapshot(spark_session):
    trades_df = silver_trades_df(
        spark_session,
        [
            trade_row(event_time="2026-04-30T10:00:00Z", trade_id=1, price="100.00", quantity="2.00"),
            trade_row(event_time="2026-04-30T10:03:00Z", trade_id=2, price="102.00", quantity="3.00"),
        ],
    )

    row = build_watchlist_summary(trades_df, "10 minutes").collect()[0]

    assert float(row.latest_price) == 102.0
    assert round(row.price_change_5m_pct, 2) == 2.00
    assert float(row.volume_5m) == 5.0
    assert row.volatility_level == "HIGH"
    assert row.latest_signal == "PRICE_SURGE"
