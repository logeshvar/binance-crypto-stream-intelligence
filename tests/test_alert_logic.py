from __future__ import annotations

from decimal import Decimal

from streaming.gold.gold_price_alerts import build_price_movement_alerts
from streaming.gold.gold_trade_summary_5min import TRADE_SUMMARY_SCHEMA
from streaming.gold.gold_volume_spikes import build_volume_spike_records
from tests.silver_trade_fixtures import silver_trades_df, trade_row
from tests.silver_trade_fixtures import utc_ts


def test_price_movement_alerts_detect_price_surge(spark_session):
    trades_df = silver_trades_df(
        spark_session,
        [
            trade_row(event_time="2026-04-30T10:00:00Z", trade_id=1, price="100.00", quantity="1.00"),
            trade_row(event_time="2026-04-30T10:04:59Z", trade_id=2, price="102.00", quantity="1.00"),
        ],
    )

    row = build_price_movement_alerts(trades_df, "10 minutes").collect()[0]

    assert row.symbol == "BTCUSDT"
    assert row.start_price == Decimal("100.000000000000000000")
    assert row.end_price == Decimal("102.000000000000000000")
    assert round(float(row.price_change_pct), 2) == 2.00
    assert row.alert_type == "PRICE_SURGE"
    assert row.severity == "HIGH"


def test_volume_spike_signals_classify_high_ratio(spark_session):
    current_summary_df = spark_session.createDataFrame(
        [
            (
                "BTCUSDT",
                utc_ts("2026-04-30T10:15:00Z"),
                utc_ts("2026-04-30T10:20:00Z"),
                Decimal("10.00"),
                3,
                Decimal("100.00"),
                Decimal("500.00"),
                Decimal("100.00"),
            )
        ],
        schema=TRADE_SUMMARY_SCHEMA,
    )
    historical_summary_df = spark_session.createDataFrame(
        [
            (
                "BTCUSDT",
                utc_ts("2026-04-30T10:00:00Z"),
                utc_ts("2026-04-30T10:05:00Z"),
                Decimal("3.00"),
                2,
                Decimal("100.00"),
                Decimal("200.00"),
                Decimal("100.00"),
            ),
            (
                "BTCUSDT",
                utc_ts("2026-04-30T10:05:00Z"),
                utc_ts("2026-04-30T10:10:00Z"),
                Decimal("2.00"),
                2,
                Decimal("100.00"),
                Decimal("200.00"),
                Decimal("100.00"),
            ),
        ],
        schema=TRADE_SUMMARY_SCHEMA,
    )

    row = build_volume_spike_records(
        current_summary_df,
        historical_summary_df,
        baseline_lookback_minutes=30,
    ).collect()[0]

    assert row.current_volume == Decimal("10.000000000000000000")
    assert round(float(row.baseline_volume), 2) == 2.50
    assert round(float(row.volume_spike_ratio), 2) == 4.00
    assert row.signal_strength == "HIGH"


def test_volume_spike_signals_require_prior_baseline(spark_session):
    current_summary_df = spark_session.createDataFrame(
        [
            (
                "BTCUSDT",
                utc_ts("2026-04-30T10:15:00Z"),
                utc_ts("2026-04-30T10:20:00Z"),
                Decimal("10.00"),
                3,
                Decimal("100.00"),
                Decimal("500.00"),
                Decimal("100.00"),
            )
        ],
        schema=TRADE_SUMMARY_SCHEMA,
    )
    historical_summary_df = spark_session.createDataFrame([], schema=TRADE_SUMMARY_SCHEMA)

    assert build_volume_spike_records(
        current_summary_df,
        historical_summary_df,
        baseline_lookback_minutes=30,
    ).count() == 0
