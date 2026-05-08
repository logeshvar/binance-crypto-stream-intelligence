from __future__ import annotations

from decimal import Decimal

from streaming.gold.gold_ohlc_1min import build_ohlc_1min
from tests.silver_trade_fixtures import silver_trades_df, trade_row, utc_ts


def test_ohlc_uses_event_time_open_and_close_prices(spark_session):
    trades_df = silver_trades_df(
        spark_session,
        [
            trade_row(event_time="2026-04-30T10:00:20Z", trade_id=2, price="110.00", quantity="2.00"),
            trade_row(event_time="2026-04-30T10:00:05Z", trade_id=1, price="100.00", quantity="1.00"),
            trade_row(event_time="2026-04-30T10:00:50Z", trade_id=3, price="105.00", quantity="3.00"),
        ],
    )

    row = build_ohlc_1min(trades_df, "10 minutes").collect()[0]

    assert row.symbol == "BTCUSDT"
    assert row.window_start == utc_ts("2026-04-30T10:00:00Z")
    assert row.window_end == utc_ts("2026-04-30T10:01:00Z")
    assert row.open_price == Decimal("100.000000000000000000")
    assert row.high_price == Decimal("110.000000000000000000")
    assert row.low_price == Decimal("100.000000000000000000")
    assert row.close_price == Decimal("105.000000000000000000")
    assert row.trade_count == 3
    assert row.total_quantity == Decimal("6.000000000000000000")
    assert row.total_trade_value == Decimal("635.000000000000000000")
