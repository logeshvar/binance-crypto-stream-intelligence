from __future__ import annotations

from decimal import Decimal

from streaming.silver.transformations import transform_trades
from tests.spark_bronze_fixtures import bronze_df, trade_event


def test_trade_transformations_parse_cast_and_filter_invalid_records(spark_session):
    source_df = bronze_df(
        spark_session,
        "market.trades.raw",
        [
            trade_event(symbol="btcusdt"),
            trade_event(trade_id=999, price="0"),
            trade_event(trade_id=1000, quantity="-1"),
        ],
    )

    rows = transform_trades(source_df).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row.symbol == "BTCUSDT"
    assert row.trade_id == 123456
    assert row.price == Decimal("62500.120000000000000000")
    assert row.quantity == Decimal("0.005000000000000000")
    assert row.trade_value == Decimal("312.500600000000000000")
    assert row.bronze_topic == "market.trades.raw"
    assert row.bronze_partition == 0
    assert row.bronze_offset == 0
    assert row.process_time is not None
