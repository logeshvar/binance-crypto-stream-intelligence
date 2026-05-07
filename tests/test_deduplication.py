from __future__ import annotations

from streaming.silver.transformations import transform_klines, transform_tickers, transform_trades
from tests.spark_bronze_fixtures import bronze_df, kline_event, ticker_event, trade_event


def test_trades_are_deduplicated_by_symbol_and_trade_id(spark_session):
    source_df = bronze_df(
        spark_session,
        "market.trades.raw",
        [
            trade_event(symbol="BTCUSDT", trade_id=1),
            trade_event(symbol="BTCUSDT", trade_id=1),
            trade_event(symbol="ETHUSDT", trade_id=1),
        ],
    )

    rows = transform_trades(source_df).collect()

    assert {(row.symbol, row.trade_id) for row in rows} == {("BTCUSDT", 1), ("ETHUSDT", 1)}


def test_klines_are_deduplicated_by_symbol_and_window_bounds(spark_session):
    source_df = bronze_df(
        spark_session,
        "market.klines.raw",
        [
            kline_event(symbol="ETHUSDT"),
            kline_event(symbol="ETHUSDT"),
            kline_event(symbol="BTCUSDT"),
        ],
    )

    rows = transform_klines(source_df).collect()

    assert len(rows) == 2
    assert {row.symbol for row in rows} == {"ETHUSDT", "BTCUSDT"}


def test_tickers_are_deduplicated_by_symbol_and_event_time(spark_session):
    source_df = bronze_df(
        spark_session,
        "market.tickers.raw",
        [
            ticker_event(symbol="SOLUSDT"),
            ticker_event(symbol="SOLUSDT"),
            ticker_event(symbol="BTCUSDT"),
        ],
    )

    rows = transform_tickers(source_df).collect()

    assert len(rows) == 2
    assert {row.symbol for row in rows} == {"SOLUSDT", "BTCUSDT"}
