import json
import unittest

from producers.config import ProducerConfig
from producers.error_context import UNKNOWN_SOURCE_TOPIC, infer_source_topic, infer_symbol


class ErrorContextTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ProducerConfig.from_env()

    def test_infer_source_topic_from_trade_payload(self) -> None:
        raw_payload = json.dumps({"data": {"e": "trade", "s": "BTCUSDT"}})

        source_topic = infer_source_topic(raw_payload, self.config)

        self.assertEqual(source_topic, "market.trades.raw")

    def test_infer_source_topic_from_kline_payload(self) -> None:
        raw_payload = json.dumps({"data": {"e": "kline", "s": "ETHUSDT"}})

        source_topic = infer_source_topic(raw_payload, self.config)

        self.assertEqual(source_topic, "market.klines.raw")

    def test_infer_source_topic_from_ticker_payload(self) -> None:
        raw_payload = json.dumps({"data": {"e": "24hrTicker", "s": "SOLUSDT"}})

        source_topic = infer_source_topic(raw_payload, self.config)

        self.assertEqual(source_topic, "market.tickers.raw")

    def test_infer_source_topic_returns_unknown_for_malformed_json(self) -> None:
        source_topic = infer_source_topic("{bad-json", self.config)

        self.assertEqual(source_topic, UNKNOWN_SOURCE_TOPIC)

    def test_infer_symbol_from_combined_stream_payload(self) -> None:
        raw_payload = json.dumps({"stream": "btcusdt@trade", "data": {"s": "btcusdt"}})

        symbol = infer_symbol(raw_payload)

        self.assertEqual(symbol, "BTCUSDT")

    def test_infer_symbol_returns_none_for_malformed_json(self) -> None:
        symbol = infer_symbol("{bad-json")

        self.assertIsNone(symbol)


if __name__ == "__main__":
    unittest.main()
