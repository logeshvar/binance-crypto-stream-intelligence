import json
import unittest

from producers.config import ProducerConfig
from producers.dlq_publisher import (
    ROUTING_ERROR,
    UNKNOWN_SOURCE_TOPIC,
    build_invalid_event,
    infer_source_topic,
    infer_symbol,
)


class DlqPublisherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ProducerConfig.from_env()

    def test_build_invalid_event_preserves_raw_payload_and_error_context(self) -> None:
        raw_payload = '{"symbol":"BTCUSDT","price":null}'

        event = build_invalid_event(
            config=self.config,
            raw_payload=raw_payload,
            source_topic="market.trades.raw",
            error_type=ROUTING_ERROR,
            error_message="price is missing",
            symbol="BTCUSDT",
        )

        self.assertEqual(event["schema_version"], "1.0")
        self.assertEqual(event["source_topic"], "market.trades.raw")
        self.assertEqual(event["error_type"], ROUTING_ERROR)
        self.assertEqual(event["error_message"], "price is missing")
        self.assertEqual(event["raw_payload"], raw_payload)
        self.assertEqual(event["symbol"], "BTCUSDT")
        self.assertIn("error_time", event)

    def test_infer_source_topic_from_trade_payload(self) -> None:
        raw_payload = json.dumps({"data": {"e": "trade", "s": "BTCUSDT"}})

        source_topic = infer_source_topic(raw_payload, self.config)

        self.assertEqual(source_topic, "market.trades.raw")

    def test_infer_source_topic_returns_unknown_for_malformed_json(self) -> None:
        source_topic = infer_source_topic("{bad-json", self.config)

        self.assertEqual(source_topic, UNKNOWN_SOURCE_TOPIC)

    def test_infer_symbol_from_combined_stream_payload(self) -> None:
        raw_payload = json.dumps({"stream": "btcusdt@trade", "data": {"s": "btcusdt"}})

        symbol = infer_symbol(raw_payload)

        self.assertEqual(symbol, "BTCUSDT")


if __name__ == "__main__":
    unittest.main()
