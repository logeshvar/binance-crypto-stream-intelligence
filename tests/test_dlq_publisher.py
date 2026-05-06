import unittest

from producers.config import ProducerConfig
from producers.dlq_publisher import (
    ROUTING_ERROR,
    build_invalid_event,
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


if __name__ == "__main__":
    unittest.main()
