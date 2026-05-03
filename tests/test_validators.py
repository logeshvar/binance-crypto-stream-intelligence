import unittest

from producers.validators import (
    validate_kline_event,
    validate_market_event,
    validate_ticker_event,
    validate_trade_event,
)


class ValidatorTests(unittest.TestCase):
    def test_valid_trade_event_passes(self) -> None:
        event = {
            "event_type": "trade",
            "symbol": "BTCUSDT",
            "event_time": "2026-04-30T10:15:30.123Z",
            "trade_id": 123456,
            "price": "62500.12",
            "quantity": "0.005",
        }

        result = validate_trade_event(event)

        self.assertTrue(result.is_valid)

    def test_trade_event_rejects_non_positive_price_and_quantity(self) -> None:
        event = {
            "event_type": "trade",
            "symbol": "BTCUSDT",
            "event_time": "2026-04-30T10:15:30.123Z",
            "trade_id": 123456,
            "price": "0",
            "quantity": "-1",
        }

        result = validate_trade_event(event)

        self.assertFalse(result.is_valid)
        self.assertIn("price must be greater than zero", result.errors)
        self.assertIn("quantity must be greater than zero", result.errors)

    def test_trade_event_rejects_missing_event_time(self) -> None:
        event = {
            "event_type": "trade",
            "symbol": "BTCUSDT",
            "trade_id": 123456,
            "price": "62500.12",
            "quantity": "0.005",
        }

        result = validate_trade_event(event)

        self.assertFalse(result.is_valid)
        self.assertIn("event_time is missing", result.errors)

    def test_valid_kline_event_passes(self) -> None:
        event = {
            "event_type": "kline",
            "symbol": "ETHUSDT",
            "kline_start_time": "2026-04-30T10:15:00Z",
            "open_price": "3120.10",
            "high_price": "3124.00",
            "low_price": "3118.45",
            "close_price": "3122.30",
            "volume": "18.420",
        }

        result = validate_kline_event(event)

        self.assertTrue(result.is_valid)

    def test_kline_event_rejects_non_numeric_price(self) -> None:
        event = {
            "event_type": "kline",
            "symbol": "ETHUSDT",
            "kline_start_time": "2026-04-30T10:15:00Z",
            "open_price": "not-a-number",
            "high_price": "3124.00",
            "low_price": "3118.45",
            "close_price": "3122.30",
            "volume": "18.420",
        }

        result = validate_kline_event(event)

        self.assertFalse(result.is_valid)
        self.assertIn("open_price is not numeric", result.errors)

    def test_kline_event_rejects_negative_volume(self) -> None:
        event = {
            "event_type": "kline",
            "symbol": "ETHUSDT",
            "kline_start_time": "2026-04-30T10:15:00Z",
            "open_price": "3120.10",
            "high_price": "3124.00",
            "low_price": "3118.45",
            "close_price": "3122.30",
            "volume": "-1",
        }

        result = validate_kline_event(event)

        self.assertFalse(result.is_valid)
        self.assertIn("volume must be greater than or equal to zero", result.errors)

    def test_valid_ticker_event_passes(self) -> None:
        event = {
            "event_type": "ticker",
            "symbol": "SOLUSDT",
            "event_time": "2026-04-30T10:15:30.123Z",
            "last_price": "153.70000000",
            "volume": "512430.12000000",
        }

        result = validate_ticker_event(event)

        self.assertTrue(result.is_valid)

    def test_ticker_event_rejects_non_positive_last_price(self) -> None:
        event = {
            "event_type": "ticker",
            "symbol": "SOLUSDT",
            "event_time": "2026-04-30T10:15:30.123Z",
            "last_price": "0",
            "volume": "512430.12000000",
        }

        result = validate_ticker_event(event)

        self.assertFalse(result.is_valid)
        self.assertIn("last_price must be greater than zero", result.errors)

    def test_market_event_rejects_unknown_event_type(self) -> None:
        result = validate_market_event({"event_type": "book_ticker"})

        self.assertFalse(result.is_valid)
        self.assertIn("unsupported event_type: book_ticker", result.errors)


if __name__ == "__main__":
    unittest.main()
