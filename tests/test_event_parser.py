import json
import unittest

from producers.config import ProducerConfig
from producers.event_router import route_binance_message


class BinanceEventRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ProducerConfig.from_env()

    def test_routes_trade_event_to_trade_topic(self) -> None:
        message = {
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "E": 1672515782136,
                "s": "BTCUSDT",
                "t": 12345,
                "p": "0.001",
                "q": "100",
                "T": 1672515782136,
                "m": True,
                "M": True,
            },
        }

        event = route_binance_message(json.dumps(message), self.config)

        self.assertEqual(event.topic, "market.trades.raw")
        self.assertEqual(event.key, "BTCUSDT")
        self.assertEqual(event.event_type, "trade")
        self.assertEqual(event.value["trade_value"], "0.100")

    def test_routes_kline_event_to_kline_topic(self) -> None:
        message = {
            "stream": "ethusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1672515782136,
                "s": "ETHUSDT",
                "k": {
                    "t": 1672515780000,
                    "T": 1672515839999,
                    "s": "ETHUSDT",
                    "i": "1m",
                    "f": 100,
                    "L": 200,
                    "o": "0.0010",
                    "c": "0.0020",
                    "h": "0.0025",
                    "l": "0.0015",
                    "v": "1000",
                    "n": 100,
                    "x": False,
                    "q": "1.0000",
                    "V": "500",
                    "Q": "0.500",
                    "B": "123456",
                },
            },
        }

        event = route_binance_message(json.dumps(message), self.config)

        self.assertEqual(event.topic, "market.klines.raw")
        self.assertEqual(event.key, "ETHUSDT")
        self.assertEqual(event.event_type, "kline")
        self.assertEqual(event.value["interval"], "1m")

    def test_routes_ticker_event_to_ticker_topic(self) -> None:
        message = {
            "stream": "solusdt@ticker",
            "data": {
                "e": "24hrTicker",
                "E": 1672515782136,
                "s": "SOLUSDT",
                "p": "0.0015",
                "P": "250.00",
                "w": "0.0018",
                "x": "0.0009",
                "c": "0.0025",
                "Q": "10",
                "b": "0.0024",
                "B": "10",
                "a": "0.0026",
                "A": "100",
                "o": "0.0010",
                "h": "0.0025",
                "l": "0.0010",
                "v": "10000",
                "q": "18",
                "O": 0,
                "C": 86400000,
                "F": 0,
                "L": 18150,
                "n": 18151,
            },
        }

        event = route_binance_message(json.dumps(message), self.config)

        self.assertEqual(event.topic, "market.tickers.raw")
        self.assertEqual(event.key, "SOLUSDT")
        self.assertEqual(event.event_type, "ticker")
        self.assertEqual(event.value["trade_count"], 18151)


if __name__ == "__main__":
    unittest.main()
