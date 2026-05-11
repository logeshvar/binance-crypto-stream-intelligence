from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from dashboards.queries import add_attention_score
from dashboards.queries import build_overview_metrics
from dashboards.queries import delta_table_exists
from dashboards.queries import freshness_label
from dashboards.queries import get_gold_table_paths
from dashboards.queries import price_change_direction
from dashboards.queries import time_range_caption
from dashboards.queries import time_range_start


def test_get_gold_table_paths_uses_supplied_root(tmp_path):
    paths = get_gold_table_paths(tmp_path)

    assert paths["gold_symbol_1min_ohlc"] == tmp_path / "gold_symbol_1min_ohlc"
    assert paths["gold_market_watchlist_summary"] == tmp_path / "gold_market_watchlist_summary"


def test_delta_table_exists_checks_delta_log(tmp_path):
    table_path = tmp_path / "gold_table"
    table_path.mkdir()
    assert not delta_table_exists(table_path)

    (table_path / "_delta_log").mkdir()
    assert delta_table_exists(table_path)


def test_freshness_label_classifies_event_age():
    now = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)

    assert freshness_label(now - timedelta(seconds=60), now=now) == "FRESH"
    assert freshness_label(now - timedelta(minutes=5), now=now) == "LAGGING"
    assert freshness_label(now - timedelta(hours=1), now=now) == "STALE"
    assert freshness_label(None, now=now) == "NO_DATA"


def test_add_attention_score_ranks_symbols_by_actionable_signals():
    snapshot = pd.DataFrame(
        [
            {
                "symbol": "SOLUSDT",
                "price_change_5m_pct": 0.02,
                "volume_5m": 100,
                "volume_spike_ratio": 1.0,
                "signal_strength": "LOW",
                "volatility_level": "LOW",
            },
            {
                "symbol": "BTCUSDT",
                "price_change_5m_pct": 0.3,
                "volume_5m": 300,
                "volume_spike_ratio": 2.4,
                "signal_strength": "HIGH",
                "volatility_level": "MEDIUM",
            },
            {
                "symbol": "ETHUSDT",
                "price_change_5m_pct": -1.8,
                "volume_5m": 250,
                "volume_spike_ratio": 1.1,
                "signal_strength": "LOW",
                "volatility_level": "HIGH",
            },
        ]
    )

    ranked = add_attention_score(snapshot)

    assert ranked.iloc[0]["symbol"] == "BTCUSDT"
    assert ranked.iloc[0]["attention_reason"] == "Volume spike"
    assert ranked.iloc[1]["symbol"] == "ETHUSDT"
    assert ranked.iloc[1]["attention_reason"] == "Sharp price move"
    assert ranked.iloc[1]["market_direction"] == "DOWN"
    assert ranked.iloc[-1]["attention_reason"] == "Normal"


def test_price_change_direction_classifies_small_moves_as_flat():
    result = price_change_direction(pd.Series([-0.5, -0.01, 0.0, 0.2]))

    assert result.tolist() == ["DOWN", "FLAT", "FLAT", "UP"]


def test_time_range_start_uses_dashboard_timezone_for_today():
    now = datetime(2026, 5, 9, 18, 0, tzinfo=timezone.utc)

    start = time_range_start("today", now=now, tz=ZoneInfo("Asia/Kolkata"))

    assert start == datetime(2026, 5, 8, 18, 30, tzinfo=timezone.utc)


def test_time_range_start_supports_relative_ranges_and_all():
    now = datetime(2026, 5, 9, 18, 0, tzinfo=timezone.utc)

    assert time_range_start("last_week", now=now, tz=ZoneInfo("UTC")) == now - timedelta(days=7)
    assert time_range_start("last_month", now=now, tz=ZoneInfo("UTC")) == now - timedelta(days=30)
    assert time_range_start("last_year", now=now, tz=ZoneInfo("UTC")) == now - timedelta(days=365)
    assert time_range_start("all", now=now, tz=ZoneInfo("UTC")) is None


def test_time_range_caption_describes_all_history():
    assert time_range_caption("all", None) == "All: all available local Delta history"


def test_build_overview_metrics_uses_preloaded_dashboard_frames():
    watchlist = pd.DataFrame(
        [
            {"symbol": "BTCUSDT", "volume_spike_ratio": 2.2, "price_change_5m_pct": 0.4},
            {"symbol": "ETHUSDT", "volume_spike_ratio": 1.1, "price_change_5m_pct": -1.8},
        ]
    )
    spikes = pd.DataFrame([{"signal_strength": "HIGH"}, {"signal_strength": "LOW"}])
    price_alerts = pd.DataFrame([{"symbol": "ETHUSDT"}])
    latest_time = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    health = pd.DataFrame([{"latest_time": latest_time}])

    metrics = build_overview_metrics(watchlist, spikes, price_alerts, health)

    assert metrics["symbols"] == 2
    assert metrics["high_volume_spikes"] == 1
    assert metrics["price_alerts"] == 1
    assert metrics["strongest_spike"] == 2.2
    assert metrics["biggest_abs_move"] == 1.8
    assert metrics["latest_gold_time"] == latest_time
