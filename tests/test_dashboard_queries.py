from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from dashboards.queries import add_attention_score
from dashboards.queries import delta_table_exists
from dashboards.queries import freshness_label
from dashboards.queries import get_gold_table_paths
from dashboards.queries import price_change_direction


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
