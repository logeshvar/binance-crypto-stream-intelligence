from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dashboards.queries import delta_table_exists
from dashboards.queries import freshness_label
from dashboards.queries import get_gold_table_paths


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
