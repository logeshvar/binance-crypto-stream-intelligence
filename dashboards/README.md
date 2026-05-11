# Dashboard

Streamlit dashboard for local Gold Delta tables and published market alerts.

Run it from the repository root:

```bash
make dashboard PYTHON=.venv/bin/python
```

The app reads Gold Delta tables from `GOLD_PATH`, defaulting to `./storage/gold`, and reads published alerts from `TOPIC_SIGNALS_ALERTS`, defaulting to `market.signals.alerts`.

## Views

- Command Center: attention-ranked symbols, strongest volume spike, biggest 5-minute move, and freshness status.
- Symbol Drilldown: 1-minute close price history, 5-minute volume, and spike ratio for the selected symbol.
- Alerts: business-facing alert records without broker transport metadata.
- Pipeline Health: selected-range row counts and latest update times for Gold tables.

The sidebar time range filter supports Today, Last Week, Last Month, Last Year, and All. The filter changes only the serving view; it does not delete historical Delta data.
