# Dashboard

Streamlit dashboard for local Gold Delta tables and Kafka alert events.

Run it from the repository root:

```bash
streamlit run dashboards/streamlit_app.py
```

The app reads Gold Delta tables from `GOLD_PATH`, defaulting to `./storage/gold`, and reads Kafka alerts from `TOPIC_SIGNALS_ALERTS`, defaulting to `market.signals.alerts`.
