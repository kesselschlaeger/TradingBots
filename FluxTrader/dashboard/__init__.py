"""FluxTrader Web-Dashboard (FastAPI, separater Prozess).

Liest ausschliesslich aus ``PersistentState`` (SQLite) und optional einer
eingebetteten ``HealthState``-Instanz, wenn im gleichen Prozess wie der
Runner laufend. Importiert nichts aus ``live/runner.py`` oder
``execution/``.
"""
