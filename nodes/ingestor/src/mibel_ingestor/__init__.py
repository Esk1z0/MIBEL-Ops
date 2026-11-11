"""MIBEL-Ops ingestor node.

Lightweight hexagonal design (ADR-05): a small core (model, gates, pipeline, scheduler) and
replaceable adapters (connectors, HTTP, SQLite store, sinks, metrics).
"""

__version__ = "0.1.0"
