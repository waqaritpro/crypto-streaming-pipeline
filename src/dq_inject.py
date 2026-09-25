"""Publish deliberately broken messages to exercise the data-quality checks.

This is a *demo / test* utility, not part of the live pipeline. The real
producer only ever emits clean ticks, so to prove the aggregator's DQ checks
actually fire we inject a handful of crafted bad payloads onto the same topic.
Run the aggregator, then run this, then look at the quality report::

    python -m src.aggregator          # in one terminal
    python -m src.dq_inject            # in another
    python -m src.reports              # see the DQ events it logged

Each injected message maps to one check in src/quality.py.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

from src.config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("dq_inject")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def bad_messages() -> list[tuple[str, bytes]]:
    """Return (label, raw_bytes) pairs, one per data-quality check."""
    pair = f"BTC-{config.quote_currency}"
    stale_ts = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    dup_ts = _now_iso()

    def msg(**over) -> bytes:
        base = {
            "pair": pair,
            "base": "BTC",
            "quote": config.quote_currency,
            "price": 60000.0,
            "source": "dq-inject",
            "ts": _now_iso(),
        }
        base.update(over)
        return json.dumps(base).encode("utf-8")

    dup = msg(ts=dup_ts)  # sent twice below to trigger the duplicate check

    return [
        ("malformed_json", b"{not valid json at all"),
        ("missing_field (no price)", json.dumps(
            {"pair": pair, "base": "BTC", "quote": config.quote_currency,
             "source": "dq-inject", "ts": _now_iso()}).encode("utf-8")),
        ("out_of_range (negative)", msg(price=-5.0)),
        ("out_of_range (absurdly high)", msg(price=99_000_000.0)),
        ("invalid_price (non-numeric)", msg(price="not-a-number")),
        ("stale (10 min old)", msg(ts=stale_ts)),
        ("price_jump (10x spike)", msg(price=600000.0)),
        ("duplicate #1", dup),
        ("duplicate #2 (same pair+ts)", dup),
    ]


def run() -> None:
    producer = Producer(
        {"bootstrap.servers": config.bootstrap_servers, "client.id": "dq-injector"}
    )
    pair = f"BTC-{config.quote_currency}"

    log.info("Injecting %d crafted messages to topic '%s'...", len(bad_messages()), config.topic)
    for label, raw in bad_messages():
        producer.produce(topic=config.topic, key=pair, value=raw)
        log.info("  -> %s", label)
    producer.flush(10)
    log.info("Done. Run the aggregator (if not already) then `python -m src.reports`.")


if __name__ == "__main__":
    run()
