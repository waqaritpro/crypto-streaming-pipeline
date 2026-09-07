"""Stream live crypto spot prices into a Redpanda topic.

Polls the keyless Coinbase public spot-price API once per configured interval,
normalises each response into a clean JSON message, and publishes it to the
Redpanda topic keyed by trading pair. Run it with:

    python -m src.producer

Stop with Ctrl+C — outstanding messages are flushed on the way out.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from src.config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")

# Flipped by the SIGINT/SIGTERM handler so the main loop can exit cleanly.
_shutdown = False


def _handle_signal(signum, _frame) -> None:
    global _shutdown
    log.info("Received signal %s — shutting down after this round...", signum)
    _shutdown = True


def fetch_price(session: requests.Session, pair: str) -> dict | None:
    """Fetch one spot price and return a normalised message, or None on error.

    Coinbase returns e.g.::

        {"data": {"amount": "63000.12", "base": "BTC", "currency": "USD"}}
    """
    url = config.price_api_url.format(pair=pair)
    try:
        resp = session.get(url, timeout=config.request_timeout_seconds)
        resp.raise_for_status()
        data = resp.json()["data"]
        return {
            "pair": pair,
            "base": data["base"],
            "quote": data["currency"],
            "price": float(data["amount"]),
            "source": "coinbase",
            # Event time: when we observed the price, in UTC ISO-8601.
            "ts": datetime.now(timezone.utc).isoformat(),
        }
    except (requests.RequestException, KeyError, ValueError) as exc:
        log.warning("Failed to fetch %s: %s", pair, exc)
        return None


def _delivery_report(err, msg) -> None:
    """Async callback invoked once per message by librdkafka."""
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": "crypto-price-producer",
            # Wait for the leader ack — good balance of durability and speed.
            "acks": "1",
            "enable.idempotence": False,
        }
    )


def run() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    producer = build_producer()
    session = requests.Session()
    pairs = config.pairs()

    log.info(
        "Streaming %s -> topic '%s' on %s every %ss",
        ", ".join(pairs),
        config.topic,
        config.bootstrap_servers,
        config.poll_interval_seconds,
    )

    while not _shutdown:
        for pair in pairs:
            message = fetch_price(session, pair)
            if message is None:
                continue

            producer.produce(
                topic=config.topic,
                key=pair,
                value=json.dumps(message).encode("utf-8"),
                callback=_delivery_report,
            )
            log.info("Published %s @ %.2f %s", pair, message["price"], message["quote"])

        # Serve delivery callbacks without blocking the poll cadence.
        producer.poll(0)

        # Interruptible sleep: check the shutdown flag every 100ms.
        waited = 0.0
        while waited < config.poll_interval_seconds and not _shutdown:
            _sleep(0.1)
            waited += 0.1

    log.info("Flushing outstanding messages...")
    producer.flush(10)
    log.info("Producer stopped.")


def _sleep(seconds: float) -> None:
    # Wrapped so the interruptible-sleep loop stays easy to read/test.
    import time

    time.sleep(seconds)


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:  # noqa: BLE001 — top-level guard for a CLI script
        log.error("Fatal error: %s", exc)
        sys.exit(1)
