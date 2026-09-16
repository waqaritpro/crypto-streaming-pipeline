"""Consume crypto price messages from Redpanda and persist them to Postgres.

Reads the ``crypto-prices`` topic continuously, batches messages, and writes
them into the ``crypto_prices`` table with idempotent inserts (ON CONFLICT DO
NOTHING on the ``(pair, event_ts)`` natural key). Kafka offsets are committed
only after the batch is safely in the database, giving at-least-once delivery;
combined with the idempotent insert, replays never create duplicate rows.

Run it with::

    python -m src.consumer

Stop with Ctrl+C — the current batch is flushed and offsets committed on the
way out.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
from pathlib import Path

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from psycopg2.extras import execute_values

from src.config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("consumer")

# Applied on startup so a fresh database is ready without a manual step.
_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"

_INSERT_SQL = """
    INSERT INTO crypto_prices (pair, base, quote, price, source, event_ts)
    VALUES %s
    ON CONFLICT (pair, event_ts) DO NOTHING
"""

# Flipped by the SIGINT/SIGTERM handler so the poll loop can exit cleanly.
_shutdown = False


def _handle_signal(signum, _frame) -> None:
    global _shutdown
    log.info("Received signal %s — finishing current batch then stopping...", signum)
    _shutdown = True


def apply_schema(conn) -> None:
    """Create the table/indexes if they don't exist (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()
    log.info("Schema ensured from %s", _SCHEMA_PATH.name)


def parse_message(raw: bytes) -> tuple | None:
    """Turn a raw message value into an insert row, or None if it's malformed."""
    try:
        m = json.loads(raw)
        return (
            m["pair"],
            m["base"],
            m["quote"],
            float(m["price"]),
            m["source"],
            m["ts"],
        )
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        log.warning("Skipping malformed message: %s", exc)
        return None


def flush_batch(conn, consumer, rows: list[tuple]) -> int:
    """Write a batch to Postgres, then commit Kafka offsets. Returns rows inserted.

    The transaction and the offset commit are ordered so that a crash can only
    ever replay already-persisted messages (at-least-once), never lose them.
    """
    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM crypto_prices")
        before = cur.fetchone()[0]
        execute_values(cur, _INSERT_SQL, rows)
        cur.execute("SELECT count(*) FROM crypto_prices")
        after = cur.fetchone()[0]
    conn.commit()

    # Only advance offsets once the data is durably committed.
    consumer.commit(asynchronous=False)

    inserted = after - before
    skipped = len(rows) - inserted
    log.info(
        "Batch flushed: %d consumed, %d inserted, %d duplicate(s) ignored",
        len(rows),
        inserted,
        skipped,
    )
    return inserted


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": config.consumer_group,
            "client.id": "crypto-price-consumer",
            # Start at the beginning for a brand-new group so no history is lost.
            "auto.offset.reset": "earliest",
            # We commit explicitly after each DB write, not on a timer.
            "enable.auto.commit": False,
        }
    )


def run() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    conn = psycopg2.connect(config.dsn())
    apply_schema(conn)

    consumer = build_consumer()
    consumer.subscribe([config.topic])

    log.info(
        "Consuming topic '%s' (group '%s') on %s -> Postgres %s:%s/%s",
        config.topic,
        config.consumer_group,
        config.bootstrap_servers,
        config.postgres_host,
        config.postgres_port,
        config.postgres_db,
    )

    batch: list[tuple] = []
    total_inserted = 0

    try:
        while not _shutdown:
            msg = consumer.poll(timeout=config.consumer_batch_timeout_seconds)

            if msg is None:
                # No message this round — flush whatever we've accumulated.
                total_inserted += flush_batch(conn, consumer, batch)
                batch = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            row = parse_message(msg.value())
            if row is not None:
                batch.append(row)

            if len(batch) >= config.consumer_batch_size:
                total_inserted += flush_batch(conn, consumer, batch)
                batch = []
    finally:
        log.info("Flushing final batch before exit...")
        try:
            total_inserted += flush_batch(conn, consumer, batch)
        finally:
            consumer.close()
            conn.close()
        log.info("Consumer stopped. %d row(s) inserted this run.", total_inserted)


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:  # noqa: BLE001 — top-level guard for a CLI script
        log.error("Fatal error: %s", exc)
        sys.exit(1)
