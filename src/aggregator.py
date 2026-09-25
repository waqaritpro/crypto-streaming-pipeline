"""Compute windowed price aggregates and log data-quality events.

A third stage in the pipeline, running alongside the producer and consumer. It
reads the ``crypto-prices`` topic in its own consumer group, runs every message
through the data-quality checks in :mod:`src.quality`, and folds the clean ticks
into fixed time windows (default 1 minute) per trading pair. For each window it
tracks the rolling average and the high/low, upserting the result into the
``price_aggregates`` table. Every quality problem is written to
``data_quality_events`` for later reporting.

Run it with::

    python -m src.aggregator

Stop with Ctrl+C — open windows are flushed and offsets committed on the way out.

Design notes
------------
* Windows are keyed by ``(pair, window_start)`` where ``window_start`` is the
  event time truncated down to the window boundary. The aggregator keeps open
  windows in memory and **upserts absolute values** each flush, so more ticks
  landing in an open window simply correct its row in place.
* A window is *sealed* (evicted from memory) only once wall-clock has moved a
  generous grace period past its end, after which late ticks for it are ignored
  for aggregation — they are, by then, already flagged ``stale`` by the DQ pass.
* Offsets are committed only after each flush is durably written, matching the
  consumer's at-least-once discipline.
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from psycopg2.extras import execute_values

from src.config import config
from src.quality import Verdict, inspect, preview_of

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("aggregator")

_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "aggregates.sql"

_UPSERT_AGG_SQL = """
    INSERT INTO price_aggregates
        (pair, window_start, window_seconds, avg_price, high_price, low_price, sample_count, updated_at)
    VALUES %s
    ON CONFLICT (pair, window_start, window_seconds) DO UPDATE SET
        avg_price    = EXCLUDED.avg_price,
        high_price   = EXCLUDED.high_price,
        low_price    = EXCLUDED.low_price,
        sample_count = EXCLUDED.sample_count,
        updated_at   = EXCLUDED.updated_at
"""

_INSERT_DQ_SQL = """
    INSERT INTO data_quality_events
        (check_name, severity, pair, detail, event_ts, raw_message)
    VALUES %s
"""

# Flipped by the SIGINT/SIGTERM handler so the poll loop can exit cleanly.
_shutdown = False


def _handle_signal(signum, _frame) -> None:
    global _shutdown
    log.info("Received signal %s — finishing current batch then stopping...", signum)
    _shutdown = True


@dataclass
class Window:
    """Running aggregate for one (pair, window_start)."""

    total: float = 0.0
    count: int = 0
    high: float = float("-inf")
    low: float = float("inf")

    def add(self, price: float) -> None:
        self.total += price
        self.count += 1
        self.high = max(self.high, price)
        self.low = min(self.low, price)

    @property
    def avg(self) -> float:
        return self.total / self.count if self.count else 0.0


def apply_schema(conn) -> None:
    """Create the aggregate/DQ tables if they don't exist (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()
    log.info("Schema ensured from %s", _SCHEMA_PATH.name)


def window_start_for(event_ts: datetime, width: int) -> datetime:
    """Truncate an event time down to its window boundary (UTC)."""
    epoch = int(event_ts.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % width), tz=timezone.utc)


class Aggregator:
    """Holds the in-memory windowing state and the DQ cross-message state."""

    def __init__(self) -> None:
        self.width = config.window_seconds
        # Sealing grace: keep a window in memory this long past its end before
        # evicting, so ordinary late ticks still land in the right window.
        self.grace = timedelta(seconds=self.width + config.dq_staleness_seconds)
        self.windows: dict[tuple[str, datetime], Window] = {}
        self.sealed: set[tuple[str, datetime]] = set()
        # DQ cross-message state, threaded through quality.inspect().
        self.seen_keys: set[tuple[str, str]] = set()
        self.last_price: dict[str, float] = {}
        self.newest_event_ts: datetime | None = None
        # Buffered DQ rows awaiting the next flush.
        self.pending_dq: list[tuple] = []

    def observe(self, raw: bytes, now: datetime) -> Verdict:
        """Run DQ checks and, if clean, fold the tick into its window."""
        verdict = inspect(
            raw, now=now, seen_keys=self.seen_keys, last_price=self.last_price
        )

        for issue in verdict.issues:
            self.pending_dq.append(
                (
                    issue.check_name,
                    issue.severity,
                    issue.pair,
                    issue.detail,
                    issue.event_ts,
                    preview_of(raw),
                )
            )

        if verdict.contributes and verdict.price is not None and verdict.event_ts:
            if verdict.event_ts > (self.newest_event_ts or verdict.event_ts):
                self.newest_event_ts = verdict.event_ts
            elif self.newest_event_ts is None:
                self.newest_event_ts = verdict.event_ts

            key = (verdict.pair, window_start_for(verdict.event_ts, self.width))
            if key in self.sealed:
                # Window already finalised; the tick is stale (already flagged).
                log.debug("Ignoring late tick for sealed window %s", key)
            else:
                self.windows.setdefault(key, Window()).add(verdict.price)

        return verdict

    def _agg_rows(self, updated_at: datetime) -> list[tuple]:
        rows = []
        for (pair, w_start), win in self.windows.items():
            rows.append(
                (
                    pair,
                    w_start,
                    self.width,
                    round(win.avg, 8),
                    win.high,
                    win.low,
                    win.count,
                    updated_at,
                )
            )
        return rows

    def flush(self, conn, consumer, now: datetime) -> tuple[int, int]:
        """Upsert open windows and DQ events, then commit offsets.

        Returns ``(windows_written, dq_written)``.
        """
        agg_rows = self._agg_rows(now)
        dq_rows = self.pending_dq

        if not agg_rows and not dq_rows:
            return (0, 0)

        with conn.cursor() as cur:
            if agg_rows:
                execute_values(cur, _UPSERT_AGG_SQL, agg_rows)
            if dq_rows:
                execute_values(cur, _INSERT_DQ_SQL, dq_rows)
        conn.commit()
        consumer.commit(asynchronous=False)

        self.pending_dq = []
        self._seal_closed_windows()
        return (len(agg_rows), len(dq_rows))

    def _seal_closed_windows(self) -> None:
        """Evict windows whose end is safely in the past to bound memory."""
        if self.newest_event_ts is None:
            return
        cutoff = self.newest_event_ts - self.grace
        for key in list(self.windows):
            _pair, w_start = key
            window_end = w_start + timedelta(seconds=self.width)
            if window_end <= cutoff:
                self.sealed.add(key)
                del self.windows[key]


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": config.aggregator_group,
            "client.id": "crypto-price-aggregator",
            "auto.offset.reset": "earliest",
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

    agg = Aggregator()

    log.info(
        "Aggregating topic '%s' (group '%s') into %ds windows -> Postgres %s:%s/%s",
        config.topic,
        config.aggregator_group,
        config.window_seconds,
        config.postgres_host,
        config.postgres_port,
        config.postgres_db,
    )

    last_flush = time.monotonic()
    tot_windows = tot_dq = 0

    try:
        while not _shutdown:
            msg = consumer.poll(timeout=1.0)
            now = datetime.now(timezone.utc)

            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        pass
                    else:
                        raise KafkaException(msg.error())
                else:
                    agg.observe(msg.value(), now)

            # Flush on the configured cadence regardless of traffic.
            if time.monotonic() - last_flush >= config.aggregator_flush_seconds:
                w, d = agg.flush(conn, consumer, now)
                if w or d:
                    tot_windows += w
                    tot_dq += d
                    log.info(
                        "Flushed: %d window(s) upserted, %d DQ event(s) logged", w, d
                    )
                last_flush = time.monotonic()
    finally:
        log.info("Final flush before exit...")
        try:
            w, d = agg.flush(conn, consumer, datetime.now(timezone.utc))
            tot_windows += w
            tot_dq += d
        finally:
            consumer.close()
            conn.close()
        log.info(
            "Aggregator stopped. %d window upsert(s), %d DQ event(s) this run.",
            tot_windows,
            tot_dq,
        )


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:  # noqa: BLE001 — top-level guard for a CLI script
        log.error("Fatal error: %s", exc)
        sys.exit(1)
