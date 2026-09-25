"""Data-quality checks for the crypto price stream.

A single entry point, :func:`inspect`, takes one raw Kafka message value and
returns a :class:`Verdict`: the parsed message (when usable), any quality
issues found, and whether the tick is clean enough to feed into the aggregates.

The checks are deliberately independent and side-effect-free so they are easy
to unit-test and reason about. Stateful checks (duplicates, price jumps) take
their state in as arguments and hand back the updates for the caller to apply.

Checks performed
----------------
* **malformed_json / missing_field / invalid_price / invalid_timestamp** —
  structural problems. Severity ``error``; the tick is dropped from aggregates.
* **out_of_range** — price outside the configured sane bounds. ``error``; dropped.
* **duplicate** — a ``(pair, ts)`` already seen this run. ``warning``; dropped so
  it is not double-counted.
* **stale / future_timestamp** — event time too far from wall-clock. ``warning``;
  the value is still real, so it is kept.
* **price_jump** — a suspiciously large move vs. the pair's previous price.
  ``warning``; kept (it may well be a real spike).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from src.config import config

# Fields every well-formed price message must carry (see producer.fetch_price).
REQUIRED_FIELDS = ("pair", "base", "quote", "price", "source", "ts")

# How much of an offending payload we keep for triage in the DQ log.
_RAW_PREVIEW_LEN = 500


@dataclass
class Issue:
    """One data-quality problem found in a message."""

    check_name: str
    severity: str  # 'error' or 'warning'
    detail: str
    pair: str | None = None
    event_ts: datetime | None = None


@dataclass
class Verdict:
    """Outcome of inspecting one message."""

    issues: list[Issue] = field(default_factory=list)
    # Parsed fields when the message is usable enough to aggregate.
    pair: str | None = None
    price: float | None = None
    event_ts: datetime | None = None
    # True when the tick should contribute to the windowed aggregates.
    contributes: bool = False


def _parse_ts(value) -> datetime | None:
    """Parse an ISO-8601 timestamp into an aware UTC datetime, or None."""
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def inspect(
    raw: bytes,
    *,
    now: datetime,
    seen_keys: set[tuple[str, str]],
    last_price: dict[str, float],
) -> Verdict:
    """Run all quality checks on one raw message value.

    ``seen_keys`` and ``last_price`` are read and mutated in place so the caller
    keeps the cross-message state between calls.
    """
    verdict = Verdict()

    # --- Structural: is it even JSON? ------------------------------------
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        verdict.issues.append(Issue("malformed_json", "error", f"not valid JSON: {exc}"))
        return verdict
    if not isinstance(msg, dict):
        verdict.issues.append(Issue("malformed_json", "error", "payload is not a JSON object"))
        return verdict

    pair = msg.get("pair") if isinstance(msg.get("pair"), str) else None
    verdict.pair = pair

    # --- Missing / null required fields ----------------------------------
    missing = [f for f in REQUIRED_FIELDS if msg.get(f) is None]
    if missing:
        verdict.issues.append(
            Issue("missing_field", "error", f"missing/null field(s): {', '.join(missing)}", pair)
        )
        # Can't trust the row; still try to salvage a timestamp for the log.
        verdict.event_ts = _parse_ts(msg.get("ts"))
        return verdict

    # --- Timestamp parseable? --------------------------------------------
    event_ts = _parse_ts(msg["ts"])
    if event_ts is None:
        verdict.issues.append(
            Issue("invalid_timestamp", "error", f"unparseable ts: {msg['ts']!r}", pair)
        )
        return verdict
    verdict.event_ts = event_ts

    # --- Price numeric? ---------------------------------------------------
    try:
        price = float(msg["price"])
    except (TypeError, ValueError):
        verdict.issues.append(
            Issue("invalid_price", "error", f"non-numeric price: {msg['price']!r}", pair, event_ts)
        )
        return verdict
    verdict.price = price

    # --- Price in a sane range? ------------------------------------------
    if price <= config.dq_price_min or price > config.dq_price_max:
        verdict.issues.append(
            Issue(
                "out_of_range",
                "error",
                f"price {price} outside ({config.dq_price_min}, {config.dq_price_max}]",
                pair,
                event_ts,
            )
        )
        return verdict

    # --- Duplicate of a tick we've already processed this run? -----------
    key = (pair, msg["ts"])
    if key in seen_keys:
        verdict.issues.append(
            Issue("duplicate", "warning", f"duplicate tick for {pair} @ {msg['ts']}", pair, event_ts)
        )
        return verdict
    seen_keys.add(key)

    # From here the tick is structurally sound and unique: it will aggregate.
    verdict.contributes = True

    # --- Stale / future timestamp (warning only, value still counts) -----
    age = (now - event_ts).total_seconds()
    if age > config.dq_staleness_seconds:
        verdict.issues.append(
            Issue("stale", "warning", f"event is {age:.0f}s old (> {config.dq_staleness_seconds:.0f}s)", pair, event_ts)
        )
    elif -age > config.dq_future_seconds:
        verdict.issues.append(
            Issue("future_timestamp", "warning", f"event is {-age:.0f}s in the future", pair, event_ts)
        )

    # --- Implausible jump vs. the pair's previous price (warning only) ---
    prev = last_price.get(pair)
    if prev is not None and prev > 0:
        jump_pct = abs(price - prev) / prev * 100
        if jump_pct > config.dq_max_jump_pct:
            verdict.issues.append(
                Issue(
                    "price_jump",
                    "warning",
                    f"{jump_pct:.1f}% move vs. previous {prev} (> {config.dq_max_jump_pct:.0f}%)",
                    pair,
                    event_ts,
                )
            )
    last_price[pair] = price

    return verdict


def preview_of(raw: bytes) -> str:
    """Truncated, decode-safe view of a raw payload for the DQ log."""
    return raw.decode("utf-8", "replace")[:_RAW_PREVIEW_LEN]
