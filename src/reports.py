"""Print a plain-text report of the Week 3 modelling layer.

Two sections, read straight from Postgres:

* **Windowed aggregates** — the most recent 1-minute windows per trading pair,
  with the rolling average and the high/low.
* **Data-quality report** — a summary of DQ events by check and severity, plus
  the most recent individual events.

Run it any time the aggregator has been going::

    python -m src.reports
"""

from __future__ import annotations

import psycopg2

from src.config import config

_RULE = "-" * 78


def _fetch(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchall()


def latest_aggregates(cur, per_pair: int = 5) -> None:
    print(_RULE)
    print(f"WINDOWED PRICE AGGREGATES  (window = {config.window_seconds}s, latest {per_pair} per pair)")
    print(_RULE)

    rows = _fetch(
        cur,
        """
        SELECT pair, window_start, avg_price, high_price, low_price, sample_count
        FROM (
            SELECT pair, window_start, avg_price, high_price, low_price, sample_count,
                   row_number() OVER (PARTITION BY pair ORDER BY window_start DESC) AS rn
            FROM price_aggregates
            WHERE window_seconds = %s
        ) t
        WHERE rn <= %s
        ORDER BY pair, window_start DESC
        """,
        (config.window_seconds, per_pair),
    )

    if not rows:
        print("  (no aggregates yet — is the aggregator running with fresh ticks?)")
        return

    print(f"  {'pair':<10} {'window_start (UTC)':<22} {'avg':>14} {'high':>14} {'low':>14} {'n':>4}")
    for pair, w_start, avg, high, low, n in rows:
        print(
            f"  {pair:<10} {w_start.strftime('%Y-%m-%d %H:%M:%S'):<22} "
            f"{avg:>14.2f} {high:>14.2f} {low:>14.2f} {n:>4}"
        )


def quality_report(cur, recent: int = 12) -> None:
    print()
    print(_RULE)
    print("DATA-QUALITY REPORT")
    print(_RULE)

    total = _fetch(cur, "SELECT count(*) FROM data_quality_events")[0][0]
    print(f"  total events logged: {total}")

    summary = _fetch(
        cur,
        """
        SELECT check_name, severity, count(*)
        FROM data_quality_events
        GROUP BY check_name, severity
        ORDER BY count(*) DESC, check_name
        """,
    )
    print()
    print(f"  {'check_name':<20} {'severity':<10} {'count':>6}")
    print(f"  {'-' * 20} {'-' * 10} {'-' * 6}")
    for check_name, severity, count in summary:
        print(f"  {check_name:<20} {severity:<10} {count:>6}")

    events = _fetch(
        cur,
        """
        SELECT detected_at, check_name, severity, coalesce(pair, '-') AS pair, detail
        FROM data_quality_events
        ORDER BY detected_at DESC
        LIMIT %s
        """,
        (recent,),
    )
    if events:
        print()
        print(f"  most recent {len(events)} event(s):")
        for detected_at, check_name, severity, pair, detail in events:
            print(
                f"    {detected_at.strftime('%H:%M:%S')} [{severity:<7}] "
                f"{check_name:<18} {pair:<9} {detail}"
            )


def run() -> None:
    conn = psycopg2.connect(config.dsn())
    try:
        with conn.cursor() as cur:
            latest_aggregates(cur)
            quality_report(cur)
        print(_RULE)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
