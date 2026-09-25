-- Schema for the Week 3 modelling layer: windowed price aggregates and the
-- data-quality event log.
--
-- Applied automatically by the aggregator on startup (idempotent: safe to run
-- repeatedly), and usable by hand:
--
--   docker exec -i crypto-postgres psql -U crypto -d crypto < sql/aggregates.sql
--
-- Two independent concerns live here:
--   * price_aggregates    — one row per trading pair per fixed time window,
--                           holding the rolling average and the high/low seen.
--   * data_quality_events — one row per data-quality problem detected on the
--                           stream (missing fields, bad prices, stale/dupes).

-- ---------------------------------------------------------------------------
-- Windowed aggregates: rolling 1-minute average + high/low per pair per window.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_aggregates (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pair           TEXT           NOT NULL,   -- e.g. 'BTC-USD'
    window_start   TIMESTAMPTZ    NOT NULL,   -- start of the window (UTC, truncated)
    window_seconds INT            NOT NULL,   -- window width, e.g. 60 for 1 minute
    avg_price      NUMERIC(20, 8) NOT NULL,   -- mean spot price over the window
    high_price     NUMERIC(20, 8) NOT NULL,   -- max spot price over the window
    low_price      NUMERIC(20, 8) NOT NULL,   -- min spot price over the window
    sample_count   INT            NOT NULL,   -- number of ticks in the window
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),  -- last time we recomputed it

    -- One aggregate row per pair per window of a given width. The aggregator
    -- upserts as more ticks land in an open window, so re-runs and late samples
    -- correct the row in place rather than duplicating it.
    CONSTRAINT uq_price_aggregates_pair_window
        UNIQUE (pair, window_start, window_seconds)
);

-- Serves "recent windows for a pair, newest first".
CREATE INDEX IF NOT EXISTS ix_price_aggregates_pair_window_desc
    ON price_aggregates (pair, window_start DESC);

-- ---------------------------------------------------------------------------
-- Data-quality event log: every problem the aggregator spots on the stream.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_quality_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    check_name  TEXT        NOT NULL,   -- 'missing_field', 'out_of_range', 'stale', 'duplicate', ...
    severity    TEXT        NOT NULL,   -- 'error' (dropped) or 'warning' (kept)
    pair        TEXT,                   -- trading pair if known
    detail      TEXT        NOT NULL,   -- human-readable description
    event_ts    TIMESTAMPTZ,            -- the message's own timestamp if parseable
    raw_message TEXT,                   -- the offending payload (truncated) for triage
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Serves "recent quality problems, newest first" and grouping by check.
CREATE INDEX IF NOT EXISTS ix_dq_events_detected_desc
    ON data_quality_events (detected_at DESC);
CREATE INDEX IF NOT EXISTS ix_dq_events_check
    ON data_quality_events (check_name);
