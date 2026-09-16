-- Schema for the crypto streaming pipeline's price history.
--
-- Applied automatically by the consumer on startup (idempotent: safe to run
-- repeatedly), and usable by hand:
--
--   docker exec -i crypto-postgres psql -U crypto -d crypto < sql/schema.sql
--
-- Each row is one observed spot price for a trading pair at a point in time.

CREATE TABLE IF NOT EXISTS crypto_prices (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pair        TEXT           NOT NULL,   -- e.g. 'BTC-USD'
    base        TEXT           NOT NULL,   -- e.g. 'BTC'
    quote       TEXT           NOT NULL,   -- e.g. 'USD'
    price       NUMERIC(20, 8) NOT NULL,   -- spot price in the quote currency
    source      TEXT           NOT NULL,   -- price origin, e.g. 'coinbase'
    event_ts    TIMESTAMPTZ    NOT NULL,   -- when the price was observed (producer)
    inserted_at TIMESTAMPTZ    NOT NULL DEFAULT now(),  -- when it landed here

    -- Natural key for idempotency: one price per pair per observation instant.
    -- Lets the consumer re-process messages (e.g. after a restart replays
    -- uncommitted offsets) without creating duplicate rows.
    CONSTRAINT uq_crypto_prices_pair_event UNIQUE (pair, event_ts)
);

-- Serves the common "latest / recent prices for a pair, newest first" query.
CREATE INDEX IF NOT EXISTS ix_crypto_prices_pair_event_desc
    ON crypto_prices (pair, event_ts DESC);
