"""Central configuration for the crypto streaming pipeline.

All settings can be overridden with environment variables (or a local ``.env``
file), so the same code runs unchanged in local dev, Docker, or CI. See
``.env.example`` for the full list of knobs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

# Load a local .env if present; real environment variables always win.
load_dotenv()


def _get_list(name: str, default: str) -> list[str]:
    """Read a comma-separated env var into a clean, upper-cased list."""
    raw = os.getenv(name, default)
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class Config:
    # --- Which coins to stream -------------------------------------------
    # Base assets to track, e.g. BTC, ETH, SOL. Configure via COINS env var.
    coins: list[str] = field(
        default_factory=lambda: _get_list("COINS", "BTC,ETH,SOL")
    )
    # Fiat quote currency the prices are denominated in.
    quote_currency: str = os.getenv("QUOTE_CURRENCY", "USD").upper()

    # --- Polling behaviour ----------------------------------------------
    # Seconds between polling rounds across all configured coins.
    poll_interval_seconds: float = float(os.getenv("POLL_INTERVAL_SECONDS", "5"))
    # Per-request HTTP timeout when calling the price API.
    request_timeout_seconds: float = float(
        os.getenv("REQUEST_TIMEOUT_SECONDS", "10")
    )

    # --- Price data source (Coinbase public spot API, keyless) ----------
    # Endpoint template; {pair} is filled with e.g. "BTC-USD".
    price_api_url: str = os.getenv(
        "PRICE_API_URL", "https://api.coinbase.com/v2/prices/{pair}/spot"
    )

    # --- Redpanda / Kafka ------------------------------------------------
    # Broker the producer connects to. From the host this is localhost:9092.
    bootstrap_servers: str = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    # Topic that price messages are published to.
    topic: str = os.getenv("TOPIC", "crypto-prices")
    # Consumer group id; all consumers sharing it split the topic's partitions.
    consumer_group: str = os.getenv("CONSUMER_GROUP", "crypto-price-writer")
    # How many messages the consumer batches into one DB transaction.
    consumer_batch_size: int = int(os.getenv("CONSUMER_BATCH_SIZE", "100"))
    # Max seconds to wait filling a batch before flushing what we have.
    consumer_batch_timeout_seconds: float = float(
        os.getenv("CONSUMER_BATCH_TIMEOUT_SECONDS", "2")
    )

    # --- Aggregation / windowing (Week 3) --------------------------------
    # Consumer group for the aggregator; separate from the writer so it reads
    # the full topic independently of the Postgres consumer.
    aggregator_group: str = os.getenv("AGGREGATOR_GROUP", "crypto-aggregator")
    # Width of each aggregation window, in seconds (60 = rolling 1-minute).
    window_seconds: int = int(os.getenv("WINDOW_SECONDS", "60"))
    # How often the aggregator upserts its open windows to Postgres.
    aggregator_flush_seconds: float = float(
        os.getenv("AGGREGATOR_FLUSH_SECONDS", "10")
    )

    # --- Data-quality thresholds (Week 3) --------------------------------
    # Prices outside (min, max] are treated as out-of-range errors and dropped.
    dq_price_min: float = float(os.getenv("DQ_PRICE_MIN", "0"))
    dq_price_max: float = float(os.getenv("DQ_PRICE_MAX", "10000000"))
    # A tick older than this many seconds (vs. wall clock) is flagged stale.
    dq_staleness_seconds: float = float(os.getenv("DQ_STALENESS_SECONDS", "120"))
    # A tick timestamped more than this far in the future is flagged too.
    dq_future_seconds: float = float(os.getenv("DQ_FUTURE_SECONDS", "60"))
    # A move larger than this percent vs. the pair's previous price is flagged.
    dq_max_jump_pct: float = float(os.getenv("DQ_MAX_JUMP_PCT", "20"))

    # --- Postgres --------------------------------------------------------
    # From the host, Postgres is published on 5434 (see docker-compose.yml).
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5434"))
    postgres_db: str = os.getenv("POSTGRES_DB", "crypto")
    postgres_user: str = os.getenv("POSTGRES_USER", "crypto")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "crypto")

    def dsn(self) -> str:
        """libpq connection string for psycopg2."""
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )

    def pairs(self) -> list[str]:
        """Return trading pairs, e.g. ['BTC-USD', 'ETH-USD']."""
        return [f"{coin}-{self.quote_currency}" for coin in self.coins]


# Import this shared instance elsewhere: ``from src.config import config``.
config = Config()
