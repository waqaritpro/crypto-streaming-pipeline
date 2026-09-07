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

    def pairs(self) -> list[str]:
        """Return trading pairs, e.g. ['BTC-USD', 'ETH-USD']."""
        return [f"{coin}-{self.quote_currency}" for coin in self.coins]


# Import this shared instance elsewhere: ``from src.config import config``.
config = Config()
