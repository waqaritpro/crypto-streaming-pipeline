# Real-Time Crypto Price Streaming Pipeline

A hands-on data engineering case study: stream live cryptocurrency prices from a
public API through a Kafka-compatible log (Redpanda) and into Postgres, then
model and serve the data for analytics — built incrementally over four weeks.

> **Status:** Week 1 of 4 complete — ingestion into the streaming log is live.

---

## Problem

Crypto markets move continuously and produce a firehose of price ticks. Pulling
that data on demand with one-off API calls doesn't scale: you get rate-limited,
you lose history the moment a request finishes, and every downstream consumer
(a dashboard, an alerting job, a backtest) ends up re-fetching the same thing.

The data-engineering answer is to **ingest once, buffer durably, and let many
consumers read independently.** This project builds that pattern end to end:

- A **producer** polls a public price API and publishes clean, structured
  events to a durable, replayable log.
- A **streaming platform** (Redpanda) decouples ingestion from consumption and
  absorbs bursts.
- A **warehouse** (Postgres) stores the history for querying and modelling.

The result is a realistic, portfolio-grade template for real-time ingestion
that mirrors how production streaming stacks are put together.

---

## Architecture

```
                    poll every N seconds (keyless HTTP)
  ┌────────────────┐        ┌──────────────────────┐        ┌──────────────────────┐
  │  Coinbase      │  JSON  │  Producer            │  JSON  │  Redpanda            │
  │  public spot   ├───────►│  src/producer.py     ├───────►│  (Kafka API)         │
  │  price API     │        │  normalises + keys   │  msgs  │  topic:              │
  └────────────────┘        │  by trading pair     │        │  "crypto-prices"     │
                            └──────────────────────┘        └──────────┬───────────┘
                                                                       │  consume
                                                                       ▼
                                                            ┌──────────────────────┐
                                                            │  Postgres            │
                                                            │  price history       │
                                                            │  (Week 2)            │
                                                            └──────────────────────┘
```

Redpanda and Postgres both run locally via `docker-compose`. The producer runs
on the host and talks to Redpanda over the standard Kafka protocol on
`localhost:9092`.

**Message shape** published to the `crypto-prices` topic (keyed by trading pair):

```json
{
  "pair": "BTC-USD",
  "base": "BTC",
  "quote": "USD",
  "price": 63000.12,
  "source": "coinbase",
  "ts": "2026-09-07T12:00:00.123456+00:00"
}
```

---

## Tech stack

| Layer            | Choice                     | Why                                                        |
| ---------------- | -------------------------- | ---------------------------------------------------------- |
| Streaming log    | **Redpanda**               | Kafka-compatible, single binary, no ZooKeeper — light for local dev |
| Warehouse        | **Postgres 16**            | Ubiquitous, great for modelling and serving analytics      |
| Producer         | **Python 3.12**            | `confluent-kafka` client + `requests`                      |
| Price source     | **Coinbase public API**    | Free, **keyless**, reliable spot prices                    |
| Orchestration    | **Docker Compose**         | One command to bring the whole local stack up              |

---

## Project layout

```
crypto-streaming-pipeline/
├── docker-compose.yml     # Redpanda + Postgres local stack
├── requirements.txt       # Python dependencies
├── .env.example           # Configuration template
├── src/
│   ├── config.py          # Central, env-overridable configuration
│   └── producer.py        # Polls the price API, publishes to Redpanda
└── README.md
```

---

## Getting started

**Prerequisites:** Docker (with Compose v2) and Python 3.12+.

**1. Start the local stack** (Redpanda + Postgres):

```bash
docker compose up -d
docker compose ps        # wait until both show "healthy"
```

**2. Install Python dependencies** (a virtual environment is recommended):

```bash
python -m venv .venv
# Windows:  .venv\Scripts\activate
# macOS/Linux:  source .venv/bin/activate
pip install -r requirements.txt
```

**3. Run the producer:**

```bash
python -m src.producer
```

You'll see a line per published tick, e.g.
`Published BTC-USD @ 63000.12 USD`. Stop it with `Ctrl+C`.

**4. Verify messages landed in the topic** by consuming a few with Redpanda's
built-in `rpk` CLI (runs inside the container):

```bash
docker exec -it crypto-redpanda rpk topic consume crypto-prices --num 5
```

**Configuration** — override any default via environment variables or a `.env`
file (see `.env.example`). For example, to track different coins:

```bash
COINS=BTC,ETH,DOGE,ADA python -m src.producer
```

**Tear down** when you're done:

```bash
docker compose down       # stop containers
docker compose down -v    # ...and delete their data volumes
```

---

## Roadmap

- [x] **Week 1 — Ingestion.** Project scaffold, Dockerised Redpanda + Postgres,
      and a configurable producer streaming live prices into the `crypto-prices`
      topic. Verified end to end by consuming messages off the topic.
- [ ] **Week 2 — Persistence.** A consumer that reads the topic and writes price
      history into Postgres, with a sensible schema and idempotent upserts.
- [ ] **Week 3 — Modelling.** Transform raw ticks into analytics-ready tables
      (OHLC candles, rolling averages) and add data-quality checks.
- [ ] **Week 4 — Serving & polish.** A dashboard/API over the modelled data,
      containerise the producer/consumer, and document the full case study.

---

## License

[MIT](LICENSE)
