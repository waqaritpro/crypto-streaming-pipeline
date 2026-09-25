# Real-Time Crypto Price Streaming Pipeline

A hands-on data engineering case study: stream live cryptocurrency prices from a
public API through a Kafka-compatible log (Redpanda) and into Postgres, then
model and serve the data for analytics — built incrementally over four weeks.

> **Status:** Week 3 of 4 complete — prices stream through the log, land in
> Postgres, and are now modelled into windowed aggregates with data-quality
> checks running on the stream.

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
                                                            │  Consumer            │
                                                            │  src/consumer.py     │
                                                            │  idempotent inserts  │
                                                            └──────────┬───────────┘
                                                                       │  write
                                                                       ▼
                                                            ┌──────────────────────┐
                                                            │  Postgres            │
                                                            │  crypto_prices table │
                                                            └──────────────────────┘

  The aggregator reads the same topic in its own consumer group:

  ┌──────────────────────┐  consume   ┌──────────────────────┐  upsert   ┌──────────────────────┐
  │  Redpanda            ├───────────►│  Aggregator          ├──────────►│  Postgres            │
  │  topic:              │   msgs     │  src/aggregator.py   │           │  price_aggregates    │
  │  "crypto-prices"     │            │  DQ checks + 1-min   │           │  data_quality_events │
  └──────────────────────┘            │  windows (avg/hi/lo) │           └──────────────────────┘
                                       └──────────────────────┘
```

Redpanda and Postgres both run locally via `docker-compose`. The producer runs
on the host and talks to Redpanda over the standard Kafka protocol on
`localhost:9092`. The consumer and the aggregator are independent consumer
groups over the same topic, so each reads the full stream without competing.

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
├── sql/
│   ├── schema.sql         # crypto_prices table + indexes (idempotent)
│   └── aggregates.sql     # price_aggregates + data_quality_events (idempotent)
├── src/
│   ├── config.py          # Central, env-overridable configuration
│   ├── producer.py        # Polls the price API, publishes to Redpanda
│   ├── consumer.py        # Reads the topic, writes prices to Postgres
│   ├── quality.py         # Data-quality checks run on each streamed message
│   ├── aggregator.py      # 1-minute windowed aggregates + DQ event logging
│   ├── dq_inject.py       # Demo: inject bad messages to exercise the DQ checks
│   └── reports.py         # Print the aggregates + a data-quality report
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

**5. Run the consumer** (in a second terminal) to persist the topic into
Postgres. It creates the `crypto_prices` table on first run, then writes
batches continuously:

```bash
python -m src.consumer
```

You'll see a line per flushed batch, e.g.
`Batch flushed: 6 consumed, 6 inserted, 0 duplicate(s) ignored`. Stop it with
`Ctrl+C` — the final batch is flushed and offsets committed on the way out.

Inserts are idempotent on `(pair, event_ts)`, so re-running the consumer or
replaying the topic never creates duplicate rows.

**6. Verify rows landed in Postgres:**

```bash
docker exec -it crypto-postgres psql -U crypto -d crypto \
  -c "SELECT count(*) FROM crypto_prices;" \
  -c "SELECT pair, price, event_ts FROM crypto_prices ORDER BY event_ts DESC LIMIT 5;"
```

**Configuration** — override any default via environment variables or a `.env`
file (see `.env.example`). For example, to track different coins:

```bash
COINS=BTC,ETH,DOGE,ADA python -m src.producer
```

**7. Run the aggregator** (a third terminal) to model the stream into
1-minute windows and log data-quality problems. It creates the
`price_aggregates` and `data_quality_events` tables on first run:

```bash
python -m src.aggregator
```

You'll see a line per flush, e.g.
`Flushed: 3 window(s) upserted, 0 DQ event(s) logged`. Each window row holds the
rolling **average**, **high**, and **low** for one trading pair over one minute.
Stop it with `Ctrl+C` — open windows are flushed and offsets committed on exit.

**8. See the aggregates and a data-quality report:**

```bash
python -m src.reports
```

To *prove the DQ checks fire*, inject a batch of deliberately broken messages
(missing fields, negative/absurd prices, a stale timestamp, a duplicate, a
malformed payload) onto the topic, then re-run the report:

```bash
python -m src.dq_inject     # crafts one bad message per check
python -m src.reports       # the DATA-QUALITY REPORT now lists them
```

The aggregator's checks (see `src/quality.py`): **missing/null fields** and
**non-numeric or out-of-range prices** are `error`s and dropped from the
aggregates; **duplicates** (same `pair`+`ts`) are dropped so they aren't
double-counted; **stale/future timestamps** and **implausible price jumps** are
`warning`s that are kept but flagged.

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
- [x] **Week 2 — Persistence.** A consumer (`src/consumer.py`) that reads the
      topic and writes price history into the `crypto_prices` table, with a
      sensible schema, batched idempotent inserts, logging, and graceful
      shutdown. Offsets are committed only after each batch is durably written.
- [x] **Week 3 — Modelling.** An aggregator (`src/aggregator.py`) that reads the
      topic in its own consumer group, computes rolling 1-minute windows per
      trading pair (average + high/low + sample count) into `price_aggregates`,
      and runs data-quality checks (`src/quality.py`) on every message —
      missing fields, out-of-range prices, stale/duplicate ticks — logging each
      problem to `data_quality_events`. `src/reports.py` prints the aggregates
      and a quality report.
- [ ] **Week 4 — Serving & polish.** A dashboard/API over the modelled data,
      containerise the producer/consumer, and document the full case study.

---

## License

[MIT](LICENSE)
