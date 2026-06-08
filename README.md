# Legacy Mainframe Trading Platform (Test App)

A polyglot, multi-tier, event-driven demo system that simulates a real-world financial trading platform. It deliberately mixes modern microservices with legacy mainframe-style outputs (COBOL batch logs, IBM MQ/CICS messages, SMF records) to generate high-volume, messy, partially-instrumented telemetry — the kind of workload that stresses observability tooling.

It is built as a **test / reference workload**: hundreds of meaningful datapoints per second across business, risk, compliance, infrastructure, latency, market-microstructure, batch, and async event flows.

> Full architecture, data dictionary, deployment guides (macOS, Windows/WSL2, GCP/GKE/Cloud Run), and troubleshooting live in **[`trading-platform/README.md`](trading-platform/README.md)**.

## What's inside

`trading-platform/` — 11 services across 4 languages, plus supporting infrastructure:

| Service | Language | Port | Role |
|---------|----------|------|------|
| API Gateway | Node.js / Express | 3000 | Routing, WebSocket relay, rate limiting, trace propagation, chaos injection |
| Order Service | Python / FastAPI | 8001 | Order lifecycle, FIX simulation, audit trail, algo slicing |
| Quote Service | Go / Chi | 8002 | Real-time quotes, L2 order book, VWAP/TWAP/volatility |
| Analytics | Python / FastAPI | 8003 | Business KPIs, P&L attribution, execution quality |
| Risk Engine | Python / FastAPI | 8004 | VaR, Greeks, stress tests, pre-trade checks, compliance alerts |
| Market Data Sim | Go | — | GBM price simulation, flash crashes, vol spikes |
| Event Processor | Python | — | Kafka consumer across 7 topics, DLQ, cross-system correlation |
| Legacy Adapter | Python | — | COBOL batch logs, IBM MQ messages, SMF records, ABEND codes |
| Batch Reconciler | Python | — | EOD trade/position/settlement reconciliation, drift detection |
| Traffic Generator | Python | — | Variable load (500 RPS base, 10x bursts) + chaos |
| Trading UI | React / Vite | 8080 | Real-time dashboard, order book, order entry, KPI panels |

Infrastructure: **Kafka + Zookeeper** (9 topics), **PostgreSQL** primary + read replica, **Redis**, and **Elasticsearch**.

## Why it exists

The platform intentionally produces the things that break tracing and log pipelines:

- Partial / missing distributed traces (legacy services emit nothing standard)
- Five simultaneous log formats (JSON, COBOL fixed-width, IBM MQ/CICS, SMF, mixed plaintext+JSON) with four timestamp conventions
- High-cardinality metrics (client IDs, order IDs, dynamic Kafka topics as labels)
- Async fan-out/fan-in (algo parent → child orders → multiple fills → multiple topics)
- Batch spikes, cascading failures, and variable bursty load (up to ~5000 req/s)

This makes it a useful target for evaluating observability, log-parsing, and noise-reduction tools.

## Quick start

Requires Docker 24+ and Docker Compose v2 (8 GB+ RAM free).

```bash
cd trading-platform
./scripts/start.sh        # phased startup (recommended)
# or: docker compose up -d

# verify
docker compose ps
curl http://localhost:3000/health
open http://localhost:8080   # Trading UI
```

See [`trading-platform/README.md`](trading-platform/README.md) for platform-specific setup (Apple Silicon, Intel, Windows/WSL2, cloud), the full datapoint reference, and troubleshooting.

## Repository layout

```
trading-platform/        The full microservices trading platform (see its README)
README.md                This overview
```
