# Financial Trading Platform

A polyglot, multi-tier, event-driven, partially-instrumented enterprise demo system that simulates a real-world financial trading platform. Designed to generate high-volume, variable synthetic traffic with hundreds of meaningful datapoints per second across business, risk, compliance, infrastructure, latency, market microstructure, batch, and async event flows.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Services](#services)
- [Data Reference](#data-reference)
  - [Core Trading Business Metadata](#1-core-trading-business-metadata)
  - [Market Data Attributes](#2-market-data-attributes)
  - [Business KPIs (Front-Office)](#3-business-kpis-front-office)
  - [Risk & Compliance Metadata](#4-risk--compliance-metadata)
  - [System & Infrastructure Attributes](#5-system--infrastructure-attributes)
  - [Batch & ETL Metadata](#6-batch--etl-metadata)
  - [Failure & Stress Indicators](#7-failure--stress-indicators)
  - [Business Health KPIs (Executive Level)](#8-business-health-kpis-executive-level)
  - [Log Format Chaos](#9-log-format-chaos)
- [Installation & Running](#installation--running)
  - [Prerequisites](#prerequisites)
  - [macOS (Apple Silicon / ARM)](#macos-apple-silicon--arm)
  - [macOS (Intel / AMD)](#macos-intel--amd)
  - [Windows](#windows)
  - [GCP / Cloud Deployment](#gcp--cloud-deployment)
  - [Docker Only (Any Platform)](#docker-only-any-platform)
- [Configuration](#configuration)
- [Useful Commands](#useful-commands)
- [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
                                    ┌─────────────────┐
                                    │   Trading UI    │ :8080
                                    │   (React/Vite)  │
                                    └────────┬────────┘
                                             │
                                    ┌────────▼────────┐
                           ┌────────│  API Gateway    │────────┐
                           │        │  (Node.js)      │ :3000  │
                           │        └────────┬────────┘        │
                           │                 │                  │
              ┌────────────▼──┐   ┌──────────▼──┐   ┌─────────▼────┐
              │ Order Service │   │Quote Service│   │ Risk Engine  │
              │ (Python)      │   │   (Go)      │   │  (Python)    │
              │ :8001         │   │   :8002     │   │  :8004       │
              └───────┬───────┘   └──────┬──────┘   └──────┬───────┘
                      │                  │                  │
         ┌────────────▼──────────────────▼──────────────────▼─────────┐
         │                        Kafka                                │
         │  topics: orders, trades, order-updates, risk-updates,       │
         │  market-data, quote-updates, compliance-alerts, legacy-mq,  │
         │  dead-letter                                                │
         └──┬──────────┬──────────────┬───────────────┬───────────────┘
            │          │              │               │
   ┌────────▼──┐ ┌─────▼──────┐ ┌────▼────────┐ ┌───▼──────────┐
   │  Event    │ │  Legacy    │ │   Batch     │ │ Market Data  │
   │ Processor │ │  Adapter   │ │ Reconciler  │ │  Simulator   │
   │ (Python)  │ │  (Python)  │ │  (Python)   │ │    (Go)      │
   └─────┬─────┘ └────────────┘ └──────┬──────┘ └──────────────┘
         │                             │
   ┌─────▼─────┐              ┌────────▼────────┐
   │           ├──────────────│   PostgreSQL    │
   │Elastic    │              │  + Replica      │
   │Search     │              │  :5432 / :5433  │
   └───────────┘              └─────────────────┘
                              ┌─────────────────┐
                              │     Redis       │
                              │     :6379       │
                              └─────────────────┘

         ┌────────────────────┐
         │ Traffic Generator  │  → 500+ req/s with bursts up to 5000/s
         │ (Python)           │
         └────────────────────┘
```

**Total:** 11 services, 4 languages (Python, Go, Node.js, JavaScript), 42 files, ~6,000 lines of code.

---

## Services

| Service | Language | Port | Description |
|---------|----------|------|-------------|
| **API Gateway** | Node.js / Express | 3000 | Request routing, WebSocket relay, rate limiting, trace propagation, chaos injection |
| **Order Service** | Python / FastAPI | 8001 | Full order lifecycle, FIX protocol simulation, audit trail, algo order slicing |
| **Quote Service** | Go / Chi | 8002 | Real-time quotes, L2 order book, VWAP/TWAP/volatility calculations |
| **Analytics** | Python / FastAPI | 8003 | Business KPIs, P&L attribution, execution quality, client analytics |
| **Risk Engine** | Python / FastAPI | 8004 | VaR, Greeks, stress tests, pre-trade risk checks, compliance alerts |
| **Market Data Sim** | Go | — | Geometric Brownian motion price simulation, flash crashes, volatility spikes |
| **Event Processor** | Python | — | Kafka consumer across 7 topics, dead letter queue, cross-system correlation |
| **Legacy Adapter** | Python | — | COBOL batch logs, IBM MQ messages, SMF records, ABEND codes |
| **Batch Reconciler** | Python | — | EOD trade/position/settlement reconciliation, data drift detection |
| **Traffic Generator** | Python | — | Variable load (500 RPS base, 10x bursts), client behavior patterns, chaos |
| **Trading UI** | React / Vite | 8080 | Real-time dashboard with WebSocket, order book, order entry, KPI panels |

### Infrastructure

| Component | Port | Purpose |
|-----------|------|---------|
| **Kafka** (Confluent) | 9092 / 29092 | Event streaming, 12 partitions, 9 topics |
| **Zookeeper** | 2181 | Kafka coordination |
| **PostgreSQL** (primary) | 5432 | Orders, trades, positions, risk, audit trail (10 tables, seeded data) |
| **PostgreSQL** (replica) | 5433 | Read replica for analytics queries |
| **Redis** | 6379 | Quote caching, rate limiting, metrics buffering |
| **Elasticsearch** | 9200 | Trade search, analytics indexing, risk snapshots |

---

## Data Reference

### 1. Core Trading Business Metadata

These describe the business meaning of each trade or order.

#### Order Metadata

Emitted by Order Service → stored in Postgres `orders` table + Kafka `orders` topic.

| Datapoint | Type | Example | Description |
|-----------|------|---------|-------------|
| `order_id` | VARCHAR(50) | `ord-a1b2c3d4` | Unique order identifier (UUID) |
| `parent_order_id` | VARCHAR(50) | `ord-parent-01` | Parent order for algo child slices |
| `client_id` | VARCHAR(50) | `INST-002` | Client / account identifier |
| `symbol` | VARCHAR(20) | `AAPL` | Instrument symbol |
| `isin` | VARCHAR(12) | `US0378331005` | International Securities Identification Number |
| `cusip` | VARCHAR(9) | `037833100` | CUSIP identifier |
| `side` | VARCHAR(4) | `buy` / `sell` | Order side |
| `order_type` | VARCHAR(20) | `limit` | market, limit, stop, stop_limit, ioc, fok |
| `quantity` | DECIMAL | `5000` | Order quantity |
| `limit_price` | DECIMAL | `185.50` | Limit price (for limit/stop_limit orders) |
| `stop_price` | DECIMAL | `184.00` | Stop trigger price |
| `filled_quantity` | DECIMAL | `3500` | Cumulative filled quantity |
| `avg_fill_price` | DECIMAL | `185.47` | Volume-weighted average fill price |
| `status` | VARCHAR(20) | `partially_filled` | new, partially_filled, filled, cancelled, rejected, expired |
| `time_in_force` | VARCHAR(10) | `day` | day, gtc, ioc, fok, gtd |
| `venue` | VARCHAR(50) | `NASDAQ` | NYSE, NASDAQ, CME, dark_pool, internal_crossing |
| `algo_strategy` | VARCHAR(50) | `VWAP` | VWAP, TWAP, POV, Sniper, Iceberg, DMA |
| `broker` | VARCHAR(50) | `GS-PRIME` | Executing broker |
| `counterparty` | VARCHAR(50) | `MM-001` | Trade counterparty |
| `slippage_bps` | DECIMAL | `2.35` | Slippage vs arrival price in basis points |
| `commission` | DECIMAL | `12.50` | Commission charged |
| `fees` | DECIMAL | `3.20` | Exchange/regulatory fees |
| `reject_reason` | TEXT | `insufficient margin` | Reason for rejection (if rejected) |
| `fix_cl_ord_id` | VARCHAR(50) | `CL-a1b2c3` | FIX protocol Client Order ID |
| `fix_orig_cl_ord_id` | VARCHAR(50) | `CL-orig-01` | Original ClOrdID for amends |
| `correlation_id` | VARCHAR(50) | `corr-x1y2z3` | Cross-service correlation ID |
| `created_at` | TIMESTAMP | `2024-03-15T14:30:22Z` | Order creation time |
| `submitted_at` | TIMESTAMP | — | Submission to venue time |
| `acknowledged_at` | TIMESTAMP | — | Venue acknowledgment time |
| `first_fill_at` | TIMESTAMP | — | Time of first fill |
| `completed_at` | TIMESTAMP | — | Final state time |

#### Trade / Execution Metadata

Emitted by Order Service → stored in Postgres `trades` table + Kafka `trades` topic.

| Datapoint | Type | Example | Description |
|-----------|------|---------|-------------|
| `trade_id` | VARCHAR(50) | `trd-e5f6g7` | Unique trade identifier |
| `order_id` | VARCHAR(50) | `ord-a1b2c3d4` | Parent order ID |
| `client_id` | VARCHAR(50) | `INST-002` | Client identifier |
| `symbol` | VARCHAR(20) | `AAPL` | Instrument |
| `side` | VARCHAR(4) | `buy` | Buy or sell |
| `quantity` | DECIMAL | `1000` | Fill quantity |
| `price` | DECIMAL | `185.47` | Execution price |
| `execution_venue` | VARCHAR(50) | `dark_pool` | Where the trade executed |
| `liquidity_flag` | VARCHAR(10) | `maker` | maker, taker, auction |
| `commission` | DECIMAL | `5.00` | Per-trade commission |
| `fees` | DECIMAL | `1.50` | Per-trade fees |
| `clearing_house` | VARCHAR(50) | `DTCC` | Clearing house |
| `settlement_date` | DATE | `2024-03-18` | T+2 settlement date |
| `slippage_bps` | DECIMAL | `2.35` | Slippage vs arrival price |
| `market_impact_bps` | DECIMAL | `1.80` | Estimated market impact |
| `arrival_price` | DECIMAL | `185.50` | Price at order arrival |
| `benchmark_price` | DECIMAL | `185.25` | Benchmark (e.g., VWAP) price |
| `fix_exec_id` | VARCHAR(50) | `EX-h8i9j0` | FIX Execution ID |
| `executed_at` | TIMESTAMP | — | Execution timestamp |
| `executed_at_nanos` | BIGINT | `1710510622551000000` | Nanosecond-precision timestamp |

---

### 2. Market Data Attributes

These are the signals traders and algos react to.

#### Market Microstructure Data

Emitted by Market Data Simulator + Quote Service → Kafka `market-data` topic + Redis cache.
Updated every 100ms for 19 instruments across equity, FX, crypto, and derivatives.

| Datapoint | Type | Example | Description |
|-----------|------|---------|-------------|
| `symbol` | string | `AAPL` | Instrument symbol |
| `bid` | float | `185.25` | Best bid price |
| `ask` | float | `185.27` | Best ask price |
| `bid_size` | int | `500` | Size at best bid |
| `ask_size` | int | `300` | Size at best ask |
| `last_price` | float | `185.26` | Last trade price |
| `last_size` | int | `100` | Last trade size |
| `spread_bps` | float | `1.08` | Bid-ask spread in basis points |
| `book_depth.bids[]` | array | `[{price, size, count}]` | L2 order book — 10 bid levels |
| `book_depth.asks[]` | array | `[{price, size, count}]` | L2 order book — 10 ask levels |
| `book_imbalance` | float | `0.25` | `(bid_total - ask_total) / (bid_total + ask_total)` |
| `volume` | float | `15234567` | Cumulative daily volume |
| `open` | float | `184.80` | Day open price |
| `high` | float | `186.50` | Day high |
| `low` | float | `184.20` | Day low |
| `change_pct` | float | `0.45` | Percent change from open |
| `quote_updates_per_sec` | int | `42` | Quote update rate |
| `timestamp_nanos` | int64 | `1710510622551000000` | Nanosecond timestamp |
| `event_type` | string | `quote_update` | `quote_update` or `trade_print` |

#### Derived Indicators

Calculated in real-time by Quote Service and Market Data Simulator.

| Datapoint | Type | Description |
|-----------|------|-------------|
| `vwap` | float | Volume-Weighted Average Price (cumulative) |
| `twap` | float | Time-Weighted Average Price (running average) |
| `realized_vol` | float | Realized volatility (rolling 100-tick window, annualized) |
| `implied_vol` | float | Implied volatility (simulated) |

#### Market Events (Random Injection)

| Event | Probability | Duration | Effect |
|-------|-------------|----------|--------|
| Volatility spike | 0.1% per tick | 10 ticks | 3x normal volatility |
| Spread widening | 0.05% per tick | 5 ticks | 5x normal spread |
| Flash crash | 0.02% per tick | 20 ticks | 2-5% price drop with recovery |
| Auction imbalance | periodic | variable | Imbalanced book simulation |

#### Instruments

19 instruments across 4 asset classes:

| Symbol | Name | Asset Class | Exchange | Base Price |
|--------|------|-------------|----------|------------|
| AAPL | Apple Inc. | equity | NASDAQ | $185 |
| MSFT | Microsoft Corp. | equity | NASDAQ | $420 |
| GOOGL | Alphabet Inc. | equity | NASDAQ | $175 |
| AMZN | Amazon.com Inc. | equity | NASDAQ | $185 |
| TSLA | Tesla Inc. | equity | NASDAQ | $245 |
| JPM | JPMorgan Chase | equity | NYSE | $195 |
| GS | Goldman Sachs | equity | NYSE | $450 |
| NVDA | NVIDIA Corp. | equity | NASDAQ | $880 |
| META | Meta Platforms | equity | NASDAQ | $500 |
| SPY | SPDR S&P 500 ETF | equity | NYSE | $520 |
| EUR/USD | Euro/Dollar | fx | CME | 1.0850 |
| GBP/USD | Pound/Dollar | fx | CME | 1.2650 |
| USD/JPY | Dollar/Yen | fx | CME | 149.50 |
| BTC-USD | Bitcoin/Dollar | crypto | COINBASE | $67,500 |
| ETH-USD | Ethereum/Dollar | crypto | COINBASE | $3,400 |
| ES | E-mini S&P 500 | derivatives | CME | 5,250 |
| NQ | E-mini NASDAQ 100 | derivatives | CME | 18,500 |
| CL | Crude Oil | derivatives | NYMEX | $78.50 |
| GC | Gold | derivatives | COMEX | $2,350 |

---

### 3. Business KPIs (Front-Office)

Aggregated by Analytics Service → stored in Postgres `business_kpis` table.

#### Execution KPIs

| KPI | Type | Description |
|-----|------|-------------|
| `fill_rate` | ratio (0-1) | Percentage of orders that get filled |
| `avg_slippage_bps` | float | Average slippage in basis points |
| `market_impact` | float | Average market impact in bps |
| `reject_rate` | ratio (0-1) | Percentage of orders rejected |
| `cancel_replace_ratio` | ratio | Ratio of cancel/amend to new orders |
| `hit_ratio` | ratio | Hit ratio for RFQ (request for quote) flow |

#### Revenue KPIs

| KPI | Type | Description |
|-----|------|-------------|
| `total_pnl` | currency | Combined realized + unrealized P&L |
| `realized_pnl` | currency | Realized profit/loss |
| `unrealized_pnl` | currency | Mark-to-market unrealized P&L |
| `pnl_attribution.price` | currency | P&L from price movements |
| `pnl_attribution.fx` | currency | P&L from FX movements |
| `pnl_attribution.carry` | currency | P&L from carry/interest |
| `pnl_attribution.fees` | currency | P&L impact from fees/commissions |
| `total_commission` | currency | Total commission revenue |
| `revenue_per_minute` | currency | Revenue rate |
| `cost_per_trade` | currency | Average cost per executed trade |

#### Client KPIs

| KPI | Type | Description |
|-----|------|-------------|
| `client_profitability` | currency | Net P&L per client |
| `order_flow_toxicity` | score (0-1) | Toxicity of client order flow (adverse selection) |
| `latency_sensitivity` | score (0-1) | Client's sensitivity to execution latency |
| `fill_quality` | score (0-1) | Quality of fills provided to client |
| `retention_score` | score (0-1) | Client retention likelihood |

#### Execution Quality (per venue, per algo)

| KPI | Description |
|-----|-------------|
| `venue_performance` | Fill rate, slippage, latency by venue (NYSE, NASDAQ, dark_pool, etc.) |
| `algo_performance` | Fill rate, slippage, market impact by algo (VWAP, TWAP, POV, etc.) |
| `slippage_distribution` | Histogram of slippage values |

---

### 4. Risk & Compliance Metadata

Real-time risk calculations from Risk Engine → Kafka `risk-updates` + `compliance-alerts`.

#### Risk Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `var_95` | currency | 95th percentile Value-at-Risk |
| `var_99` | currency | 99th percentile Value-at-Risk |
| `expected_shortfall` | currency | Expected loss beyond VaR (CVaR) |
| `delta` | float | First-order price sensitivity (per position) |
| `gamma` | float | Second-order price sensitivity (per position) |
| `vega` | float | Volatility sensitivity (per position) |
| `theta` | float | Time decay (per position) |
| `rho` | float | Interest rate sensitivity (per position) |
| `exposure` | currency | Total market exposure |
| `concentration_pct` | ratio | Concentration by sector/symbol |
| `counterparty_exposure` | currency | Exposure to each counterparty |
| `margin_used` | currency | Margin currently consumed |
| `margin_available` | currency | Remaining margin |
| `risk_score` | int (0-100) | Composite risk score |

#### Stress Test Scenarios

| Scenario | Parameters | Output |
|----------|-----------|--------|
| Market crash | All prices -20% | P&L impact per sector, total loss |
| Rate hike | Interest rates +200bp | P&L impact on rate-sensitive positions |
| Vol spike | Implied vol +50% | P&L impact via Vega exposure |

#### Compliance Metadata

| Datapoint | Description |
|-----------|-------------|
| **Order audit trail** | Every state transition: new → acknowledged → partial_fill → fill → cancel/reject |
| **FIX message logs** | FIX 4.4 messages: 35=D (new), 35=8 (execution), 35=F (cancel), 35=G (amend) |
| **Spoofing detection** | Rapid cancel-replace pattern detection per client |
| **Concentration warnings** | Alerts when single-name or sector concentration exceeds thresholds |
| **Margin calls** | Triggered when margin_used approaches margin_available |
| **Alert types** | spoofing, layering, wash_trade, fat_finger, concentration |
| **Alert severity** | critical, high, medium, low |
| **Regulatory timestamps** | MiFID II / SEC Rule 613 CAT compliant timestamps |

---

### 5. System & Infrastructure Attributes

#### Latency Attributes

| Metric | Source | Description |
|--------|--------|-------------|
| Gateway request latency | API Gateway | Per-request latency (normal: 0-50ms, chaos spike: 500-2000ms) |
| Order execution latency | Order Service | Time from submission to fill (10-500ms simulated) |
| Quote service latency | Quote Service | Per-quote-request latency |
| Kafka end-to-end latency | Event Processor | Time from produce to consume |
| Batch job duration | Batch Reconciler | Reconciliation cycle duration |

#### Gateway Metrics (Prometheus-style at `/metrics`)

| Metric | Type | Description |
|--------|------|-------------|
| `request_count` | counter | Total requests by method, path, status |
| `request_latency_histogram` | histogram | Buckets: 10, 25, 50, 100, 250, 500, 1000, 2500, 5000ms |
| `error_count` | counter | Total 4xx/5xx responses |
| `active_ws_connections` | gauge | Current WebSocket connections |
| `kafka_messages_forwarded` | counter | Messages relayed to WebSocket clients |

#### Trace / Log Attributes

| Attribute | Description |
|-----------|-------------|
| `trace_id` | 16-byte hex, generated at gateway, propagated via `X-Trace-ID` header |
| `span_id` | 8-byte hex, generated per service hop |
| `fix_cl_ord_id` | FIX Client Order ID |
| `fix_exec_id` | FIX Execution ID |
| `correlation_id` | Cross-service correlation identifier |

---

### 6. Batch & ETL Metadata

Batch Reconciler runs every 300 seconds → stored in Postgres `batch_jobs` table.

| Datapoint | Type | Description |
|-----------|------|-------------|
| `job_name` | string | e.g., `RECON-20240315-001` |
| `job_type` | string | reconciliation, eod_risk, settlement, etl |
| `status` | string | running, completed, failed, partial |
| `records_processed` | int | Total records examined |
| `records_failed` | int | Records with errors |
| `mismatches_found` | int | Trade reconciliation mismatches |
| `missing_trades` | int | Trades in Kafka but not in DB (or vice versa) |
| `settlement_mismatches` | int | Unsettled past-due trades |
| `data_drift_detected` | bool | Schema changes or unexpected nulls |
| `duration_seconds` | float | Wall-clock duration of batch run |
| `started_at` | timestamp | Job start time |
| `completed_at` | timestamp | Job completion time |

#### Reconciliation Steps

1. **Trade Reconciliation** — Compare DB trades vs Kafka events, report matched/mismatched/missing
2. **Position Reconciliation** — Verify positions match cumulative trade sums
3. **Settlement Check** — Flag trades past settlement date that aren't settled
4. **Risk Recalculation** — End-of-day VaR recalculation with duration tracking
5. **Data Drift Detection** — Check for schema changes or unexpected null values

---

### 7. Failure & Stress Indicators

These are the signals that reveal cascading failures.

#### Application-Level Failures

| Signal | Source | Rate | Description |
|--------|--------|------|-------------|
| Order rejections | Order Service | 2% | insufficient margin, risk limit exceeded, invalid symbol, market closed |
| DB timeouts | Order Service | 0.5% | Simulated database connection timeout |
| Kafka producer errors | Order Service | 0.5% | Simulated Kafka write failure |
| Deserialization failures | Event Processor | 0.5% | Malformed message parsing errors |
| Correlation failures | Event Processor | 15% | Failed to correlate legacy MQ txn to modern order |
| Circuit breaker warnings | Gateway | 1% | Half-open circuit breaker for downstream services |
| High latency warnings | Gateway | 1% | Latency spike detection |

#### Legacy System Failures

| Signal | Source | Rate | Description |
|--------|--------|------|-------------|
| ABEND codes | Legacy Adapter | 3% | S0C7 (data exception), S0C4 (protection), S322 (time), S806 (load) |
| MQ errors | Legacy Adapter | 2% | MQRC_Q_FULL (2053) — queue capacity exceeded |
| CICS failures | Legacy Adapter | 1% | Transaction processing failures |
| COBOL stack traces | Legacy Adapter | 3% | Multiline fixed-width stack traces |

#### Infrastructure Failures

| Signal | Source | Description |
|--------|--------|-------------|
| Consumer lag | Event Processor | Kafka consumer falling behind (logged every ~2% of messages) |
| Partition rebalancing | Event Processor | Consumer group rebalance events (~60s intervals) |
| Dead letter queue growth | Event Processor | Failed messages routed to `dead-letter` topic |
| Out-of-order events | Event Processor | Timestamp ordering violations |
| Batch partial failures | Batch Reconciler | 5% of reconciliation runs partially fail |
| Batch full failures | Batch Reconciler | 1% of reconciliation runs completely fail |
| CPU spikes | Batch Reconciler | During batch processing windows |

#### Traffic-Generated Chaos

| Signal | Source | Description |
|--------|--------|-------------|
| Burst storms | Traffic Generator | 10x normal load for 10 seconds every 120s |
| Micro-bursts | Traffic Generator | 5x load for 2 seconds every 30-60s |
| Cancel storms | Traffic Generator | 50 cancellations in 2 seconds |
| Fat finger orders | Traffic Generator | Extremely large quantity orders |
| Duplicate submissions | Traffic Generator | Same order submitted multiple times |
| Invalid orders | Traffic Generator | Missing fields, invalid symbols, negative quantities |
| Market open surge | Traffic Generator | 3x load for first 60 seconds |

---

### 8. Business Health KPIs (Executive Level)

Available via Analytics Service GET `/analytics/kpis`.

| KPI | Type | Description |
|-----|------|-------------|
| `total_volume` | currency | Total daily notional trading volume |
| `total_trades` | int | Total executed trades |
| `total_orders` | int | Total orders submitted |
| `revenue_per_minute` | currency | Revenue generation rate |
| `cost_per_trade` | currency | Average all-in cost per trade |
| `system_availability` | ratio (0-1) | Uptime percentage |
| `avg_latency_ms` | float | Average request latency |
| `p99_latency_ms` | float | 99th percentile request latency |
| `fill_rate` | ratio (0-1) | Order fill rate |
| `reject_rate` | ratio (0-1) | Order rejection rate |

---

### 9. Log Format Chaos

The platform simultaneously emits logs in 5 different formats with 4 different timestamp conventions, simulating real-world enterprise log chaos.

#### Format 1: Structured JSON (Gateway, Order Service, Risk Engine, Analytics, Event Processor)

```json
{
  "level": "info",
  "trace_id": "a3f8b2c1d4e5f6a7b8c9d0e1f2a3b4c5",
  "span_id": "1a2b3c4d5e6f7a8b",
  "service": "order-service",
  "order_id": "ord-a1b2c3d4",
  "client_id": "INST-002",
  "symbol": "AAPL",
  "latency_ms": 23.5,
  "msg": "order filled",
  "timestamp": "2024-03-15T14:30:22.551Z"
}
```

#### Format 2: COBOL Fixed-Width Batch Logs (Legacy Adapter)

```
COBRUN  20240315 143022 BATCH-7742 TRADE-PROC   OK   TRDID=TRD-0001234 QTY=000001000 PRC=0000185.5000 ACCT=INST-001      STAT=FILLED  RC=0000
COBRUN  20240315 143022 BATCH-7742 SETTL-CHK    WARN TRDID=TRD-0001234 SETTLE-DT=20240318 MISMATCH=N MARGIN-CALL=N       RC=0004
```

#### Format 3: IBM MQ / CICS Transaction Messages (Legacy Adapter → Kafka `legacy-mq`)

```
MQMD: MsgId=414D5120514D31202020202020 PutDate=20240315 PutTime=14302255 Format=MQSTR
CICS-TXN: TXID=TX7742 TERM=3270A PROG=TRDPROC1 ABCODE=    RESP=NORMAL
```

#### Format 4: SMF Records (Legacy Adapter)

```
SMF030 2024-03-15 14:30:22.551 SYSTEM=PROD1 JOBNAME=TRDBATCH STEPNAME=PROC01 CPU=00:00:01.23 ELAPSED=00:00:05.67 EXCP=0000012
```

#### Format 5: Mixed Plaintext + JSON (Batch Reconciler)

```
2024-03-15 23:00:01 [BATCH-RECON] ====================================
2024-03-15 23:00:01 [BATCH-RECON] END-OF-DAY RECONCILIATION STARTING
2024-03-15 23:00:01 [BATCH-RECON] ====================================
2024-03-15 23:00:01 [BATCH-RECON] Step 1/5: Trade Reconciliation
{"job_id": "RECON-20240315", "step": 1, "status": "complete", "matched": 15234, "mismatched": 3}
```

#### Timestamp Formats

| Format | Example | Source |
|--------|---------|--------|
| ISO 8601 | `2024-03-15T14:30:22.551Z` | JSON logs |
| YYYYMMDD HHMMSS | `20240315 143022` | COBOL logs |
| Julian date | `2024075` (March 15 = day 75) | Some legacy records |
| Epoch milliseconds | `1710510622551` | Some MQ messages |

---

### Kafka Topics

| Topic | Producers | Consumers | Volume |
|-------|-----------|-----------|--------|
| `orders` | Order Service | Event Processor, Legacy Adapter | ~200/s |
| `trades` | Order Service | Event Processor, Analytics, Legacy Adapter | ~150/s |
| `order-updates` | Order Service | Event Processor, Analytics | ~300/s |
| `market-data` | Market Data Sim | Quote Service, Event Processor, Gateway (WS) | ~190/s (19 symbols × 10/s) |
| `quote-updates` | Quote Service | — | ~190/s |
| `risk-updates` | Risk Engine | Event Processor, Analytics | ~4/s |
| `compliance-alerts` | Risk Engine | Event Processor | ~1/s |
| `legacy-mq` | Legacy Adapter | Event Processor | ~100/s |
| `dead-letter` | Event Processor | — | ~5/s |

### Postgres Tables

| Table | Rows/day (est.) | Description |
|-------|-----------------|-------------|
| `instruments` | 25 (static) | Seeded instrument reference data |
| `clients` | 20 (static) | Seeded client/account data |
| `orders` | ~500K | All orders with full metadata |
| `trades` | ~400K | All executions with nanosecond timestamps |
| `positions` | ~400 | Current positions per client×symbol |
| `risk_snapshots` | ~17K | VaR/Greeks snapshots every 5 seconds |
| `order_audit_trail` | ~1.5M | Every order state transition |
| `compliance_alerts` | ~5K | Spoofing, concentration, margin alerts |
| `market_data_snapshots` | ~100K | Periodic market data captures |
| `batch_jobs` | ~288 | Reconciliation job results |
| `business_kpis` | ~8.6K | Aggregated KPI snapshots |

### Seeded Clients

| Client ID | Name | Type | Tier | Risk Limit |
|-----------|------|------|------|------------|
| INST-001 | Bridgewater Associates | institutional | platinum | $500M |
| INST-002 | Renaissance Technologies | institutional | platinum | $1B |
| INST-003 | Citadel Securities | market_maker | platinum | $2B |
| INST-004 | Two Sigma Investments | institutional | platinum | $750M |
| INST-005 | DE Shaw & Co | institutional | gold | $300M |
| INST-006 | Man Group | institutional | gold | $200M |
| INST-007 | Millennium Management | institutional | gold | $400M |
| INST-008 | Point72 Asset Management | institutional | gold | $250M |
| HF-001 | Alpha Capital Partners | institutional | silver | $50M |
| HF-002 | Quantum Trading LLC | institutional | silver | $30M |
| HF-003 | Nordic Arbitrage Fund | institutional | silver | $25M |
| PROP-001 | Internal Prop Desk A | proprietary | platinum | $100M |
| PROP-002 | Internal Prop Desk B | proprietary | gold | $50M |
| RET-001 | John Smith | retail | bronze | $100K |
| RET-002 | Jane Doe | retail | bronze | $250K |
| RET-003 | Bob Johnson | retail | silver | $500K |
| MM-001 | Virtu Financial | market_maker | platinum | $3B |
| MM-002 | Flow Traders | market_maker | gold | $500M |
| ALGO-001 | Systematic Alpha Fund | institutional | gold | $150M |
| ALGO-002 | High Frequency Strategies | institutional | platinum | $800M |

---

## Installation & Running

### Prerequisites

All platforms need:
- **Docker** 24+ and **Docker Compose** v2 (included with Docker Desktop)
- **8 GB RAM** minimum available for Docker (16 GB recommended)
- **20 GB** free disk space
- **Ports available:** 2181, 3000, 5432, 5433, 6379, 8001-8004, 8080, 9092, 9200

---

### macOS (Apple Silicon / ARM)

Tested on M1, M2, M3, M4 Macs running macOS 13+.

#### Step 1: Install Docker Desktop

```bash
# Install via Homebrew
brew install --cask docker

# Or download from https://docs.docker.com/desktop/install/mac-install/
# Choose "Apple Silicon" / "Apple chip"
```

Launch Docker Desktop and in **Settings → Resources**, allocate:
- **CPUs:** 6+
- **Memory:** 8 GB minimum (12-16 GB recommended)
- **Disk:** 40 GB+

#### Step 2: Clone and start

```bash
git clone <repository-url> trading-platform
cd trading-platform

# Phased startup (recommended)
./scripts/start.sh

# Or start everything at once (may hit race conditions on slower machines)
docker compose up -d
```

#### Step 3: Verify

```bash
# Check all containers are running
docker compose ps

# Check gateway health
curl http://localhost:3000/health

# Open the trading UI
open http://localhost:8080
```

#### ARM-specific notes

- All images used (Confluent Kafka, Postgres, Redis, Elasticsearch, Node, Python, Go, Nginx) have native `linux/arm64` builds. No emulation needed.
- If you see "no matching manifest for linux/arm64" errors for any image, add `platform: linux/amd64` to that service in `docker-compose.yml` to use Rosetta emulation. This is slower but works.
- Elasticsearch on ARM may require: `docker compose exec elasticsearch sysctl -w vm.max_map_count=262144` if you see bootstrap check failures.

#### Stop

```bash
./scripts/stop.sh
# Or: docker compose down

# To also remove volumes (deletes all data):
docker compose down -v
```

---

### macOS (Intel / AMD)

Tested on Intel Macs running macOS 12+.

#### Step 1: Install Docker Desktop

```bash
# Install via Homebrew
brew install --cask docker

# Or download from https://docs.docker.com/desktop/install/mac-install/
# Choose "Intel chip"
```

Configure Docker Desktop resources same as ARM above (8 GB+ RAM).

#### Step 2: Clone and start

```bash
git clone <repository-url> trading-platform
cd trading-platform
./scripts/start.sh
```

#### Step 3: Verify

```bash
docker compose ps
curl http://localhost:3000/health
open http://localhost:8080
```

#### Intel-specific notes

- All images are `linux/amd64` native. No compatibility issues.
- Older Intel Macs (pre-2018) may be slower due to limited Docker VM performance. Consider reducing `TARGET_RPS` in `docker-compose.yml` from 500 to 100.
- If Elasticsearch fails with `max virtual memory areas vm.max_map_count [65530] is too low`:
  ```bash
  # Inside the Docker VM (macOS doesn't expose this directly)
  # Restart Docker Desktop — it sets this automatically
  ```

---

### Windows

Tested on Windows 10 (21H2+) and Windows 11 with WSL 2.

#### Step 1: Install WSL 2

Open PowerShell as Administrator:

```powershell
# Install WSL 2 with Ubuntu (if not already installed)
wsl --install

# Restart your computer after installation
# Then set WSL 2 as default
wsl --set-default-version 2
```

#### Step 2: Install Docker Desktop

1. Download Docker Desktop for Windows from https://docs.docker.com/desktop/install/windows-install/
2. During installation, ensure **"Use WSL 2 instead of Hyper-V"** is checked
3. After installation, open Docker Desktop → **Settings**:
   - **General:** Ensure "Use the WSL 2 based engine" is checked
   - **Resources → WSL Integration:** Enable for your Ubuntu distro
   - **Resources → Advanced:** Allocate 8 GB+ RAM, 6+ CPUs

#### Step 3: Clone and start (in WSL 2 terminal)

Open Ubuntu (WSL 2) terminal:

```bash
# Clone into the WSL filesystem (NOT /mnt/c/ — performance is much better on the Linux filesystem)
cd ~
git clone <repository-url> trading-platform
cd trading-platform

# Make scripts executable
chmod +x scripts/*.sh

# Start
./scripts/start.sh
```

#### Step 4: Verify

```bash
docker compose ps
curl http://localhost:3000/health
```

Open a browser on Windows and navigate to `http://localhost:8080`.

#### Windows-specific notes

- **Always run from the WSL 2 filesystem** (`~/trading-platform`), not from `/mnt/c/Users/...`. Docker volume mounts from the Windows filesystem are 5-10x slower.
- If Docker commands don't work in WSL, ensure Docker Desktop's WSL Integration is enabled for your distro in Settings → Resources → WSL Integration.
- **Firewall:** Windows Firewall may block Docker ports. If `localhost:8080` doesn't work from the browser, try `127.0.0.1:8080` or add inbound rules for ports 3000, 8080.
- **Line endings:** If you cloned on Windows (not WSL), convert line endings:
  ```bash
  # In WSL
  sudo apt install dos2unix
  find . -name "*.sh" -exec dos2unix {} \;
  find . -name "*.py" -exec dos2unix {} \;
  ```
- **Memory:** WSL 2 can consume a lot of RAM. Create `%UserProfile%\.wslconfig` if needed:
  ```ini
  [wsl2]
  memory=12GB
  processors=6
  ```
- **PowerShell alternative** (without WSL): You can also run directly from PowerShell, but use forward slashes and `docker compose` commands directly instead of the shell scripts:
  ```powershell
  cd trading-platform
  docker compose up -d
  ```

---

### GCP / Cloud Deployment

#### Option A: Single VM (GCE) — Simplest

Best for demos and testing. Runs everything on one VM.

```bash
# Create a VM (e2-standard-8: 8 vCPU, 32 GB RAM recommended)
gcloud compute instances create trading-platform-demo \
  --zone=us-central1-a \
  --machine-type=e2-standard-8 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=100GB \
  --tags=trading-demo

# Allow traffic to the UI and API
gcloud compute firewall-rules create trading-demo-ports \
  --allow=tcp:3000,tcp:8080 \
  --target-tags=trading-demo \
  --source-ranges=0.0.0.0/0

# SSH in
gcloud compute ssh trading-platform-demo --zone=us-central1-a
```

On the VM:

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker

# Install Docker Compose plugin
sudo apt-get update
sudo apt-get install docker-compose-plugin

# Increase vm.max_map_count for Elasticsearch
sudo sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf

# Clone and start
git clone <repository-url> trading-platform
cd trading-platform
./scripts/start.sh
```

Access:
- Trading UI: `http://<EXTERNAL_IP>:8080`
- API Gateway: `http://<EXTERNAL_IP>:3000`

```bash
# Get external IP
gcloud compute instances describe trading-platform-demo \
  --zone=us-central1-a \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

Cleanup:
```bash
gcloud compute instances delete trading-platform-demo --zone=us-central1-a
gcloud compute firewall-rules delete trading-demo-ports
```

#### Option B: GKE (Google Kubernetes Engine)

For a more production-like deployment.

```bash
# Create a GKE cluster
gcloud container clusters create trading-platform \
  --zone=us-central1-a \
  --num-nodes=3 \
  --machine-type=e2-standard-4 \
  --enable-autoscaling \
  --min-nodes=3 \
  --max-nodes=6

# Get credentials
gcloud container clusters get-credentials trading-platform --zone=us-central1-a
```

You'll need to:
1. Push Docker images to Google Artifact Registry (or GCR)
2. Convert `docker-compose.yml` to Kubernetes manifests (use `kompose convert` or write them manually)
3. Deploy infrastructure (Kafka, Postgres, Redis, ES) via Helm charts:

```bash
# Add Helm repos
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add elastic https://helm.elastic.co

# Install infrastructure
helm install kafka bitnami/kafka --set replicaCount=3
helm install postgres bitnami/postgresql --set auth.database=trading,auth.username=trading,auth.password=trading123
helm install redis bitnami/redis
helm install elasticsearch elastic/elasticsearch --set replicas=1,resources.requests.memory=2Gi
```

4. Build and push service images:

```bash
# Configure Artifact Registry
gcloud artifacts repositories create trading-platform \
  --repository-format=docker \
  --location=us-central1

# Build and push each service
for svc in gateway order-service quote-service analytics risk-engine event-processor legacy-adapter batch-reconciler market-data-sim ui; do
  docker build -t us-central1-docker.pkg.dev/PROJECT_ID/trading-platform/$svc:latest ./services/$svc
  docker push us-central1-docker.pkg.dev/PROJECT_ID/trading-platform/$svc:latest
done

docker build -t us-central1-docker.pkg.dev/PROJECT_ID/trading-platform/traffic-generator:latest ./traffic-generator
docker push us-central1-docker.pkg.dev/PROJECT_ID/trading-platform/traffic-generator:latest
```

5. Create Kubernetes Deployments and Services for each component
6. Expose the UI and Gateway via a LoadBalancer or Ingress

#### Option C: Cloud Run (Serverless — Partial)

Cloud Run can host the stateless HTTP services (gateway, order-service, quote-service, analytics, risk-engine, UI). However, Kafka consumers (event-processor, legacy-adapter), the market data simulator, and infrastructure (Kafka, Postgres, Redis, ES) need to run elsewhere (GCE VM or GKE).

This is not recommended for the full demo but can work for a lightweight version.

---

### Frontend on Cloud — How It Works

When running locally, the UI is at `http://localhost:8080` and everything "just works" because all services are on the same machine. On cloud deployments, there are extra considerations for the frontend to connect to the backend correctly.

#### Architecture

```
Browser (your laptop)
  │
  │  HTTP :8080 / WSS :8080
  ▼
┌──────────────────────────┐
│  UI container (Nginx)     │  ← Serves static React app + proxies API/WS
│  - GET /             → SPA │
│  - GET /api/*  → gateway   │
│  - GET /ws     → gateway   │
│  - GET /health → gateway   │
│  - GET /metrics→ gateway   │
└──────────┬───────────────┘
           │ internal Docker network
           ▼
┌──────────────────────────┐
│  API Gateway (Node.js)    │  :3000 (internal only on cloud)
└──────────────────────────┘
```

The key design: **Nginx in the UI container proxies everything.** The browser only talks to port 8080. All `/api/*`, `/ws`, `/health`, and `/metrics` requests are reverse-proxied to the gateway container over Docker's internal network. This means:

- The browser never needs to know the gateway's address
- WebSocket connections go through `ws://<your-cloud-ip>:8080/ws` (or `wss://` with TLS)
- No CORS issues — everything is same-origin from the browser's perspective
- You only need to expose **one port** (8080) to the internet

#### Step 1: Expose only port 8080

On cloud, you only need to open port 8080 (the UI). Do **not** expose the gateway (3000) or any other service port to the internet.

**GCE firewall:**
```bash
# Only expose the UI port
gcloud compute firewall-rules create trading-demo-ui \
  --allow=tcp:8080 \
  --target-tags=trading-demo \
  --source-ranges=0.0.0.0/0

# Optional: expose gateway too (for direct API testing)
# gcloud compute firewall-rules create trading-demo-api \
#   --allow=tcp:3000 \
#   --target-tags=trading-demo \
#   --source-ranges=YOUR_IP/32
```

**GKE / Kubernetes:**
```yaml
# Expose only the UI service
apiVersion: v1
kind: Service
metadata:
  name: trading-ui
spec:
  type: LoadBalancer
  ports:
    - port: 80
      targetPort: 80
  selector:
    app: trading-ui
```

**AWS Security Group:**
```bash
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxx \
  --protocol tcp --port 8080 \
  --cidr 0.0.0.0/0
```

#### Step 2: Access the UI

```bash
# Get the external IP (GCE example)
EXTERNAL_IP=$(gcloud compute instances describe trading-platform-demo \
  --zone=us-central1-a \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

echo "Trading UI: http://${EXTERNAL_IP}:8080"
```

Open `http://<EXTERNAL_IP>:8080` in your browser. The dashboard will:
1. Load the React SPA from Nginx
2. Fetch quotes via `GET /api/quotes/snapshot` → Nginx proxies to gateway → quote service
3. Connect WebSocket via `ws://<EXTERNAL_IP>:8080/ws` → Nginx upgrades and proxies to gateway
4. All other API calls (`/api/v1/orders`, `/api/v1/analytics/kpis`, etc.) go through Nginx

#### Step 3: Adding HTTPS / TLS (production cloud)

For production-like deployments, you should terminate TLS. Three options:

**Option A: Cloud load balancer (recommended for GCP/AWS/Azure)**

Put a cloud load balancer in front of the UI container. The LB handles TLS termination; Nginx still proxies internally over HTTP.

```bash
# GCP: Create an HTTPS load balancer with managed SSL cert
gcloud compute addresses create trading-ip --global
gcloud compute ssl-certificates create trading-cert \
  --domains=trading.example.com \
  --global

# Then create a backend service pointing to your VM/GKE on port 8080
# The LB terminates TLS and forwards HTTP to Nginx
```

The React app already handles this — it detects `https:` protocol and uses `wss://` for WebSocket:
```javascript
// From App.jsx — already implemented
const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
const url = `${proto}://${window.location.host}/ws`;
```

**Option B: Nginx TLS termination (self-signed or Let's Encrypt)**

Create a cloud-specific nginx config with TLS:

```bash
# On the cloud VM, generate a self-signed cert (for demos)
mkdir -p /path/to/certs
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout /path/to/certs/selfsigned.key \
  -out /path/to/certs/selfsigned.crt \
  -subj "/CN=trading-demo"
```

Create `services/ui/nginx-ssl.conf`:
```nginx
server {
    listen 80;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name _;

    ssl_certificate /etc/nginx/certs/selfsigned.crt;
    ssl_certificate_key /etc/nginx/certs/selfsigned.key;

    gzip on;
    gzip_types text/plain text/css application/json application/javascript text/xml;

    location / {
        root /usr/share/nginx/html;
        try_files $uri $uri/ /index.html;
    }

    location /api/ {
        proxy_pass http://gateway:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /health {
        proxy_pass http://gateway:3000;
    }

    location /metrics {
        proxy_pass http://gateway:3000;
    }

    location /ws {
        proxy_pass http://gateway:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 86400s;
        proxy_send_timeout 86400s;
    }
}
```

Mount the cert and custom config in `docker-compose.yml`:
```yaml
  ui:
    build: ./services/ui
    ports:
      - "443:443"
      - "8080:80"   # redirect to HTTPS
    volumes:
      - ./services/ui/nginx-ssl.conf:/etc/nginx/conf.d/default.conf
      - /path/to/certs:/etc/nginx/certs:ro
```

**Option C: Caddy or Traefik reverse proxy**

Add a Caddy container in front of the UI for automatic Let's Encrypt:

```yaml
  caddy:
    image: caddy:2-alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile
      - caddy_data:/data
    depends_on:
      - ui
```

`Caddyfile`:
```
trading.example.com {
    reverse_proxy ui:80
}
```

This gives you automatic HTTPS with zero config (Caddy handles Let's Encrypt cert provisioning).

#### Step 4: Verify frontend connectivity

Once deployed, verify all frontend connections work:

```bash
EXTERNAL_IP=<your-cloud-ip>

# 1. Static assets load
curl -s -o /dev/null -w "%{http_code}" http://${EXTERNAL_IP}:8080/
# Expected: 200

# 2. API proxy works (quotes via Nginx → gateway → quote service)
curl -s http://${EXTERNAL_IP}:8080/api/quotes/snapshot | head -c 200
# Expected: JSON array of quotes

# 3. Health proxy works
curl -s http://${EXTERNAL_IP}:8080/health
# Expected: {"status":"ok", ...}

# 4. WebSocket works (quick test with wscat)
npx wscat -c ws://${EXTERNAL_IP}:8080/ws
# Expected: JSON messages streaming every ~100ms

# 5. Order submission works through the UI proxy
curl -X POST http://${EXTERNAL_IP}:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"client_id":"INST-002","symbol":"AAPL","side":"buy","order_type":"market","quantity":100}'
# Expected: JSON order response with order_id
```

#### Troubleshooting Frontend on Cloud

| Problem | Cause | Solution |
|---------|-------|----------|
| UI loads but no data | Nginx can't reach gateway container | Check `docker compose ps` — gateway must be running. Check `docker compose logs ui` for proxy errors |
| WebSocket shows "disconnected" (red dot) | WebSocket upgrade blocked | Ensure Nginx has `proxy_http_version 1.1` and `Upgrade` headers. If behind a cloud LB, enable WebSocket support on the LB |
| "Mixed content" browser error | Page loaded via HTTPS but WS/API uses HTTP | The app auto-detects protocol. Ensure your TLS termination is consistent. Check browser console for the actual URL being called |
| CORS errors in console | Shouldn't happen (same-origin via Nginx proxy) | If you're accessing the gateway directly (port 3000) instead of through the UI (port 8080), use the UI port. The gateway has CORS enabled as fallback |
| WebSocket connects then immediately drops | Cloud LB has short idle timeout | Increase LB idle timeout to 3600s. The Nginx config already sets `proxy_read_timeout 86400s` |
| Slow initial load on cloud | Large JS bundle over high-latency link | Nginx gzip is enabled by default. Consider CDN for static assets in production |
| "502 Bad Gateway" on /api/* | Gateway container not ready yet | Wait 30s after startup. Gateway depends on Kafka/Redis which take time to initialize |

---

### Docker Only (Any Platform)

If you already have Docker and Docker Compose installed on any platform:

```bash
cd trading-platform

# Option 1: Phased startup (recommended — waits for infrastructure health)
./scripts/start.sh

# Option 2: All at once
docker compose up -d

# Option 3: Build from scratch (first time or after code changes)
docker compose build --no-cache
docker compose up -d

# Option 4: Start without traffic generator (quieter, for manual testing)
docker compose up -d --scale traffic-generator=0
```

#### Scaling individual services

```bash
# Run 3 instances of the event processor
docker compose up -d --scale event-processor=3

# Run 2 quote services
docker compose up -d --scale quote-service=2
```

#### Monitoring logs

```bash
# All logs (firehose)
docker compose logs -f

# Specific service
docker compose logs -f order-service

# Legacy chaos (COBOL + MQ + SMF)
docker compose logs -f legacy-adapter

# Batch reconciliation
docker compose logs -f batch-reconciler

# Traffic stats
docker compose logs -f traffic-generator

# Multiple services
docker compose logs -f gateway order-service risk-engine

# With timestamps
docker compose logs -f -t gateway
```

---

## Configuration

Key environment variables that can be modified in `docker-compose.yml`:

| Variable | Service | Default | Description |
|----------|---------|---------|-------------|
| `TARGET_RPS` | traffic-generator | `500` | Base requests per second |
| `BURST_MULTIPLIER` | traffic-generator | `10` | Multiplier during burst periods |
| `CHAOS_ENABLED` | traffic-generator | `true` | Enable chaos injection (invalid orders, storms) |
| `TICK_INTERVAL_MS` | market-data-sim | `100` | Market data tick interval in ms |
| `RECONCILIATION_INTERVAL` | batch-reconciler | `300` | Seconds between reconciliation runs |
| `KAFKA_NUM_PARTITIONS` | kafka | `12` | Default Kafka partition count |

---

## Useful Commands

```bash
# View running containers and resource usage
docker compose ps
docker stats

# Check Kafka topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Check Kafka consumer lag
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --describe --all-groups

# Query Postgres
docker compose exec postgres psql -U trading -d trading -c "SELECT count(*) FROM orders;"
docker compose exec postgres psql -U trading -d trading -c "SELECT status, count(*) FROM orders GROUP BY status;"
docker compose exec postgres psql -U trading -d trading -c "SELECT symbol, count(*), avg(slippage_bps) FROM trades GROUP BY symbol ORDER BY count(*) DESC;"

# Check Redis
docker compose exec redis redis-cli INFO keyspace
docker compose exec redis redis-cli KEYS "quote:*"

# Check Elasticsearch
curl http://localhost:9200/_cat/indices?v
curl http://localhost:9200/trades/_count

# API endpoints
curl http://localhost:3000/health
curl http://localhost:3000/metrics
curl http://localhost:3000/api/v1/quotes/AAPL
curl http://localhost:3000/api/v1/quotes/book/AAPL
curl http://localhost:3000/api/v1/analytics/kpis
curl http://localhost:3000/api/v1/risk/INST-002
curl http://localhost:3000/api/v1/risk/portfolio

# Submit a test order
curl -X POST http://localhost:3000/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "INST-002",
    "symbol": "AAPL",
    "side": "buy",
    "order_type": "limit",
    "quantity": 1000,
    "limit_price": 185.50,
    "time_in_force": "day",
    "algo_strategy": "VWAP"
  }'
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Elasticsearch won't start | Run `sudo sysctl -w vm.max_map_count=262144` on the Docker host |
| Kafka takes long to start | Wait 30-60 seconds. Use `./scripts/start.sh` for phased startup |
| Services crash on startup | Infrastructure may not be ready. Restart: `docker compose restart order-service` |
| Port already in use | Stop conflicting services or change ports in `docker-compose.yml` |
| Out of memory | Increase Docker Desktop RAM to 12+ GB. Reduce `TARGET_RPS` to 100 |
| Slow on Windows | Ensure project is on WSL 2 filesystem, not `/mnt/c/` |
| Go services fail to build | The Dockerfile runs `go mod tidy` during build. Ensure internet access |
| UI shows "disconnected" | Gateway may not be ready yet. Wait 30s and refresh |
| No market data in UI | Check market-data-sim and quote-service: `docker compose logs market-data-sim` |

---

## Why Observability Tools Struggle With This

This platform simultaneously produces:

1. **Partial traces** — Some services emit trace IDs, some don't. Legacy services emit nothing standard.
2. **Log chaos** — 5 simultaneous log formats (JSON, COBOL, MQ, SMF, mixed plaintext+JSON) with 4 timestamp conventions.
3. **Metric cardinality explosions** — Client IDs, order IDs, dynamic Kafka topics as labels.
4. **Async gaps** — Kafka → Python → Go → DB chains where most tools lose distributed context.
5. **Batch job spikes** — No traces, huge CPU/IO bursts, long time windows, hard to correlate with real-time flow.
6. **Legacy noise** — SMF records, MQ messages, CICS transactions, COBOL stack traces, non-OTLP formats.
7. **Cascading failures** — Slow DB → thread pool saturation → queue buildup → Kafka lag → UI timeouts.
8. **Fan-out/fan-in** — One order creates multiple child orders (algo slicing), each with multiple fills, each producing multiple events across multiple topics.
9. **Variable load** — 500 req/s baseline with 5000 req/s bursts, quiet periods, and chaos injection.
10. **Mixed protocols** — HTTP REST, WebSocket, Kafka, FIX-style messages, MQ messages, batch file processing.
