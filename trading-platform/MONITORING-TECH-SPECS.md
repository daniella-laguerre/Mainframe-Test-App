# Trading Platform — Monitoring/APM Vendor Fit Specs

Use this document to evaluate whether a monitoring/observability product (Splunk Observability, AppDynamics, Datadog, New Relic, Dynatrace, etc.) fits this stack.

**Last updated:** 2026-04-27

---

## 1. High-Level Architecture

| Attribute | Value |
|---|---|
| Architecture style | Polyglot microservices |
| Service count | 11 application services + 5 infrastructure components |
| Communication | HTTP/REST, WebSocket, Kafka events |
| Deployment model | Single Windows VM (native processes, no containers) |
| Front door | IIS (reverse proxy, static UI hosting, SSL termination) |
| Internal gateway | Node.js Express (request routing, aggregation, rate limiting, WebSocket relay) |

---

## 2. Application Services

| Service | Language | Runtime | Framework | Port | Type |
|---|---|---|---|---|---|
| API Gateway | Node.js | Node 20 LTS | Express 4 + ws | 3000 | HTTP + WebSocket |
| Order Service | Python | Python 3.11 | FastAPI + uvicorn | 8001 | HTTP (async) |
| Quote Service | Go | Go 1.22 | Chi router | 8002 | HTTP |
| Analytics | Python | Python 3.11 | FastAPI + uvicorn | 8003 | HTTP (async) |
| Risk Engine | Python | Python 3.11 | FastAPI + uvicorn | 8004 | HTTP (async) |
| Market Data Sim | Go | Go 1.22 | (kafka-go producer) | — | Kafka producer |
| Event Processor | Python | Python 3.11 | aiokafka | — | Kafka consumer (multi-topic) |
| Legacy Adapter | Python | Python 3.11 | aiokafka | — | Kafka consumer/producer |
| Batch Reconciler | Python | Python 3.11 | aiokafka, asyncpg | — | Cron-style batch job |
| Traffic Generator | Python | Python 3.11 | httpx, asyncio | — | Load generator (internal) |
| Trading UI | JavaScript | Browser | React 18 + Vite | 80/443 (IIS) | SPA frontend |

**Key observations:**
- **7 Python services** all using FastAPI/asyncio — auto-instrumentation maturity is critical
- **2 Go services** — Go has the weakest auto-instrumentation story across vendors
- **3 services have no inbound HTTP** (event-processor, legacy-adapter, batch-reconciler) — they need agent support for **non-HTTP workloads** (Kafka consumer instrumentation, cron job tracking)
- **WebSocket relay** in the gateway — vendor must support WebSocket span correlation

---

## 3. Infrastructure Components

| Component | Version | Port | Notes |
|---|---|---|---|
| Apache Kafka | 3.7.0 | 9092 | Bare-metal install (no Confluent platform) |
| Apache Zookeeper | 3.7.0 (bundled) | 2181 | Kafka coordination |
| PostgreSQL | 16 | 5432 (primary), 5433 (replica) | Native Windows install |
| Redis | 7 (Memurai for Windows) | 6379 | Memurai is a Redis-compatible drop-in |
| Elasticsearch | 8.11.0 | 9200 | Single-node, security disabled |
| IIS | Windows Server 2022 built-in | 80, 443 | Reverse proxy + static UI |

**Vendor must support:**
- Kafka broker metrics (broker JMX, topic-level lag, partition skew)
- **Kafka consumer lag** per consumer group (critical SLI)
- Postgres query performance (slow query log, lock waits, replication lag)
- Redis hit rate, evictions, memory pressure
- Elasticsearch JVM heap, query latency, indexing rate
- IIS request logs, error codes, response times

---

## 4. Observability Endpoints (Already Reserved)

| Port | Protocol | Purpose |
|---|---|---|
| 4317 | TCP (gRPC) | OTLP gRPC ingestion |
| 4318 | UDP | AgenticUDP / custom telemetry |
| 4320 | TCP | Reserved for additional telemetry |

**Implication:** The platform is **OpenTelemetry-ready**. Vendors that natively accept OTLP (or ship an OTel Collector) will integrate cleanly. Proprietary-only protocols (e.g., AppDynamics Smart Agent without OTel bridge) will require additional translation.

---

## 5. Host & Deployment Environment

| Attribute | Value |
|---|---|
| Cloud provider | Google Cloud Platform |
| Compute service | Compute Engine (GCE) |
| OS | Windows Server 2022 |
| Machine type | `e2-standard-4` (4 vCPU, 16 GB RAM) |
| Disk | 80–100 GB `pd-standard` (HDD) or `pd-balanced` (SSD) |
| Container runtime | None (native Windows processes) |
| Orchestrator | None (PowerShell scripts: `start-all.ps1`, `stop-all.ps1`) |
| Process visibility | All services run hidden (no console) — must be monitored via OS process table, ETW, perf counters, or in-process agents |

**Vendor must support:**
- Windows Server 2022 (not just Linux)
- Native (non-containerized) deployments
- Auto-discovery of processes via Windows process enumeration / WMI / ETW
- Hidden processes (no console window) — agents must hook in via env vars, DLL injection, or OS APIs, not console attachment

---

## 6. Scale & Volume

| Metric | Baseline | Burst |
|---|---|---|
| Request rate (gateway ingress) | 500 RPS | 5,000 RPS (10x burst) |
| Kafka throughput | ~1,500 msgs/sec across 7 topics | 15,000 msgs/sec under burst |
| Active WebSocket connections | ~50 | ~500 |
| Postgres query rate | ~200 QPS | ~2,000 QPS |
| Trace volume estimate | ~50 spans/sec/service × 11 = ~550 spans/sec | ~5,500 spans/sec |
| Log volume estimate | ~5 MB/min per service × 11 = ~55 MB/min | ~550 MB/min |
| Metrics cardinality | Low (per-service, per-endpoint) | Tag explosion possible from `client_id`, `symbol` |

**Vendor must handle:**
- Trace ingestion at 5,000+ spans/sec without sampling required
- High-cardinality tags (instruments × clients = millions of unique tag combinations)
- Bursty traffic patterns (10x spikes)

---

## 7. Required Monitoring Capabilities

### 7.1 Application Performance Monitoring (APM)
- ☐ Distributed tracing across HTTP + Kafka
- ☐ Auto-instrumentation for FastAPI, Express, Go HTTP servers
- ☐ Async/await trace propagation (Python asyncio, Node.js promises)
- ☐ WebSocket span correlation
- ☐ Kafka producer/consumer span linking (causal chain through queue)
- ☐ Database query instrumentation (asyncpg, kafka-go, redis-py)
- ☐ Error/exception tracking with stack traces
- ☐ Slow trace analysis (>p99 outliers)

### 7.2 Infrastructure Monitoring
- ☐ Windows host metrics (CPU, RAM, disk I/O, network)
- ☐ Per-process resource usage (CPU, RAM, file handles, threads)
- ☐ Kafka broker JMX metrics + topic/partition lag
- ☐ Postgres pg_stat_statements + replication lag
- ☐ Redis INFO metrics
- ☐ Elasticsearch cluster health, shard stats, JVM heap
- ☐ IIS performance counters (Active Server Pages, requests/sec, queue length)

### 7.3 Logs
- ☐ Centralized log collection from `C:\trading-platform\logs\*.out.log` and `*.err.log`
- ☐ IIS access log parsing (W3C format)
- ☐ Windows Event Log integration
- ☐ Log-to-trace correlation (trace ID injection in logs)
- ☐ Structured (JSON) log parsing

### 7.4 Alerting & SLO
- ☐ Multi-condition alert rules (e.g., latency × error rate)
- ☐ SLO/error budget tracking
- ☐ Anomaly detection (vs. static thresholds)
- ☐ Alert routing (PagerDuty, Slack, email, webhook)
- ☐ Maintenance windows / silencing

### 7.5 Real User Monitoring (RUM)
- ☐ Browser-side React performance
- ☐ Core Web Vitals (LCP, FID, CLS)
- ☐ Frontend error tracking
- ☐ Trace correlation: browser → IIS → gateway → backend

### 7.6 Synthetic Monitoring
- ☐ HTTP endpoint checks (gateway `/health`, UI loads)
- ☐ Multi-step API journey tests (login → submit order → verify fill)
- ☐ Geographic distribution of synthetic checks

---

## 8. Vendor-Specific Compatibility Checklist

For each candidate vendor, verify:

### 8.1 Agent Availability
| Requirement | Yes/No |
|---|---|
| Windows Server 2022 machine agent | ☐ |
| Python 3.11 agent (with FastAPI + asyncio + aiokafka support) | ☐ |
| Node.js 20 agent (with Express + ws auto-instrumentation) | ☐ |
| Go 1.22 instrumentation library (auto or manual SDK) | ☐ |
| Java agent for Kafka/Zookeeper/Elasticsearch (JMX export) | ☐ |
| Browser RUM SDK | ☐ |
| Native OTLP receiver (or OTel Collector available) | ☐ |

### 8.2 Deployment & Operations
| Requirement | Yes/No |
|---|---|
| Agent install via MSI / Chocolatey | ☐ |
| Agent works with hidden Windows processes | ☐ |
| Configuration via env vars (not just GUI) | ☐ |
| No mandatory cloud-only ingest (supports on-prem proxy) | ☐ |
| Agent overhead < 5% CPU per instrumented process | ☐ |
| Agent does not require service restart for config changes | ☐ |

### 8.3 Cost Model
| Question | Answer |
|---|---|
| Pricing basis (host-based, per-GB ingest, per-span, hybrid)? | |
| Cost for 1 host, 11 instrumented processes? | |
| Cost at 5,000 spans/sec sustained? | |
| Cost for 550 MB/min log ingest? | |
| Cost for browser RUM (sessions/month)? | |
| Free tier or trial available? | |
| Volume discount tiers? | |

### 8.4 Data Retention & Access
| Question | Answer |
|---|---|
| Trace retention period (default / max)? | |
| Log retention period? | |
| Metric retention period (raw / rollup)? | |
| Query API available (REST / GraphQL)? | |
| Data export options? | |
| Data residency (regions available)? | |

---

## 9. Integration Constraints

- **Cannot use Docker.** All instrumentation must work with native Windows processes.
- **No Kubernetes.** Auto-discovery via K8s annotations is irrelevant; need WMI / process-table / env-var based discovery.
- **OTel-first preferred.** The platform reserves ports 4317/4318 for OTLP. Vendors that natively accept OTLP avoid translation overhead and lock-in.
- **Hidden processes.** Services run via `Start-Process -WindowStyle Hidden`. Agents that require attaching to a console (rare but exists) won't work.
- **Air-gap-friendly preferred.** A future deployment may run in a restricted network — a vendor with on-prem collector / local proxy support is a plus.

---

## 10. Reference Architecture for Vendor

```
┌─────────────────────────────────────────────────────────────────┐
│  GCE VM: legacy-trade-host (Windows Server 2022, 4 vCPU/16 GB)  │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  IIS :443/:80                                            │   │
│   │  - Static UI (React)                                     │   │
│   │  - Reverse proxy /api/* /ws  ──> :3000                   │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│   ┌──────────────────────────▼──────────────────────────────┐   │
│   │  Node Gateway :3000                                       │   │
│   └──┬──────┬──────┬──────┬─────────────────────────────────┘   │
│      │      │      │      │                                      │
│      ▼      ▼      ▼      ▼                                      │
│   :8001  :8002  :8003  :8004    (Order, Quote, Analytics, Risk) │
│                                                                  │
│   Python consumers (no port): event-processor, legacy-adapter,  │
│                                batch-reconciler, market-data-sim,│
│                                traffic-generator                 │
│                                                                  │
│   Infra: Kafka :9092, Zookeeper :2181, Postgres :5432/:5433,    │
│          Redis :6379, Elasticsearch :9200                        │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  OTel Collector :4317 (gRPC), :4318 (UDP), :4320         │   │
│   │  ──> exports to vendor backend                           │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ outbound HTTPS
                              ▼
                    Vendor SaaS / on-prem backend
```

---

## 11. Decision Criteria Summary

A monitoring vendor is a **good fit** if it:

1. ✅ Has native OTLP support (no proprietary-only bridge)
2. ✅ Provides Windows Server 2022 agents
3. ✅ Auto-instruments Python (FastAPI + asyncio + aiokafka), Node.js (Express + ws), and Go HTTP libraries
4. ✅ Supports Kafka consumer lag and broker JMX out of the box
5. ✅ Handles 5,000 spans/sec burst without forcing aggressive sampling
6. ✅ Provides a single pane of glass for traces + metrics + logs
7. ✅ Has predictable, non-cardinality-explosion-driven pricing

A vendor is a **poor fit** if it:

1. ❌ Requires Kubernetes for auto-discovery
2. ❌ Lacks a Windows agent
3. ❌ Has no Go or weak Python async instrumentation
4. ❌ Requires container runtime
5. ❌ Charges per high-cardinality metric (would explode given symbols × clients)
6. ❌ Cloud-only with no on-prem collector option

---

## 12. Open Questions for Vendor

1. What is the agent's behavior under 100% CPU host saturation (can it itself become a contention source)?
2. How does the agent handle Python `asyncio.create_task()` and FastAPI background tasks?
3. Is there built-in support for `aiokafka` Producer/Consumer instrumentation, or does it require manual integration?
4. Does the Go SDK support automatic context propagation through the `kafka-go` library?
5. What is the maximum number of unique trace tags (cardinality) before billing/performance impact?
6. Can the vendor's IIS instrumentation correlate IIS request IDs with downstream trace IDs?
7. What happens to in-flight traces if the agent disconnects from the backend (buffer behavior, drop policy)?
