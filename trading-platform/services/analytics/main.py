import asyncio
import json
import logging
import os
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "analytics",
            "message": record.getMessage(),
        }
        if hasattr(record, "extra"):
            log_entry.update(record.extra)
        return json.dumps(log_entry)


handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger = logging.getLogger("analytics")
logger.handlers = [handler]
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/trading")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
ES_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")

# ---------------------------------------------------------------------------
# In-memory aggregation state
# ---------------------------------------------------------------------------
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "NVDA", "JPM", "GS", "JNJ", "XOM", "AMZN", "TSLA"]
CLIENTS = [f"CLIENT_{str(i).zfill(3)}" for i in range(1, 11)]
DESKS = ["Equities", "Fixed Income", "Derivatives", "FX", "Commodities"]

# KPI accumulators
kpi_state = {
    "total_trades": 0,
    "total_volume": 0.0,
    "total_fills": 0,
    "total_orders": 0,
    "total_rejects": 0,
    "total_cancels": 0,
    "total_replaces": 0,
    "total_revenue": 0.0,
    "latency_samples": [],
    "slippage_samples": [],
    "start_time": time.time(),
}

# P&L trackers
pnl_by_client: dict[str, dict] = {}
pnl_by_symbol: dict[str, dict] = {}
pnl_by_desk: dict[str, dict] = {}

# Client analytics
client_analytics: dict[str, dict] = {}

# Execution quality
execution_state = {
    "fill_events": [],
    "slippage_distribution": [],
    "venue_fills": defaultdict(int),
    "algo_fills": defaultdict(lambda: {"fills": 0, "slippage_sum": 0.0}),
}

# Market microstructure
market_state: dict[str, dict] = {}

# VWAP trackers
vwap_accum: dict[str, dict] = defaultdict(lambda: {"volume": 0, "notional": 0.0})

# Connection handles
_pg_pool = None
_kafka_consumer = None
_redis = None
_es = None
_bg_tasks: list[asyncio.Task] = []


# ---------------------------------------------------------------------------
# Seed demo data
# ---------------------------------------------------------------------------
def _seed_demo_data():
    """Populate initial state with realistic demo data."""
    kpi_state["total_trades"] = int(np.random.randint(5000, 15000))
    kpi_state["total_volume"] = round(float(np.random.uniform(50_000_000, 200_000_000)), 2)
    kpi_state["total_fills"] = int(kpi_state["total_trades"] * 0.95)
    kpi_state["total_orders"] = int(kpi_state["total_trades"] * 1.3)
    kpi_state["total_rejects"] = int(kpi_state["total_orders"] * 0.02)
    kpi_state["total_cancels"] = int(kpi_state["total_orders"] * 0.15)
    kpi_state["total_replaces"] = int(kpi_state["total_orders"] * 0.08)
    kpi_state["total_revenue"] = round(kpi_state["total_volume"] * 0.0002, 2)
    kpi_state["latency_samples"] = [float(np.random.lognormal(1.5, 0.8)) for _ in range(500)]
    kpi_state["slippage_samples"] = [float(np.random.normal(0.5, 2.0)) for _ in range(500)]

    for cid in CLIENTS:
        realized = round(float(np.random.uniform(-50000, 150000)), 2)
        unrealized = round(float(np.random.uniform(-30000, 80000)), 2)
        pnl_by_client[cid] = {
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "total_pnl": round(realized + unrealized, 2),
            "pnl_attribution": {
                "price": round(float(np.random.uniform(-20000, 100000)), 2),
                "fx": round(float(np.random.uniform(-5000, 5000)), 2),
                "carry": round(float(np.random.uniform(0, 10000)), 2),
                "fees": round(float(np.random.uniform(-8000, -500)), 2),
            },
        }
        client_analytics[cid] = {
            "profitability": round(float(np.random.uniform(-0.05, 0.15)), 4),
            "order_flow_toxicity": round(float(np.random.uniform(0.1, 0.9)), 4),
            "latency_sensitivity": round(float(np.random.uniform(0.0, 1.0)), 4),
            "fill_quality": round(float(np.random.uniform(0.7, 0.99)), 4),
            "retention_score": round(float(np.random.uniform(0.5, 1.0)), 4),
            "total_trades": int(np.random.randint(100, 2000)),
            "total_volume": round(float(np.random.uniform(1_000_000, 50_000_000)), 2),
            "avg_order_size": int(np.random.randint(100, 5000)),
        }

    for sym in SYMBOLS:
        realized = round(float(np.random.uniform(-30000, 100000)), 2)
        unrealized = round(float(np.random.uniform(-20000, 60000)), 2)
        pnl_by_symbol[sym] = {
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "total_pnl": round(realized + unrealized, 2),
            "pnl_attribution": {
                "price": round(float(np.random.uniform(-15000, 80000)), 2),
                "fx": round(float(np.random.uniform(-2000, 2000)), 2),
                "carry": round(float(np.random.uniform(0, 5000)), 2),
                "fees": round(float(np.random.uniform(-5000, -200)), 2),
            },
        }
        market_state[sym] = {
            "bid": round(float(np.random.uniform(100, 400)), 2),
            "ask": 0,
            "spread_bps": round(float(np.random.uniform(1, 15)), 2),
            "volatility_1d": round(float(np.random.uniform(0.01, 0.05)), 4),
            "volatility_5d": round(float(np.random.uniform(0.02, 0.08)), 4),
            "volume_profile": {
                "morning": round(float(np.random.uniform(0.3, 0.5)), 2),
                "midday": round(float(np.random.uniform(0.15, 0.25)), 2),
                "afternoon": round(float(np.random.uniform(0.3, 0.5)), 2),
            },
            "order_book_pressure": round(float(np.random.uniform(-1, 1)), 4),
        }
        market_state[sym]["ask"] = round(market_state[sym]["bid"] * (1 + market_state[sym]["spread_bps"] / 10000), 2)

        vwap_accum[sym] = {
            "volume": int(np.random.randint(100000, 1000000)),
            "notional": round(float(np.random.uniform(10_000_000, 100_000_000)), 2),
        }

    for desk in DESKS:
        realized = round(float(np.random.uniform(-100000, 500000)), 2)
        unrealized = round(float(np.random.uniform(-50000, 200000)), 2)
        pnl_by_desk[desk] = {
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "total_pnl": round(realized + unrealized, 2),
            "pnl_attribution": {
                "price": round(float(np.random.uniform(-50000, 300000)), 2),
                "fx": round(float(np.random.uniform(-10000, 10000)), 2),
                "carry": round(float(np.random.uniform(0, 30000)), 2),
                "fees": round(float(np.random.uniform(-20000, -2000)), 2),
            },
        }

    venues = ["NYSE", "NASDAQ", "BATS", "IEX", "ARCA"]
    for v in venues:
        execution_state["venue_fills"][v] = int(np.random.randint(500, 5000))

    algos = ["TWAP", "VWAP", "POV", "IS", "Sniper"]
    for a in algos:
        execution_state["algo_fills"][a] = {
            "fills": int(np.random.randint(200, 3000)),
            "slippage_sum": round(float(np.random.uniform(-500, 500)), 2),
        }


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------
async def _connect_infrastructure():
    global _pg_pool, _redis, _es
    # Postgres
    try:
        import asyncpg
        _pg_pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=5)
        await _pg_pool.execute("""
            CREATE TABLE IF NOT EXISTS business_kpis (
                id SERIAL PRIMARY KEY,
                kpis JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        logger.info("Connected to Postgres")
    except Exception as exc:
        logger.warning(f"Postgres unavailable, running in-memory: {exc}")

    # Redis
    try:
        import redis.asyncio as aioredis
        _redis = aioredis.from_url(REDIS_URL)
        await _redis.ping()
        logger.info("Connected to Redis")
    except Exception as exc:
        logger.warning(f"Redis unavailable: {exc}")

    # Elasticsearch
    try:
        from elasticsearch import AsyncElasticsearch
        _es = AsyncElasticsearch(ES_URL)
        info = await _es.info()
        logger.info(f"Connected to Elasticsearch: {info['version']['number']}")
    except Exception as exc:
        logger.warning(f"Elasticsearch unavailable: {exc}")


async def _kafka_consumer_loop():
    """Consume from Kafka topics and aggregate metrics."""
    try:
        from aiokafka import AIOKafkaConsumer
        consumer = AIOKafkaConsumer(
            "trades", "order-updates", "risk-updates",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="analytics-service",
            auto_offset_reset="latest",
        )
        await consumer.start()
        logger.info("Kafka consumer started")
        try:
            async for msg in consumer:
                try:
                    payload = json.loads(msg.value.decode())
                    topic = msg.topic
                    if topic == "trades":
                        _process_trade(payload)
                    elif topic == "order-updates":
                        _process_order_update(payload)
                    elif topic == "risk-updates":
                        pass  # risk snapshots consumed for dashboarding
                except Exception as exc:
                    logger.error(f"Error processing Kafka message: {exc}")
        finally:
            await consumer.stop()
    except Exception as exc:
        logger.warning(f"Kafka consumer unavailable, using simulated data: {exc}")


def _process_trade(trade: dict):
    qty = trade.get("quantity", 0)
    price = trade.get("price", 0)
    notional = qty * price
    kpi_state["total_trades"] += 1
    kpi_state["total_fills"] += 1
    kpi_state["total_volume"] += notional
    kpi_state["total_revenue"] += notional * 0.0002

    slippage = trade.get("slippage_bps", float(np.random.normal(0.5, 2.0)))
    kpi_state["slippage_samples"].append(slippage)
    latency = trade.get("latency_ms", float(np.random.lognormal(1.5, 0.8)))
    kpi_state["latency_samples"].append(latency)

    # VWAP
    sym = trade.get("symbol", "UNKNOWN")
    vwap_accum[sym]["volume"] += qty
    vwap_accum[sym]["notional"] += notional

    # Index to ES
    asyncio.create_task(_index_trade(trade))


def _process_order_update(update: dict):
    kpi_state["total_orders"] += 1
    status = update.get("status", "")
    if status == "rejected":
        kpi_state["total_rejects"] += 1
    elif status == "cancelled":
        kpi_state["total_cancels"] += 1
    elif status == "replaced":
        kpi_state["total_replaces"] += 1


async def _index_trade(trade: dict):
    if _es is None:
        return
    try:
        await _es.index(index="trades", document=trade)
    except Exception as exc:
        logger.warning(f"ES indexing failed: {exc}")


async def _kpi_flush_loop():
    """Periodically flush KPIs to Postgres and drift simulated data."""
    while True:
        try:
            # Simulate incremental trading activity
            new_trades = int(np.random.randint(5, 30))
            new_volume = round(float(np.random.uniform(50000, 500000)), 2)
            kpi_state["total_trades"] += new_trades
            kpi_state["total_fills"] += int(new_trades * 0.95)
            kpi_state["total_orders"] += int(new_trades * 1.3)
            kpi_state["total_rejects"] += int(np.random.randint(0, 3))
            kpi_state["total_cancels"] += int(np.random.randint(0, 5))
            kpi_state["total_replaces"] += int(np.random.randint(0, 3))
            kpi_state["total_volume"] += new_volume
            kpi_state["total_revenue"] += round(new_volume * 0.0002, 2)

            # Add latency / slippage samples
            for _ in range(new_trades):
                kpi_state["latency_samples"].append(float(np.random.lognormal(1.5, 0.8)))
                kpi_state["slippage_samples"].append(float(np.random.normal(0.5, 2.0)))
            # Keep last 2000 samples
            kpi_state["latency_samples"] = kpi_state["latency_samples"][-2000:]
            kpi_state["slippage_samples"] = kpi_state["slippage_samples"][-2000:]

            # Drift P&L
            for tracker in [pnl_by_client, pnl_by_symbol, pnl_by_desk]:
                for key in tracker:
                    delta = round(float(np.random.normal(0, 2000)), 2)
                    tracker[key]["unrealized_pnl"] = round(tracker[key]["unrealized_pnl"] + delta, 2)
                    tracker[key]["total_pnl"] = round(tracker[key]["realized_pnl"] + tracker[key]["unrealized_pnl"], 2)

            # Drift market microstructure
            for sym in market_state:
                ms = market_state[sym]
                ms["bid"] = round(ms["bid"] * float(np.random.uniform(0.999, 1.001)), 2)
                ms["ask"] = round(ms["bid"] * (1 + ms["spread_bps"] / 10000), 2)
                ms["volatility_1d"] = round(abs(ms["volatility_1d"] + float(np.random.normal(0, 0.002))), 4)
                ms["order_book_pressure"] = round(float(np.random.uniform(-1, 1)), 4)

            # Flush to Postgres
            kpis_snapshot = _build_kpis()
            if _pg_pool:
                try:
                    await _pg_pool.execute(
                        "INSERT INTO business_kpis (kpis) VALUES ($1)",
                        json.dumps(kpis_snapshot),
                    )
                except Exception as exc:
                    logger.warning(f"Postgres KPI flush failed: {exc}")

            # Index to ES
            if _es:
                try:
                    await _es.index(index="kpi-snapshots", document=kpis_snapshot)
                    logger.info("ES indexing complete", extra={"extra": {"index": "kpi-snapshots"}})
                except Exception as exc:
                    logger.warning(f"ES KPI indexing failed: {exc}")

            elapsed = time.time() - kpi_state["start_time"]
            if elapsed > 60:
                logger.info(
                    f"KPI flush: {kpi_state['total_trades']} trades, ${kpi_state['total_volume']:,.0f} volume",
                    extra={"extra": {"aggregation_window_s": 10, "trades": kpi_state["total_trades"]}},
                )

            # Slow query warning simulation
            query_time = float(np.random.lognormal(2, 1))
            if query_time > 50:
                logger.warning(
                    f"Slow query detected: {query_time:.1f}ms",
                    extra={"extra": {"query_time_ms": round(query_time, 1), "query": "kpi_aggregation"}},
                )

        except Exception as exc:
            logger.error(f"KPI flush error: {exc}")
        await asyncio.sleep(10)


def _build_kpis() -> dict:
    elapsed_min = max((time.time() - kpi_state["start_time"]) / 60, 1)
    latencies = kpi_state["latency_samples"] or [0]
    slippages = kpi_state["slippage_samples"] or [0]
    total_orders = max(kpi_state["total_orders"], 1)

    return {
        "fill_rate": round(kpi_state["total_fills"] / total_orders * 100, 2),
        "avg_slippage_bps": round(float(np.mean(slippages)), 2),
        "reject_rate": round(kpi_state["total_rejects"] / total_orders * 100, 2),
        "cancel_replace_ratio": round(
            (kpi_state["total_cancels"] + kpi_state["total_replaces"]) / total_orders * 100, 2
        ),
        "total_volume": round(kpi_state["total_volume"], 2),
        "total_trades": kpi_state["total_trades"],
        "revenue_per_minute": round(kpi_state["total_revenue"] / elapsed_min, 2),
        "cost_per_trade": round(
            kpi_state["total_revenue"] * 0.3 / max(kpi_state["total_trades"], 1), 4
        ),
        "system_availability": 99.97,
        "avg_latency_ms": round(float(np.mean(latencies)), 2),
        "p99_latency_ms": round(float(np.percentile(latencies, 99)), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    _seed_demo_data()
    await _connect_infrastructure()
    _bg_tasks.append(asyncio.create_task(_kafka_consumer_loop()))
    _bg_tasks.append(asyncio.create_task(_kpi_flush_loop()))
    logger.info("Analytics service started")
    yield
    for t in _bg_tasks:
        t.cancel()
    if _pg_pool:
        await _pg_pool.close()
    if _es:
        await _es.close()
    logger.info("Analytics service stopped")


app = FastAPI(title="Analytics Service", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/analytics/kpis")
async def get_kpis():
    return _build_kpis()


@app.get("/analytics/pnl")
async def get_pnl():
    return {
        "by_client": pnl_by_client,
        "by_symbol": pnl_by_symbol,
        "by_desk": pnl_by_desk,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/analytics/client/{client_id}")
async def get_client_analytics(client_id: str):
    if client_id not in client_analytics:
        # Generate on the fly for unknown clients
        client_analytics[client_id] = {
            "profitability": round(float(np.random.uniform(-0.05, 0.15)), 4),
            "order_flow_toxicity": round(float(np.random.uniform(0.1, 0.9)), 4),
            "latency_sensitivity": round(float(np.random.uniform(0.0, 1.0)), 4),
            "fill_quality": round(float(np.random.uniform(0.7, 0.99)), 4),
            "retention_score": round(float(np.random.uniform(0.5, 1.0)), 4),
            "total_trades": int(np.random.randint(100, 2000)),
            "total_volume": round(float(np.random.uniform(1_000_000, 50_000_000)), 2),
            "avg_order_size": int(np.random.randint(100, 5000)),
        }

    analytics = client_analytics[client_id]
    pnl = pnl_by_client.get(client_id, {
        "realized_pnl": 0, "unrealized_pnl": 0, "total_pnl": 0,
        "pnl_attribution": {"price": 0, "fx": 0, "carry": 0, "fees": 0},
    })

    return {
        "client_id": client_id,
        **analytics,
        "pnl": pnl,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/analytics/execution")
async def get_execution_quality():
    slippages = kpi_state["slippage_samples"] or [0]
    total_orders = max(kpi_state["total_orders"], 1)

    # Venue performance
    total_venue_fills = sum(execution_state["venue_fills"].values()) or 1
    venue_perf = {
        v: {"fills": c, "pct": round(c / total_venue_fills * 100, 2)}
        for v, c in execution_state["venue_fills"].items()
    }

    # Algo performance
    algo_perf = {}
    for algo, data in execution_state["algo_fills"].items():
        fills = data["fills"] or 1
        algo_perf[algo] = {
            "fills": data["fills"],
            "avg_slippage_bps": round(data["slippage_sum"] / fills, 2),
        }

    return {
        "fill_rate": round(kpi_state["total_fills"] / total_orders * 100, 2),
        "slippage_distribution": {
            "mean_bps": round(float(np.mean(slippages)), 2),
            "median_bps": round(float(np.median(slippages)), 2),
            "std_bps": round(float(np.std(slippages)), 2),
            "p5_bps": round(float(np.percentile(slippages, 5)), 2),
            "p95_bps": round(float(np.percentile(slippages, 95)), 2),
        },
        "market_impact": {
            "avg_bps": round(float(np.mean(slippages)) * 0.6, 2),
            "temporary_bps": round(float(np.mean(slippages)) * 0.4, 2),
            "permanent_bps": round(float(np.mean(slippages)) * 0.2, 2),
        },
        "venue_performance": venue_perf,
        "algo_performance": algo_perf,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/analytics/market")
async def get_market_microstructure():
    result = {}
    for sym, ms in market_state.items():
        vol = vwap_accum[sym]["volume"] or 1
        notional = vwap_accum[sym]["notional"]
        result[sym] = {
            "spread_analysis": {
                "bid": ms["bid"],
                "ask": ms["ask"],
                "spread_bps": ms["spread_bps"],
                "mid": round((ms["bid"] + ms["ask"]) / 2, 2),
            },
            "volatility": {
                "intraday_1d": ms["volatility_1d"],
                "intraday_5d": ms["volatility_5d"],
            },
            "volume_profile": ms["volume_profile"],
            "order_book_pressure": ms["order_book_pressure"],
            "vwap": round(notional / vol, 2) if vol > 0 else 0,
            "total_volume": vol,
        }
    return {
        "symbols": result,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "analytics",
        "total_trades_tracked": kpi_state["total_trades"],
        "postgres": _pg_pool is not None,
        "kafka_consumer": any(not t.done() for t in _bg_tasks if "kafka" in str(t)),
        "elasticsearch": _es is not None,
        "redis": _redis is not None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
