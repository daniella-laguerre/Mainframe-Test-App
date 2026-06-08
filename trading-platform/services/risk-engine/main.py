import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "risk-engine",
            "message": record.getMessage(),
        }
        if hasattr(record, "extra"):
            log_entry.update(record.extra)
        return json.dumps(log_entry)


handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger = logging.getLogger("risk-engine")
logger.handlers = [handler]
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/trading")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# ---------------------------------------------------------------------------
# Simulated in-memory state
# ---------------------------------------------------------------------------
SECTORS = ["Technology", "Finance", "Healthcare", "Energy", "Consumer", "Industrial"]
SYMBOLS_SECTOR = {
    "AAPL": "Technology", "MSFT": "Technology", "GOOGL": "Technology", "NVDA": "Technology",
    "JPM": "Finance", "GS": "Finance", "MS": "Finance",
    "JNJ": "Healthcare", "PFE": "Healthcare", "UNH": "Healthcare",
    "XOM": "Energy", "CVX": "Energy",
    "AMZN": "Consumer", "TSLA": "Consumer",
    "CAT": "Industrial", "GE": "Industrial",
}

# Per-client state
client_positions: dict[str, list[dict]] = {}
client_risk_limits: dict[str, float] = {}
client_margin: dict[str, dict] = {}
risk_snapshots: dict[str, dict] = {}
order_history: dict[str, list[dict]] = {}  # for spoofing detection

# Background task handles
_bg_tasks: list[asyncio.Task] = []

# Connection stubs (set during startup; work as None when infra unavailable)
_pg_pool = None
_kafka_producer = None
_redis = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ensure_client(client_id: str):
    if client_id not in client_positions:
        num_pos = np.random.randint(2, 6)
        symbols = np.random.choice(list(SYMBOLS_SECTOR.keys()), size=num_pos, replace=False)
        positions = []
        for sym in symbols:
            qty = int(np.random.randint(100, 5000))
            px = round(float(np.random.uniform(50, 500)), 2)
            positions.append({
                "symbol": str(sym),
                "quantity": qty,
                "avg_price": px,
                "current_price": round(px * float(np.random.uniform(0.95, 1.05)), 2),
                "sector": SYMBOLS_SECTOR[str(sym)],
            })
        client_positions[client_id] = positions
        total_exposure = sum(p["quantity"] * p["current_price"] for p in positions)
        client_risk_limits[client_id] = round(total_exposure * float(np.random.uniform(1.5, 3.0)), 2)
        client_margin[client_id] = {
            "total": round(total_exposure * 0.6, 2),
            "used": round(total_exposure * float(np.random.uniform(0.15, 0.35)), 2),
        }


def _calc_exposure(positions: list[dict]) -> float:
    return sum(p["quantity"] * p["current_price"] for p in positions)


def _calc_var(positions: list[dict], confidence: float = 0.95, days: int = 1, sims: int = 1000) -> float:
    """Historical simulation VaR (simplified with random returns)."""
    total_value = _calc_exposure(positions)
    if total_value == 0:
        return 0.0
    returns = np.random.normal(0, 0.02, sims) * np.sqrt(days)
    pnl = total_value * returns
    var = -float(np.percentile(pnl, (1 - confidence) * 100))
    return round(max(var, 0), 2)


def _calc_expected_shortfall(positions: list[dict], confidence: float = 0.95, sims: int = 1000) -> float:
    total_value = _calc_exposure(positions)
    if total_value == 0:
        return 0.0
    returns = np.random.normal(0, 0.02, sims)
    pnl = total_value * returns
    cutoff = np.percentile(pnl, (1 - confidence) * 100)
    es = -float(np.mean(pnl[pnl <= cutoff]))
    return round(max(es, 0), 2)


def _calc_greeks(position: dict) -> dict:
    """Simplified greeks for equity position (delta-one + synthetic options greeks)."""
    notional = position["quantity"] * position["current_price"]
    return {
        "delta": round(float(np.random.uniform(0.8, 1.0)) * notional / 100_000, 4),
        "gamma": round(float(np.random.uniform(0.001, 0.01)), 6),
        "vega": round(float(np.random.uniform(0.5, 5.0)), 4),
        "theta": round(float(np.random.uniform(-2.0, -0.1)), 4),
        "rho": round(float(np.random.uniform(0.01, 0.5)), 4),
    }


def _concentration_by_sector(positions: list[dict]) -> dict[str, float]:
    total = _calc_exposure(positions)
    if total == 0:
        return {}
    sector_exp: dict[str, float] = {}
    for p in positions:
        sec = p.get("sector", "Other")
        sector_exp[sec] = sector_exp.get(sec, 0) + p["quantity"] * p["current_price"]
    return {s: round(v / total * 100, 2) for s, v in sector_exp.items()}


def _detect_spoofing(client_id: str) -> Optional[str]:
    """Check for rapid cancel/replace pattern in last 30 seconds."""
    history = order_history.get(client_id, [])
    now = time.time()
    recent = [o for o in history if now - o["ts"] < 30]
    cancels = sum(1 for o in recent if o["action"] in ("cancel", "replace"))
    if cancels >= 5:
        return f"Spoofing detected: {cancels} cancel/replace in 30s"
    return None


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class RiskCheckRequest(BaseModel):
    client_id: str
    symbol: str
    side: str = Field(..., pattern="^(buy|sell)$")
    quantity: int = Field(..., gt=0)
    price: float = Field(..., gt=0)


class RiskCheckResponse(BaseModel):
    approved: bool
    reason: str
    margin_required: float
    current_exposure: float
    var_impact: float
    risk_score: int


class StressTestRequest(BaseModel):
    scenarios: Optional[list[str]] = None  # defaults to all


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------
async def _connect_infrastructure():
    global _pg_pool, _kafka_producer, _redis
    # Postgres
    try:
        import asyncpg
        _pg_pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=5)
        await _pg_pool.execute("""
            CREATE TABLE IF NOT EXISTS risk_snapshots (
                id SERIAL PRIMARY KEY,
                client_id TEXT NOT NULL,
                snapshot JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        logger.info("Connected to Postgres")
    except Exception as exc:
        logger.warning(f"Postgres unavailable, running in-memory: {exc}")

    # Kafka
    try:
        from aiokafka import AIOKafkaProducer
        _kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        await _kafka_producer.start()
        logger.info("Connected to Kafka")
    except Exception as exc:
        logger.warning(f"Kafka unavailable, skipping publish: {exc}")

    # Redis
    try:
        import redis.asyncio as aioredis
        _redis = aioredis.from_url(REDIS_URL)
        await _redis.ping()
        logger.info("Connected to Redis")
    except Exception as exc:
        logger.warning(f"Redis unavailable: {exc}")


async def _publish_kafka(topic: str, payload: dict):
    if _kafka_producer is None:
        return
    try:
        await _kafka_producer.send_and_wait(topic, json.dumps(payload).encode())
    except Exception as exc:
        logger.warning(f"Kafka publish failed: {exc}")


async def _store_snapshot(client_id: str, snapshot: dict):
    if _pg_pool is None:
        return
    try:
        await _pg_pool.execute(
            "INSERT INTO risk_snapshots (client_id, snapshot) VALUES ($1, $2)",
            client_id, json.dumps(snapshot),
        )
    except Exception as exc:
        logger.warning(f"Postgres insert failed: {exc}")


async def _var_recalc_loop():
    """Recalculate VaR every 5 seconds for all active clients."""
    while True:
        try:
            for cid, positions in list(client_positions.items()):
                # Drift prices slightly
                for p in positions:
                    p["current_price"] = round(p["current_price"] * float(np.random.uniform(0.998, 1.002)), 2)

                exposure = _calc_exposure(positions)
                var95 = _calc_var(positions, 0.95)
                var99 = _calc_var(positions, 0.99)
                es = _calc_expected_shortfall(positions)
                limit = client_risk_limits.get(cid, exposure * 2)

                snapshot = {
                    "client_id": cid,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "total_exposure": round(exposure, 2),
                    "var_95": var95,
                    "var_99": var99,
                    "expected_shortfall": es,
                    "risk_limit": limit,
                    "utilization_pct": round(exposure / limit * 100, 2) if limit else 0,
                }
                risk_snapshots[cid] = snapshot

                # Publish to Kafka
                await _publish_kafka("risk-updates", snapshot)
                # Persist
                await _store_snapshot(cid, snapshot)

                # Compliance checks
                if exposure > limit * 0.9:
                    alert = {
                        "alert_id": str(uuid.uuid4()),
                        "type": "margin_call" if exposure > limit else "exposure_warning",
                        "client_id": cid,
                        "exposure": round(exposure, 2),
                        "limit": limit,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    await _publish_kafka("compliance-alerts", alert)
                    if exposure > limit:
                        logger.warning(f"margin call triggered", extra={"extra": {"client_id": cid, "exposure": exposure, "limit": limit}})

                # VaR breach
                if var95 > limit * 0.1:
                    logger.warning(f"VaR breach for client {cid}", extra={"extra": {"var_95": var95, "limit_10pct": round(limit * 0.1, 2)}})

                # Spoofing detection
                spoof = _detect_spoofing(cid)
                if spoof:
                    alert = {
                        "alert_id": str(uuid.uuid4()),
                        "type": "spoofing",
                        "client_id": cid,
                        "detail": spoof,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    await _publish_kafka("compliance-alerts", alert)
                    logger.warning(f"Spoofing alert: {spoof}", extra={"extra": {"client_id": cid}})

                # Concentration warning
                conc = _concentration_by_sector(positions)
                for sec, pct in conc.items():
                    if pct > 30:
                        alert = {
                            "alert_id": str(uuid.uuid4()),
                            "type": "concentration_warning",
                            "client_id": cid,
                            "sector": sec,
                            "concentration_pct": pct,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                        await _publish_kafka("compliance-alerts", alert)

            logger.info("VaR recalculation complete", extra={"extra": {"clients": len(client_positions)}})
        except Exception as exc:
            logger.error(f"VaR recalc error: {exc}")
        await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await _connect_infrastructure()
    # Seed a few demo clients
    for cid in ["CLIENT_001", "CLIENT_002", "CLIENT_003", "CLIENT_004", "CLIENT_005"]:
        _ensure_client(cid)
    task = asyncio.create_task(_var_recalc_loop())
    _bg_tasks.append(task)
    logger.info("Risk engine started")
    yield
    for t in _bg_tasks:
        t.cancel()
    if _kafka_producer:
        await _kafka_producer.stop()
    if _pg_pool:
        await _pg_pool.close()
    logger.info("Risk engine stopped")


app = FastAPI(title="Risk Engine", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post("/risk/check", response_model=RiskCheckResponse)
async def risk_check(req: RiskCheckRequest):
    _ensure_client(req.client_id)
    positions = client_positions[req.client_id]
    current_exposure = _calc_exposure(positions)
    order_notional = req.quantity * req.price
    new_exposure = current_exposure + order_notional if req.side == "buy" else current_exposure

    limit = client_risk_limits[req.client_id]
    margin_info = client_margin[req.client_id]
    margin_required = round(order_notional * 0.2, 2)
    margin_available = margin_info["total"] - margin_info["used"]

    # VaR impact (simplified)
    var_before = _calc_var(positions)
    var_impact = round(var_before * order_notional / max(current_exposure, 1) * 0.3, 2)

    # Risk score 0-100
    utilization = new_exposure / limit if limit else 1.0
    risk_score = min(100, int(utilization * 80 + var_impact / max(limit, 1) * 20 * 100))

    # Track order for spoofing detection
    order_history.setdefault(req.client_id, []).append({
        "ts": time.time(), "action": "new", "symbol": req.symbol, "qty": req.quantity,
    })

    # Rejection checks
    reason = "approved"
    approved = True

    if new_exposure > limit:
        approved = False
        reason = f"Exposure {new_exposure:.2f} exceeds risk limit {limit:.2f}"
    elif margin_required > margin_available:
        approved = False
        reason = f"Insufficient margin: required {margin_required:.2f}, available {margin_available:.2f}"
    else:
        # Concentration check
        sector = SYMBOLS_SECTOR.get(req.symbol, "Other")
        conc = _concentration_by_sector(positions)
        sector_conc = conc.get(sector, 0)
        added_conc = order_notional / max(new_exposure, 1) * 100
        if sector_conc + added_conc > 30:
            approved = False
            reason = f"Concentration in {sector} would exceed 30% ({sector_conc + added_conc:.1f}%)"

    logger.info(
        f"Risk check: {req.client_id} {req.side} {req.quantity} {req.symbol} -> {'APPROVED' if approved else 'REJECTED'}",
        extra={"extra": {
            "client_id": req.client_id, "symbol": req.symbol, "approved": approved,
            "risk_score": risk_score, "exposure": round(new_exposure, 2),
        }},
    )

    return RiskCheckResponse(
        approved=approved,
        reason=reason,
        margin_required=margin_required,
        current_exposure=round(current_exposure, 2),
        var_impact=var_impact,
        risk_score=risk_score,
    )


@app.get("/risk/{client_id}")
async def client_risk_summary(client_id: str):
    _ensure_client(client_id)
    positions = client_positions[client_id]
    exposure = _calc_exposure(positions)
    margin_info = client_margin[client_id]

    positions_with_greeks = []
    for p in positions:
        pos = dict(p)
        pos["greeks"] = _calc_greeks(p)
        pos["notional"] = round(p["quantity"] * p["current_price"], 2)
        positions_with_greeks.append(pos)

    return {
        "client_id": client_id,
        "positions": positions_with_greeks,
        "total_exposure": round(exposure, 2),
        "risk_limit": client_risk_limits[client_id],
        "var_95": _calc_var(positions, 0.95),
        "var_99": _calc_var(positions, 0.99),
        "expected_shortfall": _calc_expected_shortfall(positions),
        "margin_used": margin_info["used"],
        "margin_available": round(margin_info["total"] - margin_info["used"], 2),
        "margin_total": margin_info["total"],
        "concentration_by_sector": _concentration_by_sector(positions),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/risk/portfolio")
async def portfolio_risk():
    all_positions = []
    for positions in client_positions.values():
        all_positions.extend(positions)

    total_exposure = _calc_exposure(all_positions)

    # Top concentrations across the whole book
    sector_exp: dict[str, float] = {}
    symbol_exp: dict[str, float] = {}
    for p in all_positions:
        sec = p.get("sector", "Other")
        sector_exp[sec] = sector_exp.get(sec, 0) + p["quantity"] * p["current_price"]
        symbol_exp[p["symbol"]] = symbol_exp.get(p["symbol"], 0) + p["quantity"] * p["current_price"]

    top_sectors = sorted(sector_exp.items(), key=lambda x: -x[1])[:5]
    top_symbols = sorted(symbol_exp.items(), key=lambda x: -x[1])[:5]

    # Counterparty risk (per client exposure)
    counterparty = {}
    for cid, positions in client_positions.items():
        counterparty[cid] = round(_calc_exposure(positions), 2)

    # Stress test preview
    stress = _run_stress_tests(all_positions)

    return {
        "total_exposure": round(total_exposure, 2),
        "total_var_95": _calc_var(all_positions, 0.95),
        "total_var_99": _calc_var(all_positions, 0.99),
        "total_expected_shortfall": _calc_expected_shortfall(all_positions),
        "num_clients": len(client_positions),
        "num_positions": len(all_positions),
        "top_concentrations": {
            "by_sector": [{"sector": s, "exposure": round(e, 2), "pct": round(e / total_exposure * 100, 2)} for s, e in top_sectors],
            "by_symbol": [{"symbol": s, "exposure": round(e, 2), "pct": round(e / total_exposure * 100, 2)} for s, e in top_symbols],
        },
        "counterparty_risk": counterparty,
        "stress_test_results": stress,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _run_stress_tests(positions: list[dict]) -> list[dict]:
    total = _calc_exposure(positions)
    scenarios = [
        {"name": "Market Crash -20%", "shock": -0.20},
        {"name": "Rate Hike +200bp", "shock": -0.05},
        {"name": "Volatility Spike +50%", "shock": -0.10},
    ]
    results = []
    for sc in scenarios:
        stressed_value = total * (1 + sc["shock"])
        pnl_impact = stressed_value - total
        results.append({
            "scenario": sc["name"],
            "current_value": round(total, 2),
            "stressed_value": round(stressed_value, 2),
            "pnl_impact": round(pnl_impact, 2),
            "pct_change": round(sc["shock"] * 100, 2),
        })
    return results


@app.post("/risk/stress-test")
async def stress_test(req: StressTestRequest):
    all_positions = []
    for positions in client_positions.values():
        all_positions.extend(positions)

    default_scenarios = ["Market Crash -20%", "Rate Hike +200bp", "Volatility Spike +50%"]
    requested = req.scenarios if req.scenarios else default_scenarios

    scenario_configs = {
        "Market Crash -20%": -0.20,
        "Rate Hike +200bp": -0.05,
        "Volatility Spike +50%": -0.10,
    }

    total = _calc_exposure(all_positions)
    results = []
    for name in requested:
        shock = scenario_configs.get(name, -0.10)
        stressed = total * (1 + shock)
        # Per-sector breakdown
        sector_impact = {}
        for p in all_positions:
            sec = p.get("sector", "Other")
            notional = p["quantity"] * p["current_price"]
            sector_impact[sec] = sector_impact.get(sec, 0) + notional * shock

        results.append({
            "scenario": name,
            "current_portfolio_value": round(total, 2),
            "stressed_portfolio_value": round(stressed, 2),
            "pnl_impact": round(stressed - total, 2),
            "pct_change": round(shock * 100, 2),
            "sector_impact": {s: round(v, 2) for s, v in sector_impact.items()},
            "var_post_stress": _calc_var(all_positions, 0.95) * (1 + abs(shock)),
        })

    logger.info("Stress test executed", extra={"extra": {"scenarios": requested, "portfolio_value": round(total, 2)}})
    return {"results": results, "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "risk-engine",
        "active_clients": len(client_positions),
        "postgres": _pg_pool is not None,
        "kafka": _kafka_producer is not None,
        "redis": _redis is not None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
