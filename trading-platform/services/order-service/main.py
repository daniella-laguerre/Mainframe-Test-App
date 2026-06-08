import asyncio
import json
import logging
import os
import random
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional

import asyncpg
import httpx
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/orders")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RISK_ENGINE_URL = os.getenv("RISK_ENGINE_URL", "http://localhost:8002")
QUOTE_SERVICE_URL = os.getenv("QUOTE_SERVICE_URL", "http://localhost:8003")

# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for attr in ("trace_id", "span_id", "order_id", "client_id", "symbol", "latency_ms"):
            if hasattr(record, attr):
                log_record[attr] = getattr(record, attr)
        if record.exc_info and record.exc_info[0]:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger = logging.getLogger("order-service")
logger.handlers = [handler]
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Enums & models
# ---------------------------------------------------------------------------


class Side(str, Enum):
    buy = "buy"
    sell = "sell"


class OrderType(str, Enum):
    market = "market"
    limit = "limit"
    stop = "stop"
    stop_limit = "stop_limit"
    ioc = "ioc"
    fok = "fok"


class TimeInForce(str, Enum):
    day = "day"
    gtc = "gtc"
    ioc = "ioc"
    fok = "fok"
    gtd = "gtd"


class OrderStatus(str, Enum):
    pending = "pending"
    accepted = "accepted"
    partially_filled = "partially_filled"
    filled = "filled"
    cancelled = "cancelled"
    rejected = "rejected"
    expired = "expired"


class OrderRequest(BaseModel):
    client_id: str
    symbol: str
    side: Side
    order_type: OrderType
    quantity: float = Field(gt=0)
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: TimeInForce = TimeInForce.day
    venue: Optional[str] = None
    algo_strategy: Optional[str] = None
    parent_order_id: Optional[str] = None


class OrderResponse(BaseModel):
    order_id: str
    client_id: str
    symbol: str
    side: Side
    order_type: OrderType
    quantity: float
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: TimeInForce
    venue: Optional[str] = None
    algo_strategy: Optional[str] = None
    parent_order_id: Optional[str] = None
    status: OrderStatus
    created_at: str
    filled_quantity: float = 0.0
    avg_fill_price: Optional[float] = None
    cl_ord_id: Optional[str] = None


class TradeResponse(BaseModel):
    trade_id: str
    order_id: str
    price: float
    quantity: float
    venue: str
    liquidity_flag: str
    slippage_bps: float
    fees: float
    timestamp: str
    exec_id: Optional[str] = None


class AmendRequest(BaseModel):
    limit_price: Optional[float] = None
    quantity: Optional[float] = None


class AuditEntry(BaseModel):
    id: int
    order_id: str
    action: str
    detail: str
    timestamp: str


# ---------------------------------------------------------------------------
# Metrics (in-memory counters)
# ---------------------------------------------------------------------------

metrics = {
    "orders_submitted": 0,
    "orders_filled": 0,
    "orders_rejected": 0,
    "total_fill_time_ms": 0.0,
    "fill_count": 0,
}

# ---------------------------------------------------------------------------
# Global resources
# ---------------------------------------------------------------------------

db_pool: Optional[asyncpg.Pool] = None
kafka_producer: Optional[AIOKafkaProducer] = None
redis_client: Optional[aioredis.Redis] = None
http_client: Optional[httpx.AsyncClient] = None


async def init_db(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id       TEXT PRIMARY KEY,
                client_id      TEXT NOT NULL,
                symbol         TEXT NOT NULL,
                side           TEXT NOT NULL,
                order_type     TEXT NOT NULL,
                quantity       DOUBLE PRECISION NOT NULL,
                limit_price    DOUBLE PRECISION,
                stop_price     DOUBLE PRECISION,
                time_in_force  TEXT NOT NULL,
                venue          TEXT,
                algo_strategy  TEXT,
                parent_order_id TEXT,
                status         TEXT NOT NULL DEFAULT 'pending',
                filled_quantity DOUBLE PRECISION NOT NULL DEFAULT 0,
                avg_fill_price DOUBLE PRECISION,
                cl_ord_id      TEXT,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id       TEXT PRIMARY KEY,
                order_id       TEXT NOT NULL REFERENCES orders(order_id),
                price          DOUBLE PRECISION NOT NULL,
                quantity       DOUBLE PRECISION NOT NULL,
                venue          TEXT NOT NULL,
                liquidity_flag TEXT NOT NULL,
                slippage_bps   DOUBLE PRECISION NOT NULL DEFAULT 0,
                fees           DOUBLE PRECISION NOT NULL DEFAULT 0,
                exec_id        TEXT,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_trail (
                id         SERIAL PRIMARY KEY,
                order_id   TEXT NOT NULL,
                action     TEXT NOT NULL,
                detail     TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_client ON orders(client_id);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_order ON trades(order_id);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_order ON audit_trail(order_id);")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, kafka_producer, redis_client, http_client

    # --- Postgres ---
    try:
        db_pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=5, max_size=20)
        await init_db(db_pool)
        logger.info("Postgres connection pool initialised")
    except Exception as exc:
        logger.warning(f"Postgres unavailable, running in degraded mode: {exc}")
        db_pool = None

    # --- Kafka ---
    try:
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
        )
        await kafka_producer.start()
        logger.info("Kafka producer started")
    except Exception as exc:
        logger.warning(f"Kafka unavailable, running in degraded mode: {exc}")
        kafka_producer = None

    # --- Redis ---
    try:
        redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        logger.info("Redis connected")
    except Exception as exc:
        logger.warning(f"Redis unavailable, running in degraded mode: {exc}")
        redis_client = None

    # --- HTTP ---
    http_client = httpx.AsyncClient(timeout=5.0)

    yield

    # Teardown
    if kafka_producer:
        await kafka_producer.stop()
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()
    if http_client:
        await http_client.aclose()


app = FastAPI(title="Order Service", version="1.0.0", lifespan=lifespan)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VENUES = ["NYSE", "NASDAQ", "dark_pool", "internal_crossing", "CME"]
VENUE_WEIGHTS = [0.30, 0.30, 0.15, 0.15, 0.10]
LIQUIDITY_FLAGS = ["maker", "taker", "auction"]
LIQUIDITY_WEIGHTS = [0.40, 0.55, 0.05]
REJECTION_REASONS = ["insufficient margin", "risk limit exceeded", "invalid symbol", "market closed"]


def gen_cl_ord_id() -> str:
    return f"CL-{uuid.uuid4().hex[:12].upper()}"


def gen_exec_id() -> str:
    return f"EX-{uuid.uuid4().hex[:12].upper()}"


def fix_log(msg_type: str, cl_ord_id: str, extra: dict, trace_id: str):
    """Log a FIX-style message."""
    parts = [
        "8=FIX.4.4",
        f"35={msg_type}",
        "49=CLIENT",
        "56=EXCHANGE",
        f"11={cl_ord_id}",
    ]
    for k, v in extra.items():
        parts.append(f"{k}={v}")
    fix_msg = "|".join(parts) + "|"
    logger.info(fix_msg, extra={"trace_id": trace_id, "span_id": uuid.uuid4().hex[:16]})


async def kafka_send(topic: str, value: dict):
    if kafka_producer:
        try:
            await kafka_producer.send_and_wait(topic, value)
        except Exception as exc:
            logger.error(f"Kafka producer error on topic {topic}: {exc}")
    else:
        logger.debug(f"Kafka not available, would publish to {topic}: {value.get('order_id', '')}")


async def record_audit(order_id: str, action: str, detail: str):
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO audit_trail (order_id, action, detail) VALUES ($1, $2, $3)",
                    order_id, action, detail,
                )
        except Exception as exc:
            logger.error(f"Audit trail write failed: {exc}")


async def update_order_status(order_id: str, status: str, filled_qty: float = None, avg_price: float = None):
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                if filled_qty is not None and avg_price is not None:
                    await conn.execute(
                        "UPDATE orders SET status=$1, filled_quantity=$2, avg_fill_price=$3, updated_at=now() WHERE order_id=$4",
                        status, filled_qty, avg_price, order_id,
                    )
                else:
                    await conn.execute(
                        "UPDATE orders SET status=$1, updated_at=now() WHERE order_id=$2",
                        status, order_id,
                    )
        except Exception as exc:
            logger.error(f"Order status update failed: {exc}")


async def fetch_quote(symbol: str) -> float:
    """Fetch current price from quote service, fall back to simulated price."""
    try:
        resp = await http_client.get(f"{QUOTE_SERVICE_URL}/quotes/{symbol}")
        if resp.status_code == 200:
            return resp.json().get("price", 0)
    except Exception:
        pass
    # Simulated fallback price
    base_prices = {"AAPL": 185.50, "MSFT": 420.10, "GOOGL": 175.30, "AMZN": 195.80, "TSLA": 172.40}
    base = base_prices.get(symbol, 100.0 + random.random() * 200)
    return round(base * (1 + random.uniform(-0.002, 0.002)), 2)


async def check_risk(order: dict, trace_id: str) -> tuple[bool, str]:
    """Call risk engine. Returns (approved, reason)."""
    # Simulate 2% rejection
    if random.random() < 0.02:
        reason = random.choice(REJECTION_REASONS)
        logger.warning(
            f"Risk check rejected order: {reason}",
            extra={"trace_id": trace_id, "span_id": uuid.uuid4().hex[:16],
                    "order_id": order["order_id"], "client_id": order["client_id"], "symbol": order["symbol"]},
        )
        return False, reason

    try:
        resp = await http_client.post(f"{RISK_ENGINE_URL}/check", json=order)
        if resp.status_code == 200:
            body = resp.json()
            return body.get("approved", True), body.get("reason", "")
    except Exception:
        pass
    return True, ""


def pick_venue() -> str:
    return random.choices(VENUES, weights=VENUE_WEIGHTS, k=1)[0]


def pick_liquidity() -> str:
    return random.choices(LIQUIDITY_FLAGS, weights=LIQUIDITY_WEIGHTS, k=1)[0]


def calc_commission(quantity: float) -> float:
    per_share = random.uniform(0.001, 0.005)
    return round(quantity * per_share, 4)


def calc_slippage_bps(arrival_price: float, fill_price: float) -> float:
    if arrival_price == 0:
        return 0.0
    return round(abs(fill_price - arrival_price) / arrival_price * 10000, 2)


# ---------------------------------------------------------------------------
# Order execution (background task)
# ---------------------------------------------------------------------------

async def execute_order(order_id: str, order_data: dict, trace_id: str):
    """Simulate order execution with realistic latency and venue selection."""
    span_id = uuid.uuid4().hex[:16]
    start = time.monotonic()

    # Simulated 0.5% error
    if random.random() < 0.005:
        err = random.choice(["database timeout", "kafka producer error"])
        logger.error(
            f"Simulated infrastructure error during execution: {err}",
            extra={"trace_id": trace_id, "span_id": span_id, "order_id": order_id,
                    "client_id": order_data["client_id"], "symbol": order_data["symbol"]},
        )
        await update_order_status(order_id, OrderStatus.rejected.value)
        await record_audit(order_id, "error", f"Execution failed: {err}")
        await kafka_send("order-updates", {"order_id": order_id, "status": "rejected", "reason": err})
        metrics["orders_rejected"] += 1
        return

    # Random execution delay 10-500ms
    delay = random.uniform(0.01, 0.5)
    await asyncio.sleep(delay)

    symbol = order_data["symbol"]
    order_type = order_data["order_type"]
    side = order_data["side"]
    quantity = order_data["quantity"]
    limit_price = order_data.get("limit_price")
    algo_strategy = order_data.get("algo_strategy")

    arrival_price = await fetch_quote(symbol)

    await record_audit(order_id, "accepted", f"Order accepted, arrival_price={arrival_price}")
    await update_order_status(order_id, OrderStatus.accepted.value)
    await kafka_send("order-updates", {"order_id": order_id, "status": "accepted"})

    # FIX new order single log
    fix_log("D", order_data.get("cl_ord_id", ""), {
        "55": symbol, "54": "1" if side == "buy" else "2",
        "38": str(quantity), "40": "1" if order_type == "market" else "2",
    }, trace_id)

    # Algo strategy: split into child orders
    if algo_strategy and algo_strategy.lower() == "vwap":
        num_slices = random.randint(5, 10)
        slice_qty = quantity / num_slices
        logger.info(
            f"VWAP algo: splitting order into {num_slices} slices of {slice_qty:.2f}",
            extra={"trace_id": trace_id, "span_id": span_id, "order_id": order_id,
                    "client_id": order_data["client_id"], "symbol": symbol},
        )
        await record_audit(order_id, "algo_split", f"VWAP: {num_slices} slices of {slice_qty:.2f}")
        total_filled = 0.0
        weighted_price_sum = 0.0

        for i in range(num_slices):
            await asyncio.sleep(random.uniform(0.005, 0.05))
            slice_price = round(arrival_price * (1 + random.uniform(-0.001, 0.001)), 2)
            venue = pick_venue()
            liq = pick_liquidity()
            fees = calc_commission(slice_qty)
            slippage = calc_slippage_bps(arrival_price, slice_price)

            trade_id = str(uuid.uuid4())
            exec_id = gen_exec_id()
            total_filled += slice_qty
            weighted_price_sum += slice_price * slice_qty

            trade_payload = {
                "trade_id": trade_id, "order_id": order_id,
                "price": slice_price, "quantity": slice_qty,
                "venue": venue, "liquidity_flag": liq,
                "slippage_bps": slippage, "fees": fees, "exec_id": exec_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            if db_pool:
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO trades (trade_id, order_id, price, quantity, venue, liquidity_flag, slippage_bps, fees, exec_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                            trade_id, order_id, slice_price, slice_qty, venue, liq, slippage, fees, exec_id,
                        )
                except Exception as exc:
                    logger.error(f"Trade insert failed: {exc}")

            await kafka_send("trades", trade_payload)
            fix_log("8", order_data.get("cl_ord_id", ""), {
                "17": exec_id, "31": str(slice_price), "32": str(slice_qty),
                "150": "F", "39": "1",
            }, trace_id)

        avg_price = round(weighted_price_sum / total_filled, 4) if total_filled else 0
        await update_order_status(order_id, OrderStatus.filled.value, total_filled, avg_price)
        await record_audit(order_id, "filled", f"VWAP complete: filled={total_filled}, avg_price={avg_price}")
        await kafka_send("order-updates", {"order_id": order_id, "status": "filled", "filled_quantity": total_filled, "avg_fill_price": avg_price})
        metrics["orders_filled"] += 1

    elif algo_strategy and algo_strategy.lower() == "iceberg":
        display_qty = quantity * random.uniform(0.1, 0.3)
        logger.info(
            f"Iceberg algo: display_qty={display_qty:.2f} of total {quantity}",
            extra={"trace_id": trace_id, "span_id": span_id, "order_id": order_id,
                    "client_id": order_data["client_id"], "symbol": symbol},
        )
        await record_audit(order_id, "algo_split", f"Iceberg: display={display_qty:.2f}, total={quantity}")
        remaining = quantity
        weighted_price_sum = 0.0
        total_filled = 0.0

        while remaining > 0:
            fill_qty = min(display_qty, remaining)
            await asyncio.sleep(random.uniform(0.005, 0.05))
            fill_price = round(arrival_price * (1 + random.uniform(-0.001, 0.001)), 2)
            venue = pick_venue()
            liq = pick_liquidity()
            fees = calc_commission(fill_qty)
            slippage = calc_slippage_bps(arrival_price, fill_price)
            trade_id = str(uuid.uuid4())
            exec_id = gen_exec_id()
            total_filled += fill_qty
            weighted_price_sum += fill_price * fill_qty
            remaining -= fill_qty

            trade_payload = {
                "trade_id": trade_id, "order_id": order_id,
                "price": fill_price, "quantity": fill_qty,
                "venue": venue, "liquidity_flag": liq,
                "slippage_bps": slippage, "fees": fees, "exec_id": exec_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            if db_pool:
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO trades (trade_id, order_id, price, quantity, venue, liquidity_flag, slippage_bps, fees, exec_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                            trade_id, order_id, fill_price, fill_qty, venue, liq, slippage, fees, exec_id,
                        )
                except Exception as exc:
                    logger.error(f"Trade insert failed: {exc}")
            await kafka_send("trades", trade_payload)

        avg_price = round(weighted_price_sum / total_filled, 4) if total_filled else 0
        await update_order_status(order_id, OrderStatus.filled.value, total_filled, avg_price)
        await record_audit(order_id, "filled", f"Iceberg complete: filled={total_filled}, avg_price={avg_price}")
        await kafka_send("order-updates", {"order_id": order_id, "status": "filled", "filled_quantity": total_filled, "avg_fill_price": avg_price})
        metrics["orders_filled"] += 1

    else:
        # Standard single-fill execution
        should_fill = False
        fill_price = arrival_price

        if order_type == "market":
            should_fill = True
            fill_price = round(arrival_price * (1 + random.uniform(-0.0005, 0.0005)), 2)
        elif order_type == "limit":
            if side == "buy" and limit_price and limit_price >= arrival_price:
                should_fill = True
                fill_price = min(limit_price, arrival_price)
            elif side == "sell" and limit_price and limit_price <= arrival_price:
                should_fill = True
                fill_price = max(limit_price, arrival_price)
            else:
                should_fill = False
        elif order_type in ("stop", "stop_limit"):
            # Simulate stop trigger
            should_fill = random.random() < 0.6
            if should_fill:
                fill_price = round(arrival_price * (1 + random.uniform(-0.001, 0.001)), 2)
        elif order_type in ("ioc", "fok"):
            should_fill = True
            fill_price = round(arrival_price * (1 + random.uniform(-0.0005, 0.0005)), 2)

        if should_fill:
            venue = pick_venue()
            liq = pick_liquidity()
            fees = calc_commission(quantity)
            slippage = calc_slippage_bps(arrival_price, fill_price)
            trade_id = str(uuid.uuid4())
            exec_id = gen_exec_id()

            trade_payload = {
                "trade_id": trade_id, "order_id": order_id,
                "price": fill_price, "quantity": quantity,
                "venue": venue, "liquidity_flag": liq,
                "slippage_bps": slippage, "fees": fees, "exec_id": exec_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            if db_pool:
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO trades (trade_id, order_id, price, quantity, venue, liquidity_flag, slippage_bps, fees, exec_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                            trade_id, order_id, fill_price, quantity, venue, liq, slippage, fees, exec_id,
                        )
                except Exception as exc:
                    logger.error(f"Trade insert failed: {exc}")

            await update_order_status(order_id, OrderStatus.filled.value, quantity, fill_price)
            await record_audit(order_id, "filled", f"Filled {quantity}@{fill_price} on {venue} liq={liq} slippage={slippage}bps fees={fees}")
            await kafka_send("trades", trade_payload)
            await kafka_send("order-updates", {"order_id": order_id, "status": "filled", "filled_quantity": quantity, "avg_fill_price": fill_price})

            fix_log("8", order_data.get("cl_ord_id", ""), {
                "17": exec_id, "31": str(fill_price), "32": str(quantity),
                "150": "F", "39": "2",
            }, trace_id)

            metrics["orders_filled"] += 1
        else:
            # Limit order not filled - stays open
            logger.info(
                f"Order {order_id} not filled, limit_price={limit_price} arrival={arrival_price}",
                extra={"trace_id": trace_id, "span_id": span_id, "order_id": order_id,
                        "client_id": order_data["client_id"], "symbol": symbol},
            )
            await record_audit(order_id, "open", f"Limit not met: limit={limit_price}, market={arrival_price}")
            await kafka_send("order-updates", {"order_id": order_id, "status": "accepted"})

    elapsed_ms = round((time.monotonic() - start) * 1000, 2)
    metrics["total_fill_time_ms"] += elapsed_ms
    metrics["fill_count"] += 1

    if elapsed_ms > 200:
        logger.warning(
            f"Slow order execution: {elapsed_ms}ms",
            extra={"trace_id": trace_id, "span_id": span_id, "order_id": order_id,
                    "client_id": order_data["client_id"], "symbol": symbol, "latency_ms": elapsed_ms},
        )
    else:
        logger.info(
            f"Order execution completed in {elapsed_ms}ms",
            extra={"trace_id": trace_id, "span_id": span_id, "order_id": order_id,
                    "client_id": order_data["client_id"], "symbol": symbol, "latency_ms": elapsed_ms},
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    checks = {"status": "ok", "service": "order-service", "timestamp": datetime.now(timezone.utc).isoformat()}
    checks["postgres"] = "connected" if db_pool else "unavailable"
    checks["kafka"] = "connected" if kafka_producer else "unavailable"
    checks["redis"] = "connected" if redis_client else "unavailable"
    checks["metrics"] = {
        "orders_submitted": metrics["orders_submitted"],
        "orders_filled": metrics["orders_filled"],
        "orders_rejected": metrics["orders_rejected"],
        "avg_fill_time_ms": round(metrics["total_fill_time_ms"] / metrics["fill_count"], 2) if metrics["fill_count"] else 0,
    }
    return checks


@app.post("/orders", response_model=OrderResponse, status_code=201)
async def submit_order(
    req: OrderRequest,
    background_tasks: BackgroundTasks,
    x_trace_id: Optional[str] = Header(None),
):
    trace_id = x_trace_id or uuid.uuid4().hex
    span_id = uuid.uuid4().hex[:16]
    start = time.monotonic()

    order_id = str(uuid.uuid4())
    cl_ord_id = gen_cl_ord_id()
    now = datetime.now(timezone.utc).isoformat()

    order_data = {
        "order_id": order_id,
        "client_id": req.client_id,
        "symbol": req.symbol,
        "side": req.side.value,
        "order_type": req.order_type.value,
        "quantity": req.quantity,
        "limit_price": req.limit_price,
        "stop_price": req.stop_price,
        "time_in_force": req.time_in_force.value,
        "venue": req.venue,
        "algo_strategy": req.algo_strategy,
        "parent_order_id": req.parent_order_id,
        "cl_ord_id": cl_ord_id,
        "status": OrderStatus.pending.value,
        "created_at": now,
    }

    logger.info(
        f"New order received: {req.side.value} {req.quantity} {req.symbol} {req.order_type.value}",
        extra={"trace_id": trace_id, "span_id": span_id, "order_id": order_id,
                "client_id": req.client_id, "symbol": req.symbol},
    )

    # Risk check
    approved, reason = await check_risk(order_data, trace_id)
    if not approved:
        order_data["status"] = OrderStatus.rejected.value
        metrics["orders_rejected"] += 1
        # Still store in DB
        if db_pool:
            try:
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        """INSERT INTO orders (order_id, client_id, symbol, side, order_type, quantity,
                           limit_price, stop_price, time_in_force, venue, algo_strategy, parent_order_id,
                           status, cl_ord_id)
                           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""",
                        order_id, req.client_id, req.symbol, req.side.value, req.order_type.value,
                        req.quantity, req.limit_price, req.stop_price, req.time_in_force.value,
                        req.venue, req.algo_strategy, req.parent_order_id, OrderStatus.rejected.value, cl_ord_id,
                    )
            except Exception as exc:
                logger.error(f"DB insert failed for rejected order: {exc}")
        await record_audit(order_id, "rejected", f"Risk check failed: {reason}")
        await kafka_send("order-updates", {"order_id": order_id, "status": "rejected", "reason": reason})

        elapsed = round((time.monotonic() - start) * 1000, 2)
        logger.info(
            f"Order rejected in {elapsed}ms: {reason}",
            extra={"trace_id": trace_id, "span_id": span_id, "order_id": order_id,
                    "client_id": req.client_id, "symbol": req.symbol, "latency_ms": elapsed},
        )

        return OrderResponse(
            order_id=order_id, client_id=req.client_id, symbol=req.symbol,
            side=req.side, order_type=req.order_type, quantity=req.quantity,
            limit_price=req.limit_price, stop_price=req.stop_price,
            time_in_force=req.time_in_force, venue=req.venue,
            algo_strategy=req.algo_strategy, parent_order_id=req.parent_order_id,
            status=OrderStatus.rejected, created_at=now, cl_ord_id=cl_ord_id,
        )

    # Store in Postgres
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO orders (order_id, client_id, symbol, side, order_type, quantity,
                       limit_price, stop_price, time_in_force, venue, algo_strategy, parent_order_id,
                       status, cl_ord_id)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""",
                    order_id, req.client_id, req.symbol, req.side.value, req.order_type.value,
                    req.quantity, req.limit_price, req.stop_price, req.time_in_force.value,
                    req.venue, req.algo_strategy, req.parent_order_id, OrderStatus.pending.value, cl_ord_id,
                )
        except Exception as exc:
            logger.error(f"DB insert failed: {exc}", extra={"trace_id": trace_id, "order_id": order_id})
            # Simulated connection pool exhaustion warning
            if random.random() < 0.1:
                logger.warning("Connection pool nearing exhaustion: 18/20 connections in use",
                               extra={"trace_id": trace_id, "span_id": span_id})
            raise HTTPException(status_code=500, detail="Database error")

    # Cache in Redis
    if redis_client:
        try:
            await redis_client.setex(f"order:{order_id}", 3600, json.dumps(order_data, default=str))
        except Exception:
            pass

    # Publish to Kafka
    await kafka_send("orders", order_data)

    await record_audit(order_id, "submitted", f"Order submitted: {req.side.value} {req.quantity} {req.symbol}")

    metrics["orders_submitted"] += 1

    # Schedule background execution
    background_tasks.add_task(execute_order, order_id, order_data, trace_id)

    elapsed = round((time.monotonic() - start) * 1000, 2)
    logger.info(
        f"Order accepted in {elapsed}ms",
        extra={"trace_id": trace_id, "span_id": span_id, "order_id": order_id,
                "client_id": req.client_id, "symbol": req.symbol, "latency_ms": elapsed},
    )

    # Occasional slow query warning
    if random.random() < 0.05:
        logger.warning(
            f"Slow query detected: INSERT INTO orders took {random.randint(50, 300)}ms",
            extra={"trace_id": trace_id, "span_id": span_id, "order_id": order_id},
        )

    return OrderResponse(
        order_id=order_id, client_id=req.client_id, symbol=req.symbol,
        side=req.side, order_type=req.order_type, quantity=req.quantity,
        limit_price=req.limit_price, stop_price=req.stop_price,
        time_in_force=req.time_in_force, venue=req.venue,
        algo_strategy=req.algo_strategy, parent_order_id=req.parent_order_id,
        status=OrderStatus.pending, created_at=now, cl_ord_id=cl_ord_id,
    )


@app.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: str, x_trace_id: Optional[str] = Header(None)):
    trace_id = x_trace_id or uuid.uuid4().hex

    # Try Redis cache first
    if redis_client:
        try:
            cached = await redis_client.get(f"order:{order_id}")
            if cached:
                data = json.loads(cached)
                return OrderResponse(
                    order_id=data["order_id"], client_id=data["client_id"], symbol=data["symbol"],
                    side=data["side"], order_type=data["order_type"], quantity=data["quantity"],
                    limit_price=data.get("limit_price"), stop_price=data.get("stop_price"),
                    time_in_force=data["time_in_force"], venue=data.get("venue"),
                    algo_strategy=data.get("algo_strategy"), parent_order_id=data.get("parent_order_id"),
                    status=data["status"], created_at=data["created_at"],
                    cl_ord_id=data.get("cl_ord_id"),
                )
        except Exception:
            pass

    if not db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    start = time.monotonic()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE order_id = $1", order_id)
    elapsed = round((time.monotonic() - start) * 1000, 2)

    if elapsed > 50:
        logger.warning(f"Slow query: SELECT orders took {elapsed}ms",
                       extra={"trace_id": trace_id, "order_id": order_id, "latency_ms": elapsed})

    if not row:
        raise HTTPException(status_code=404, detail="Order not found")

    return OrderResponse(
        order_id=row["order_id"], client_id=row["client_id"], symbol=row["symbol"],
        side=row["side"], order_type=row["order_type"], quantity=row["quantity"],
        limit_price=row["limit_price"], stop_price=row["stop_price"],
        time_in_force=row["time_in_force"], venue=row["venue"],
        algo_strategy=row["algo_strategy"], parent_order_id=row["parent_order_id"],
        status=row["status"], created_at=row["created_at"].isoformat(),
        filled_quantity=row["filled_quantity"], avg_fill_price=row["avg_fill_price"],
        cl_ord_id=row["cl_ord_id"],
    )


@app.get("/orders", response_model=list[OrderResponse])
async def list_orders(
    client_id: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    side: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    x_trace_id: Optional[str] = Header(None),
):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    trace_id = x_trace_id or uuid.uuid4().hex
    start = time.monotonic()

    query = "SELECT * FROM orders WHERE 1=1"
    params = []
    idx = 1

    if client_id:
        query += f" AND client_id = ${idx}"
        params.append(client_id)
        idx += 1
    if symbol:
        query += f" AND symbol = ${idx}"
        params.append(symbol)
        idx += 1
    if status:
        query += f" AND status = ${idx}"
        params.append(status)
        idx += 1
    if side:
        query += f" AND side = ${idx}"
        params.append(side)
        idx += 1

    query += f" ORDER BY created_at DESC LIMIT ${idx} OFFSET ${idx+1}"
    params.extend([limit, offset])

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    elapsed = round((time.monotonic() - start) * 1000, 2)
    if elapsed > 100:
        logger.warning(f"Slow query: list orders took {elapsed}ms, returned {len(rows)} rows",
                       extra={"trace_id": trace_id, "latency_ms": elapsed})

    return [
        OrderResponse(
            order_id=r["order_id"], client_id=r["client_id"], symbol=r["symbol"],
            side=r["side"], order_type=r["order_type"], quantity=r["quantity"],
            limit_price=r["limit_price"], stop_price=r["stop_price"],
            time_in_force=r["time_in_force"], venue=r["venue"],
            algo_strategy=r["algo_strategy"], parent_order_id=r["parent_order_id"],
            status=r["status"], created_at=r["created_at"].isoformat(),
            filled_quantity=r["filled_quantity"], avg_fill_price=r["avg_fill_price"],
            cl_ord_id=r["cl_ord_id"],
        )
        for r in rows
    ]


@app.delete("/orders/{order_id}")
async def cancel_order(order_id: str, x_trace_id: Optional[str] = Header(None)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    trace_id = x_trace_id or uuid.uuid4().hex

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status, cl_ord_id FROM orders WHERE order_id = $1", order_id)
        if not row:
            raise HTTPException(status_code=404, detail="Order not found")
        if row["status"] in (OrderStatus.filled.value, OrderStatus.cancelled.value, OrderStatus.rejected.value):
            raise HTTPException(status_code=400, detail=f"Cannot cancel order in status: {row['status']}")

        await conn.execute("UPDATE orders SET status = $1, updated_at = now() WHERE order_id = $2",
                           OrderStatus.cancelled.value, order_id)

    await record_audit(order_id, "cancelled", "Order cancelled by client")
    await kafka_send("order-updates", {"order_id": order_id, "status": "cancelled"})

    # Invalidate cache
    if redis_client:
        try:
            await redis_client.delete(f"order:{order_id}")
        except Exception:
            pass

    fix_log("F", row["cl_ord_id"] or "", {"41": order_id}, trace_id)
    logger.info(f"Order cancelled", extra={"trace_id": trace_id, "order_id": order_id})

    return {"order_id": order_id, "status": "cancelled"}


@app.post("/orders/{order_id}/amend")
async def amend_order(order_id: str, req: AmendRequest, x_trace_id: Optional[str] = Header(None)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    trace_id = x_trace_id or uuid.uuid4().hex

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status, cl_ord_id FROM orders WHERE order_id = $1", order_id)
        if not row:
            raise HTTPException(status_code=404, detail="Order not found")
        if row["status"] in (OrderStatus.filled.value, OrderStatus.cancelled.value, OrderStatus.rejected.value):
            raise HTTPException(status_code=400, detail=f"Cannot amend order in status: {row['status']}")

        updates = []
        params = []
        idx = 1
        if req.limit_price is not None:
            updates.append(f"limit_price = ${idx}")
            params.append(req.limit_price)
            idx += 1
        if req.quantity is not None:
            updates.append(f"quantity = ${idx}")
            params.append(req.quantity)
            idx += 1

        if not updates:
            raise HTTPException(status_code=400, detail="No fields to amend")

        updates.append(f"updated_at = now()")
        query = f"UPDATE orders SET {', '.join(updates)} WHERE order_id = ${idx}"
        params.append(order_id)

        await conn.execute(query, *params)

    detail = f"Amended: price={req.limit_price}, qty={req.quantity}"
    await record_audit(order_id, "amended", detail)
    await kafka_send("order-updates", {"order_id": order_id, "status": "amended", "new_price": req.limit_price, "new_quantity": req.quantity})

    # Invalidate cache
    if redis_client:
        try:
            await redis_client.delete(f"order:{order_id}")
        except Exception:
            pass

    fix_log("G", row["cl_ord_id"] or "", {"41": order_id}, trace_id)
    logger.info(f"Order amended", extra={"trace_id": trace_id, "order_id": order_id})

    return {"order_id": order_id, "status": "amended", "new_price": req.limit_price, "new_quantity": req.quantity}


@app.get("/orders/{order_id}/audit", response_model=list[AuditEntry])
async def get_audit_trail(order_id: str, x_trace_id: Optional[str] = Header(None)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, order_id, action, detail, created_at FROM audit_trail WHERE order_id = $1 ORDER BY created_at ASC",
            order_id,
        )

    if not rows:
        raise HTTPException(status_code=404, detail="No audit trail found for this order")

    return [
        AuditEntry(id=r["id"], order_id=r["order_id"], action=r["action"],
                   detail=r["detail"] or "", timestamp=r["created_at"].isoformat())
        for r in rows
    ]


@app.get("/trades", response_model=list[TradeResponse])
async def list_trades(
    order_id: Optional[str] = Query(None),
    venue: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    x_trace_id: Optional[str] = Header(None),
):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    query = "SELECT * FROM trades WHERE 1=1"
    params = []
    idx = 1

    if order_id:
        query += f" AND order_id = ${idx}"
        params.append(order_id)
        idx += 1
    if venue:
        query += f" AND venue = ${idx}"
        params.append(venue)
        idx += 1

    query += f" ORDER BY created_at DESC LIMIT ${idx} OFFSET ${idx+1}"
    params.extend([limit, offset])

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [
        TradeResponse(
            trade_id=r["trade_id"], order_id=r["order_id"], price=r["price"],
            quantity=r["quantity"], venue=r["venue"], liquidity_flag=r["liquidity_flag"],
            slippage_bps=r["slippage_bps"], fees=r["fees"],
            timestamp=r["created_at"].isoformat(), exec_id=r["exec_id"],
        )
        for r in rows
    ]


@app.get("/trades/{trade_id}", response_model=TradeResponse)
async def get_trade(trade_id: str, x_trace_id: Optional[str] = Header(None)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database unavailable")

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM trades WHERE trade_id = $1", trade_id)

    if not row:
        raise HTTPException(status_code=404, detail="Trade not found")

    return TradeResponse(
        trade_id=row["trade_id"], order_id=row["order_id"], price=row["price"],
        quantity=row["quantity"], venue=row["venue"], liquidity_flag=row["liquidity_flag"],
        slippage_bps=row["slippage_bps"], fees=row["fees"],
        timestamp=row["created_at"].isoformat(), exec_id=row["exec_id"],
    )
