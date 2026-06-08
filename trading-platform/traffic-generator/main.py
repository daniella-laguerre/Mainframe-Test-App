import asyncio
import logging
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum

import httpx
import numpy as np

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:3000")
TARGET_RPS = int(os.getenv("TARGET_RPS", "500"))
BURST_MULTIPLIER = int(os.getenv("BURST_MULTIPLIER", "10"))
CHAOS_ENABLED = os.getenv("CHAOS_ENABLED", "true").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CLIENTS_INSTITUTIONAL = [f"INST-{i:03d}" for i in range(1, 9)]
CLIENTS_HFT = [f"HF-{i:03d}" for i in range(1, 4)]
CLIENTS_PROP = ["PROP-001", "PROP-002"]
CLIENTS_RETAIL = [f"RET-{i:03d}" for i in range(1, 4)]
CLIENTS_MM = ["MM-001", "MM-002"]
CLIENTS_ALGO = ["ALGO-001", "ALGO-002"]

ALL_CLIENTS = (
    CLIENTS_INSTITUTIONAL
    + CLIENTS_HFT
    + CLIENTS_PROP
    + CLIENTS_RETAIL
    + CLIENTS_MM
    + CLIENTS_ALGO
)

EQUITIES = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "JPM", "GS", "NVDA", "META", "SPY"]
FX = ["EUR/USD", "GBP/USD", "USD/JPY"]
CRYPTO = ["BTC-USD", "ETH-USD"]
FUTURES = ["ES", "NQ"]
ALL_SYMBOLS = EQUITIES + FX + CRYPTO + FUTURES

ORDER_TYPES = ["market", "limit", "stop", "ioc", "fok"]
ORDER_TYPE_WEIGHTS = [0.40, 0.35, 0.10, 0.10, 0.05]

ALGO_STRATEGIES = ["VWAP", "TWAP", "POV", "Iceberg", "DMA", "Sniper"]
ALGO_STRATEGY_WEIGHTS = [0.30, 0.25, 0.15, 0.15, 0.10, 0.05]

SIDES = ["buy", "sell"]

# Reference prices for realistic limit/stop values
REFERENCE_PRICES: dict[str, float] = {
    "AAPL": 195.0, "MSFT": 420.0, "GOOGL": 155.0, "AMZN": 185.0,
    "TSLA": 250.0, "JPM": 200.0, "GS": 450.0, "NVDA": 880.0,
    "META": 500.0, "SPY": 520.0,
    "EUR/USD": 1.085, "GBP/USD": 1.265, "USD/JPY": 151.5,
    "BTC-USD": 68000.0, "ETH-USD": 3500.0,
    "ES": 5250.0, "NQ": 18400.0,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("traffic-generator")


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class Metrics:
    orders: int = 0
    quotes: int = 0
    analytics: int = 0
    errors: int = 0
    cancels: int = 0
    chaos_events: int = 0
    latencies: list[float] = field(default_factory=list)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def record(self, category: str, latency: float, is_error: bool = False):
        async with self._lock:
            if is_error:
                self.errors += 1
            if category == "order":
                self.orders += 1
            elif category == "quote":
                self.quotes += 1
            elif category == "analytics":
                self.analytics += 1
            elif category == "cancel":
                self.cancels += 1
            self.latencies.append(latency)

    async def snapshot_and_reset(self) -> dict:
        async with self._lock:
            lats = self.latencies
            p99 = float(np.percentile(lats, 99)) if lats else 0.0
            total = self.orders + self.quotes + self.analytics + self.cancels
            snap = {
                "orders": self.orders,
                "quotes": self.quotes,
                "analytics": self.analytics,
                "cancels": self.cancels,
                "errors": self.errors,
                "chaos_events": self.chaos_events,
                "p99_ms": round(p99 * 1000, 1),
                "total": total,
            }
            self.orders = 0
            self.quotes = 0
            self.analytics = 0
            self.cancels = 0
            self.errors = 0
            self.latencies = []
            return snap


metrics = Metrics()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_type(client_id: str) -> str:
    prefix = client_id.split("-")[0]
    return {
        "INST": "institutional",
        "HF": "hft",
        "PROP": "prop",
        "RET": "retail",
        "MM": "market_maker",
        "ALGO": "algo",
    }.get(prefix, "retail")


def _quantity_for(ctype: str) -> int:
    if ctype == "retail":
        return random.randint(1, 100)
    if ctype == "market_maker":
        return random.randint(100, 10000)
    # institutional, hft, prop, algo
    return random.randint(1000, 50000)


def _price_for(symbol: str, order_type: str, side: str) -> float | None:
    if order_type == "market":
        return None
    ref = REFERENCE_PRICES.get(symbol, 100.0)
    spread = ref * 0.002  # 0.2% spread
    if order_type == "limit":
        offset = random.uniform(-spread, spread)
    elif order_type == "stop":
        offset = spread if side == "sell" else -spread
        offset *= random.uniform(0.5, 2.0)
    else:
        offset = random.uniform(-spread, spread)
    return round(ref + offset, 4)


def _build_order(client_id: str) -> dict:
    ctype = _client_type(client_id)
    symbol = random.choice(ALL_SYMBOLS)
    order_type = random.choices(ORDER_TYPES, ORDER_TYPE_WEIGHTS, k=1)[0]
    side = random.choice(SIDES)
    qty = _quantity_for(ctype)
    price = _price_for(symbol, order_type, side)

    order: dict = {
        "client_id": client_id,
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "quantity": qty,
        "order_id": str(uuid.uuid4()),
    }
    if price is not None:
        order["price"] = price

    # Institutional clients get algo strategies
    if ctype == "institutional":
        order["algo_strategy"] = random.choices(
            ALGO_STRATEGIES, ALGO_STRATEGY_WEIGHTS, k=1
        )[0]

    return order


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------


async def _send(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    category: str,
    json: dict | None = None,
):
    url = f"{GATEWAY_URL}{path}"
    t0 = time.monotonic()
    is_error = False
    try:
        if method == "GET":
            resp = await client.get(url)
        elif method == "POST":
            resp = await client.post(url, json=json)
        elif method == "DELETE":
            resp = await client.delete(url)
        else:
            resp = await client.request(method, url, json=json)
        if resp.status_code >= 400:
            is_error = True
    except Exception:
        is_error = True
    latency = time.monotonic() - t0
    await metrics.record(category, latency, is_error)


# ---------------------------------------------------------------------------
# Traffic generators
# ---------------------------------------------------------------------------


async def normal_trading(client: httpx.AsyncClient):
    """Submit a single order. 70% of traffic."""
    cid = random.choice(ALL_CLIENTS)
    ctype = _client_type(cid)
    order = _build_order(cid)
    await _send(client, "POST", "/api/v1/orders", "order", json=order)

    # HFT/Algo cancel-replace: 70% chance cancel within 500ms
    if ctype in ("hft", "algo") and random.random() < 0.70:
        await asyncio.sleep(random.uniform(0.05, 0.5))
        await _send(
            client, "DELETE", f"/api/v1/orders/{order['order_id']}", "cancel"
        )

    # Market makers send both sides
    if ctype == "market_maker":
        opposite = _build_order(cid)
        opposite["side"] = "sell" if order["side"] == "buy" else "buy"
        opposite["symbol"] = order["symbol"]
        await _send(client, "POST", "/api/v1/orders", "order", json=opposite)


async def market_data_query(client: httpx.AsyncClient):
    """Quote or book query. 20% of traffic."""
    symbol = random.choice(ALL_SYMBOLS)
    if random.random() < 0.75:
        await _send(client, "GET", f"/api/v1/quotes/{symbol}", "quote")
    else:
        await _send(client, "GET", f"/api/v1/quotes/book/{symbol}", "quote")


async def analytics_query(client: httpx.AsyncClient):
    """Analytics / risk queries. 10% of traffic."""
    choice = random.random()
    if choice < 0.30:
        await _send(client, "GET", "/api/v1/analytics/kpis", "analytics")
    elif choice < 0.55:
        await _send(client, "GET", "/api/v1/analytics/pnl", "analytics")
    elif choice < 0.80:
        cid = random.choice(ALL_CLIENTS)
        await _send(client, "GET", f"/api/v1/risk/{cid}", "analytics")
    else:
        await _send(client, "GET", "/api/v1/risk/portfolio", "analytics")


# ---------------------------------------------------------------------------
# Chaos injection
# ---------------------------------------------------------------------------


async def chaos_burst(client: httpx.AsyncClient):
    """100 orders for the same symbol in ~1 second."""
    symbol = random.choice(ALL_SYMBOLS)
    log.warning("CHAOS: symbol burst -- 100 orders for %s in 1s", symbol)
    async with metrics._lock:
        metrics.chaos_events += 1
    tasks = []
    for _ in range(100):
        order = _build_order(random.choice(ALL_CLIENTS))
        order["symbol"] = symbol
        tasks.append(_send(client, "POST", "/api/v1/orders", "order", json=order))
    await asyncio.gather(*tasks)


async def chaos_invalid_orders(client: httpx.AsyncClient):
    """Submit a handful of invalid orders."""
    log.warning("CHAOS: invalid orders")
    async with metrics._lock:
        metrics.chaos_events += 1
    invalids = [
        {"client_id": "INST-001", "symbol": "INVALID_SYM", "side": "buy",
         "order_type": "market", "quantity": 100, "order_id": str(uuid.uuid4())},
        {"client_id": "RET-001", "side": "buy", "order_type": "market",
         "quantity": 10, "order_id": str(uuid.uuid4())},  # missing symbol
        {"client_id": "HF-001", "symbol": "AAPL", "side": "buy",
         "order_type": "limit", "quantity": -500, "order_id": str(uuid.uuid4()),
         "price": 195.0},  # negative qty
    ]
    for o in invalids:
        await _send(client, "POST", "/api/v1/orders", "order", json=o)


async def chaos_cancel_storm(client: httpx.AsyncClient):
    """50 rapid cancels."""
    log.warning("CHAOS: cancel storm -- 50 cancels in 2s")
    async with metrics._lock:
        metrics.chaos_events += 1
    tasks = []
    for _ in range(50):
        oid = str(uuid.uuid4())
        tasks.append(_send(client, "DELETE", f"/api/v1/orders/{oid}", "cancel"))
    await asyncio.gather(*tasks)


async def chaos_duplicate_orders(client: httpx.AsyncClient):
    """Submit the same order multiple times."""
    log.warning("CHAOS: duplicate order submission")
    async with metrics._lock:
        metrics.chaos_events += 1
    order = _build_order(random.choice(CLIENTS_INSTITUTIONAL))
    for _ in range(10):
        await _send(client, "POST", "/api/v1/orders", "order", json=order)


async def chaos_fat_finger(client: httpx.AsyncClient):
    """Order with absurdly large quantity."""
    log.warning("CHAOS: fat finger order")
    async with metrics._lock:
        metrics.chaos_events += 1
    order = _build_order(random.choice(ALL_CLIENTS))
    order["quantity"] = random.randint(1_000_000, 10_000_000)
    await _send(client, "POST", "/api/v1/orders", "order", json=order)


CHAOS_ACTIONS = [
    chaos_burst,
    chaos_invalid_orders,
    chaos_cancel_storm,
    chaos_duplicate_orders,
    chaos_fat_finger,
]


# ---------------------------------------------------------------------------
# Rate / pattern control
# ---------------------------------------------------------------------------


def current_rps(elapsed: float) -> float:
    """Return the effective RPS given elapsed seconds since start."""
    # Gradual ramp over first 30 seconds
    if elapsed < 30:
        rps = TARGET_RPS * (elapsed / 30.0)
    else:
        rps = float(TARGET_RPS)

    # Market open burst: 3x for first 60s (after ramp finishes at 30s)
    if 30 <= elapsed < 60:
        rps *= 3.0

    # Periodic quiet period: 0.3x for 30s every 300s
    cycle_300 = elapsed % 300
    if 0 <= cycle_300 < 30 and elapsed >= 300:
        rps *= 0.3

    # Periodic burst: BURST_MULTIPLIER x for 10s every 120s
    cycle_120 = elapsed % 120
    if 0 <= cycle_120 < 10 and elapsed >= 120:
        rps *= BURST_MULTIPLIER

    # Random micro-bursts are handled separately in the micro-burst task
    return max(rps, 1.0)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def traffic_loop(client: httpx.AsyncClient, start_time: float):
    """Continuously emit requests at the computed RPS."""
    while True:
        elapsed = time.monotonic() - start_time
        rps = current_rps(elapsed)

        # Pick traffic type
        roll = random.random()
        if roll < 0.70:
            coro = normal_trading(client)
        elif roll < 0.90:
            coro = market_data_query(client)
        else:
            coro = analytics_query(client)

        asyncio.create_task(coro)

        # Sleep to maintain target RPS (jitter to avoid lockstep)
        interval = 1.0 / rps
        jitter = random.uniform(0.8, 1.2)
        await asyncio.sleep(interval * jitter)


async def micro_burst_loop(client: httpx.AsyncClient, start_time: float):
    """Random micro-bursts: 5x for 2 seconds every 30-60 seconds."""
    while True:
        await asyncio.sleep(random.uniform(30, 60))
        elapsed = time.monotonic() - start_time
        burst_rps = current_rps(elapsed) * 5
        log.info("Micro-burst: %.0f req/s for 2s", burst_rps)
        end = time.monotonic() + 2.0
        while time.monotonic() < end:
            roll = random.random()
            if roll < 0.70:
                asyncio.create_task(normal_trading(client))
            elif roll < 0.90:
                asyncio.create_task(market_data_query(client))
            else:
                asyncio.create_task(analytics_query(client))
            await asyncio.sleep(1.0 / burst_rps)


async def chaos_loop(client: httpx.AsyncClient):
    """Periodically inject chaos events."""
    if not CHAOS_ENABLED:
        return
    while True:
        await asyncio.sleep(180)
        action = random.choice(CHAOS_ACTIONS)
        await action(client)


async def stats_reporter(start_time: float):
    """Print throughput stats every 10 seconds."""
    while True:
        await asyncio.sleep(10)
        snap = await metrics.snapshot_and_reset()
        elapsed = time.monotonic() - start_time
        rps = snap["total"] / 10.0
        log.info(
            "Traffic: %.0f req/s | Orders: %d | Quotes: %d | Analytics: %d | "
            "Cancels: %d | Errors: %d | P99 Latency: %.1fms | Chaos: %d | "
            "Uptime: %.0fs",
            rps,
            snap["orders"],
            snap["quotes"],
            snap["analytics"],
            snap["cancels"],
            snap["errors"],
            snap["p99_ms"],
            snap["chaos_events"],
            elapsed,
        )


async def main():
    log.info(
        "Starting traffic generator | gateway=%s target_rps=%d burst_mult=%d chaos=%s",
        GATEWAY_URL,
        TARGET_RPS,
        BURST_MULTIPLIER,
        CHAOS_ENABLED,
    )

    start_time = time.monotonic()

    limits = httpx.Limits(
        max_connections=200,
        max_keepalive_connections=100,
    )
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(10.0),
        limits=limits,
    ) as client:
        tasks = [
            asyncio.create_task(traffic_loop(client, start_time)),
            asyncio.create_task(micro_burst_loop(client, start_time)),
            asyncio.create_task(chaos_loop(client)),
            asyncio.create_task(stats_reporter(start_time)),
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
