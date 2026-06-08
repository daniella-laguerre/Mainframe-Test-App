import asyncio
import json
import logging
import os
import random
import time
import traceback
import uuid
from datetime import datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/trading")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
ELASTICSEARCH_URL = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")

CONSUMER_GROUP = "event-processor-group"
TOPICS = ["orders", "trades", "order-updates", "risk-updates", "market-data", "compliance-alerts", "legacy-mq"]

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("event-processor")

# In-memory state for simulation
_positions = {}
_minute_bars = {}
_last_rebalance = time.monotonic()
_message_counts = {}
_consumer_offsets = {}


def _trace_id():
    return uuid.uuid4().hex[:32]


def _span_id():
    return uuid.uuid4().hex[:16]


def _log_structured(topic, partition, offset, processing_time_ms, extra=None, level="info"):
    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "service": "event-processor",
        "trace_id": _trace_id(),
        "span_id": _span_id(),
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "processing_time_ms": processing_time_ms,
    }
    if extra:
        entry.update(extra)
    logger.info(json.dumps(entry))


async def _send_to_dlq(producer, topic, msg_key, msg_value, error):
    """Send failed message to dead letter queue."""
    dlq_msg = {
        "original_topic": topic,
        "original_key": msg_key.decode("utf-8") if msg_key else None,
        "error": str(error),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "original_value_b64": msg_value.hex() if msg_value else None,
    }
    try:
        await producer.send_and_wait(
            "dead-letter",
            value=json.dumps(dlq_msg).encode("utf-8"),
            key=msg_key,
        )
    except Exception as e:
        logger.error(json.dumps({
            "level": "error",
            "service": "event-processor",
            "message": f"Failed to send to DLQ: {e}",
            "original_topic": topic,
        }))


def _check_rebalance():
    """Simulate consumer group rebalancing ~every 60s."""
    global _last_rebalance
    now = time.monotonic()
    if now - _last_rebalance > random.uniform(50, 70):
        _last_rebalance = now
        partitions = random.sample(range(0, 12), random.randint(2, 6))
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": "warn",
            "service": "event-processor",
            "message": "partition rebalance triggered",
            "trace_id": _trace_id(),
            "assigned_partitions": partitions,
            "consumer_group": CONSUMER_GROUP,
            "rebalance_reason": random.choice([
                "member_join", "member_leave", "heartbeat_timeout", "metadata_change"
            ]),
        }))


def _check_consumer_lag(topic, partition, offset):
    """Randomly emit consumer lag warnings."""
    key = f"{topic}-{partition}"
    _consumer_offsets[key] = offset

    if random.random() < 0.02:
        lag = random.randint(100, 50000)
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": "warn",
            "service": "event-processor",
            "message": f"consumer lag on partition {partition}: {lag} messages behind",
            "trace_id": _trace_id(),
            "topic": topic,
            "partition": partition,
            "current_offset": offset,
            "estimated_lag": lag,
        }))


async def handle_orders(msg, data, producer):
    """Update order status in DB, log state transitions."""
    order_id = data.get("order_id", f"ORD-{random.randint(1, 99999):07d}")
    old_status = data.get("previous_status", random.choice(["NEW", "PENDING", "SUBMITTED"]))
    new_status = data.get("status", random.choice(["ACKNOWLEDGED", "PARTIAL_FILL", "FILLED", "REJECTED"]))
    symbol = data.get("symbol", random.choice(["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]))

    return {
        "handler": "orders",
        "order_id": order_id,
        "state_transition": f"{old_status} -> {new_status}",
        "symbol": symbol,
        "message": f"Order {order_id} state transition: {old_status} -> {new_status}",
    }


async def handle_trades(msg, data, producer):
    """Update positions table, calculate running P&L."""
    trade_id = data.get("trade_id", f"TRD-{random.randint(1, 99999):07d}")
    symbol = data.get("symbol", random.choice(["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]))
    side = data.get("side", random.choice(["BUY", "SELL"]))
    qty = data.get("quantity", random.randint(100, 5000))
    price = data.get("price", round(random.uniform(50, 500), 4))

    pos_key = symbol
    if pos_key not in _positions:
        _positions[pos_key] = {"qty": 0, "cost_basis": 0.0, "realized_pnl": 0.0}

    pos = _positions[pos_key]
    if side == "BUY":
        pos["cost_basis"] = ((pos["cost_basis"] * pos["qty"]) + (price * qty)) / max(pos["qty"] + qty, 1)
        pos["qty"] += qty
    else:
        if pos["qty"] > 0:
            pos["realized_pnl"] += (price - pos["cost_basis"]) * min(qty, pos["qty"])
        pos["qty"] -= qty

    return {
        "handler": "trades",
        "trade_id": trade_id,
        "symbol": symbol,
        "side": side,
        "quantity": qty,
        "price": price,
        "position_qty": pos["qty"],
        "running_pnl": round(pos["realized_pnl"], 2),
    }


async def handle_order_updates(msg, data, producer):
    """Track fill rates, slippage metrics (stored in Redis)."""
    order_id = data.get("order_id", f"ORD-{random.randint(1, 99999):07d}")
    fill_qty = data.get("fill_quantity", random.randint(10, 1000))
    total_qty = data.get("total_quantity", fill_qty + random.randint(0, 5000))
    fill_rate = round(fill_qty / max(total_qty, 1) * 100, 2)
    expected_price = data.get("expected_price", round(random.uniform(50, 500), 4))
    actual_price = data.get("actual_price", round(expected_price * random.uniform(0.998, 1.002), 4))
    slippage_bps = round((actual_price - expected_price) / expected_price * 10000, 2)

    return {
        "handler": "order_updates",
        "order_id": order_id,
        "fill_rate_pct": fill_rate,
        "slippage_bps": slippage_bps,
        "expected_price": expected_price,
        "actual_price": actual_price,
    }


async def handle_risk_updates(msg, data, producer):
    """Store in Elasticsearch, check thresholds."""
    portfolio_id = data.get("portfolio_id", f"PF-{random.randint(1, 100):03d}")
    var_amount = data.get("var_amount", round(random.uniform(100000, 5000000), 2))
    var_limit = data.get("var_limit", round(var_amount * random.uniform(1.0, 2.0), 2))
    utilization = round(var_amount / max(var_limit, 1) * 100, 2)

    threshold_breach = utilization > 85
    extra = {
        "handler": "risk_updates",
        "portfolio_id": portfolio_id,
        "var_amount": var_amount,
        "var_limit": var_limit,
        "utilization_pct": utilization,
    }

    if threshold_breach:
        extra["alert"] = "VAR_THRESHOLD_BREACH"
        extra["message"] = f"Portfolio {portfolio_id} VaR utilization at {utilization}% (limit breach)"

    return extra


async def handle_market_data(msg, data, producer):
    """Aggregate into minute bars, store snapshots."""
    symbol = data.get("symbol", random.choice(["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]))
    price = data.get("price", round(random.uniform(50, 500), 4))
    volume = data.get("volume", random.randint(100, 100000))

    minute_key = f"{symbol}-{datetime.utcnow().strftime('%Y%m%d%H%M')}"
    if minute_key not in _minute_bars:
        _minute_bars[minute_key] = {
            "open": price, "high": price, "low": price, "close": price,
            "volume": 0, "tick_count": 0,
        }

    bar = _minute_bars[minute_key]
    bar["high"] = max(bar["high"], price)
    bar["low"] = min(bar["low"], price)
    bar["close"] = price
    bar["volume"] += volume
    bar["tick_count"] += 1

    # Prune old bars to avoid memory leak
    if len(_minute_bars) > 5000:
        oldest_keys = sorted(_minute_bars.keys())[:2500]
        for k in oldest_keys:
            del _minute_bars[k]

    return {
        "handler": "market_data",
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "minute_bar_key": minute_key,
        "bar_tick_count": bar["tick_count"],
    }


async def handle_compliance_alerts(msg, data, producer):
    """Log alerts, store in DB compliance_alerts table."""
    alert_id = data.get("alert_id", f"COMP-{random.randint(1, 99999):07d}")
    alert_type = data.get("alert_type", random.choice([
        "WASH_TRADE", "SPOOFING", "LAYERING", "FRONT_RUNNING",
        "CONCENTRATION_LIMIT", "RESTRICTED_SECURITY", "FAT_FINGER",
    ]))
    severity = data.get("severity", random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"]))
    account = data.get("account", f"INST-{random.randint(1, 50):03d}")

    return {
        "handler": "compliance_alerts",
        "alert_id": alert_id,
        "alert_type": alert_type,
        "severity": severity,
        "account": account,
        "message": f"Compliance alert {alert_id}: {alert_type} severity={severity} account={account}",
    }


async def handle_legacy_mq(msg, data, producer):
    """Parse MQ messages, attempt correlation with modern order IDs."""
    # data might be raw text, not JSON
    raw = msg.value.decode("utf-8", errors="replace") if isinstance(msg.value, bytes) else str(data)

    # Try to extract trade ref from MQ message
    trade_ref = None
    for line in raw.split("\n"):
        if "TRADE_REF=" in line:
            parts = line.split("TRADE_REF=")
            if len(parts) > 1:
                trade_ref = parts[1].split()[0].strip()
                break

    txid = None
    for line in raw.split("\n"):
        if "TXID=" in line:
            parts = line.split("TXID=")
            if len(parts) > 1:
                txid = parts[1].split()[0].strip()
                break

    # Correlation sometimes fails
    correlated = random.random() > 0.15
    result = {
        "handler": "legacy_mq",
        "legacy_trade_ref": trade_ref,
        "legacy_txid": txid,
        "correlated": correlated,
    }

    if not correlated:
        result["message"] = f"correlation failed for legacy txn {txid or trade_ref or 'UNKNOWN'}"
        result["level"] = "warn"

    return result


TOPIC_HANDLERS = {
    "orders": handle_orders,
    "trades": handle_trades,
    "order-updates": handle_order_updates,
    "risk-updates": handle_risk_updates,
    "market-data": handle_market_data,
    "compliance-alerts": handle_compliance_alerts,
    "legacy-mq": handle_legacy_mq,
}


async def process_message(msg, producer):
    """Process a single Kafka message from any topic."""
    start_time = time.monotonic()
    topic = msg.topic
    partition = msg.partition
    offset = msg.offset

    # Check for periodic events
    _check_rebalance()
    _check_consumer_lag(topic, partition, offset)

    # Track message counts
    _message_counts[topic] = _message_counts.get(topic, 0) + 1

    # 0.5% deserialization failure
    if random.random() < 0.005:
        error = f"MessageDeserializationError: Invalid JSON at offset {offset}"
        _log_structured(topic, partition, offset, 0, {
            "level": "error",
            "message": error,
            "raw_value_preview": msg.value[:100].hex() if msg.value else None,
        }, level="error")
        await _send_to_dlq(producer, topic, msg.key, msg.value, error)
        return

    # Parse message
    try:
        if topic == "legacy-mq":
            data = {}  # raw text, handler parses directly
        else:
            data = json.loads(msg.value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        _log_structured(topic, partition, offset, 0, {
            "level": "error",
            "message": f"Failed to decode message: {e}",
        }, level="error")
        await _send_to_dlq(producer, topic, msg.key, msg.value, str(e))
        return

    # Out-of-order detection
    event_ts = data.get("timestamp") or data.get("event_time")
    if event_ts and random.random() < 0.02:
        _log_structured(topic, partition, offset, 0, {
            "level": "warn",
            "message": "out-of-order event detected",
            "event_timestamp": event_ts,
            "server_time": datetime.utcnow().isoformat() + "Z",
            "delay_ms": random.randint(500, 30000),
        })

    # Dispatch to handler
    handler = TOPIC_HANDLERS.get(topic)
    if handler:
        try:
            result = await handler(msg, data, producer)
            processing_time_ms = round((time.monotonic() - start_time) * 1000, 2)
            log_level = result.pop("level", "info") if result else "info"
            _log_structured(topic, partition, offset, processing_time_ms, result, level=log_level)
        except Exception as e:
            processing_time_ms = round((time.monotonic() - start_time) * 1000, 2)
            _log_structured(topic, partition, offset, processing_time_ms, {
                "level": "error",
                "message": f"Handler error: {e}",
                "traceback": traceback.format_exc(),
            }, level="error")
            await _send_to_dlq(producer, topic, msg.key, msg.value, str(e))
    else:
        processing_time_ms = round((time.monotonic() - start_time) * 1000, 2)
        _log_structured(topic, partition, offset, processing_time_ms, {
            "level": "warn",
            "message": f"No handler for topic: {topic}",
        })


async def run():
    logger.info(json.dumps({
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": "info",
        "service": "event-processor",
        "message": "Event Processor starting",
        "kafka_brokers": KAFKA_BROKERS,
        "consumer_group": CONSUMER_GROUP,
        "topics": TOPICS,
    }))

    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        max_poll_records=200,
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        linger_ms=5,
    )

    await producer.start()
    await consumer.start()

    try:
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": "info",
            "service": "event-processor",
            "message": f"Consumer started, subscribed to {len(TOPICS)} topics",
            "topics": TOPICS,
        }))

        async for msg in consumer:
            await process_message(msg, producer)

    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
