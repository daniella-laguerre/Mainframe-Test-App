import asyncio
import json
import logging
import math
import os
import random
import time
import traceback
from datetime import datetime, timedelta

import asyncpg
from elasticsearch import AsyncElasticsearch

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/trading")
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
ELASTICSEARCH_URL = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
RECONCILIATION_INTERVAL = int(os.environ.get("RECONCILIATION_INTERVAL", "300"))

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("batch-recon")


def _ts():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _log_banner(msg):
    logger.info(f"{_ts()} [BATCH-RECON] {'=' * 40}")
    logger.info(f"{_ts()} [BATCH-RECON] {msg}")
    logger.info(f"{_ts()} [BATCH-RECON] {'=' * 40}")


def _log(msg):
    logger.info(f"{_ts()} [BATCH-RECON] {msg}")


def _log_json(data):
    logger.info(json.dumps(data))


async def _get_db_pool():
    try:
        pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=10, command_timeout=30)
        return pool
    except Exception as e:
        logger.warning(f"{_ts()} [BATCH-RECON] DB connection failed: {e}. Running in simulation mode.")
        return None


async def _get_es_client():
    try:
        es = AsyncElasticsearch(ELASTICSEARCH_URL)
        await es.info()
        return es
    except Exception as e:
        logger.warning(f"{_ts()} [BATCH-RECON] Elasticsearch connection failed: {e}. Running in simulation mode.")
        return None


async def _ensure_batch_jobs_table(pool):
    if pool is None:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS batch_jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    result JSONB,
                    error_message TEXT
                )
            """)
    except Exception as e:
        logger.warning(f"{_ts()} [BATCH-RECON] Failed to ensure batch_jobs table: {e}")


async def _store_batch_result(pool, job_id, job_type, status, result, error_message=None):
    if pool is None:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO batch_jobs (job_id, job_type, status, started_at, completed_at, result, error_message)
                VALUES ($1, $2, $3, NOW(), NOW(), $4::jsonb, $5)
                ON CONFLICT (job_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    completed_at = NOW(),
                    result = EXCLUDED.result,
                    error_message = EXCLUDED.error_message
                """,
                job_id, job_type, status, json.dumps(result), error_message,
            )
    except Exception as e:
        logger.warning(f"{_ts()} [BATCH-RECON] Failed to store batch result: {e}")


def _simulate_cpu_spike(duration_secs=0.5):
    """Burn CPU to simulate heavy reconciliation work."""
    end = time.monotonic() + duration_secs
    while time.monotonic() < end:
        math.factorial(random.randint(500, 1500))


async def step_trade_reconciliation(pool, job_id):
    """Step 1: EOD Trade Reconciliation."""
    _log("Step 1/5: Trade Reconciliation")

    total_trades = random.randint(8000, 25000)
    _log(f"RECON-001: Trade reconciliation started, processing {total_trades} trades")

    # Simulate processing time with CPU spike
    await asyncio.to_thread(_simulate_cpu_spike, random.uniform(0.3, 1.0))
    await asyncio.sleep(random.uniform(0.5, 2.0))

    matched = total_trades - random.randint(0, 15)
    mismatched = random.randint(0, 8)
    missing = total_trades - matched - mismatched

    result = {"matched": matched, "mismatched": mismatched, "missing": max(0, missing)}
    _log(f"RECON-001: Complete. Matched: {matched}, Mismatched: {mismatched}, Missing: {max(0, missing)}")
    _log_json({"job_id": job_id, "step": 1, "step_name": "trade_reconciliation", "status": "complete", **result})

    if random.random() < 0.05:
        _log("RECON-001: WARNING - Partial failure detected. Some trades could not be reconciled.")
        _log_json({"job_id": job_id, "step": 1, "status": "partial_failure", "unreconciled": random.randint(1, 5)})

    return result


async def step_position_reconciliation(pool, job_id):
    """Step 2: Position Reconciliation."""
    _log("Step 2/5: Position Reconciliation")

    num_positions = random.randint(200, 1000)
    _log(f"RECON-002: Verifying {num_positions} positions against trade sums")

    await asyncio.to_thread(_simulate_cpu_spike, random.uniform(0.2, 0.8))
    await asyncio.sleep(random.uniform(0.3, 1.5))

    discrepancies = random.randint(0, 5)
    result = {"positions_checked": num_positions, "discrepancies": discrepancies}

    if discrepancies > 0:
        for i in range(discrepancies):
            acct = f"INST-{random.randint(1, 50):03d}"
            symbol = random.choice(["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NVDA", "JPM"])
            diff = round(random.uniform(-500, 500), 2)
            _log(f"RECON-002: DISCREPANCY account={acct} symbol={symbol} diff={diff}")

    _log(f"RECON-002: Complete. Checked: {num_positions}, Discrepancies: {discrepancies}")
    _log_json({"job_id": job_id, "step": 2, "step_name": "position_reconciliation", "status": "complete", **result})
    return result


async def step_settlement_check(pool, job_id):
    """Step 3: Settlement Check."""
    _log("Step 3/5: Settlement Check")

    total_pending = random.randint(50, 500)
    _log(f"RECON-003: Checking {total_pending} pending settlements")

    await asyncio.sleep(random.uniform(0.3, 1.0))

    past_due = random.randint(0, 10)
    result = {"pending_settlements": total_pending, "past_due": past_due}

    if past_due > 0:
        for i in range(min(past_due, 5)):
            trade_id = f"TRD-{random.randint(1, 99999):07d}"
            settle_date = (datetime.utcnow() - timedelta(days=random.randint(1, 5))).strftime("%Y-%m-%d")
            _log(f"RECON-003: PAST DUE trade={trade_id} settle_date={settle_date} status=UNSETTLED")

    _log(f"RECON-003: Complete. Pending: {total_pending}, Past due: {past_due}")
    _log_json({"job_id": job_id, "step": 3, "step_name": "settlement_check", "status": "complete", **result})
    return result


async def step_risk_recalculation(pool, job_id):
    """Step 4: Risk Recalculation (VaR)."""
    _log("Step 4/5: Risk Recalculation (End-of-Day VaR)")

    start_time = time.monotonic()
    num_portfolios = random.randint(20, 100)
    _log(f"RECON-004: Recalculating VaR for {num_portfolios} portfolios")

    await asyncio.to_thread(_simulate_cpu_spike, random.uniform(0.5, 2.0))
    await asyncio.sleep(random.uniform(0.5, 2.0))

    duration_ms = int((time.monotonic() - start_time) * 1000)
    total_var = round(random.uniform(1_000_000, 50_000_000), 2)
    var_change_pct = round(random.uniform(-15, 15), 2)

    result = {
        "portfolios": num_portfolios,
        "total_var_usd": total_var,
        "var_change_pct": var_change_pct,
        "duration_ms": duration_ms,
    }

    _log(f"RECON-004: Complete. VaR=${total_var:,.2f} change={var_change_pct:+.2f}% duration={duration_ms}ms")
    _log_json({"job_id": job_id, "step": 4, "step_name": "risk_recalculation", "status": "complete", **result})

    if abs(var_change_pct) > 10:
        _log(f"RECON-004: ALERT - VaR change exceeds 10% threshold! Escalating to risk team.")

    return result


async def step_data_drift_detection(pool, job_id):
    """Step 5: Data Drift Detection."""
    _log("Step 5/5: Data Drift Detection")

    tables_checked = random.randint(10, 30)
    _log(f"RECON-005: Scanning {tables_checked} tables for schema changes and null values")

    await asyncio.sleep(random.uniform(0.3, 1.0))

    schema_changes = random.randint(0, 2)
    unexpected_nulls = random.randint(0, 8)
    result = {
        "tables_checked": tables_checked,
        "schema_changes_detected": schema_changes,
        "unexpected_null_columns": unexpected_nulls,
    }

    if schema_changes > 0:
        for i in range(schema_changes):
            table = random.choice(["trades", "orders", "positions", "accounts", "instruments"])
            change = random.choice(["new_column_added", "column_type_changed", "column_dropped"])
            _log(f"RECON-005: DRIFT DETECTED table={table} change={change}")

    if unexpected_nulls > 0:
        for i in range(min(unexpected_nulls, 3)):
            table = random.choice(["trades", "orders", "positions"])
            col = random.choice(["settlement_date", "counterparty_id", "clearing_house", "fx_rate"])
            null_pct = round(random.uniform(0.1, 15.0), 2)
            _log(f"RECON-005: UNEXPECTED NULLS table={table} column={col} null_pct={null_pct}%")

    _log(f"RECON-005: Complete. Schema changes: {schema_changes}, Unexpected nulls: {unexpected_nulls}")
    _log_json({"job_id": job_id, "step": 5, "step_name": "data_drift_detection", "status": "complete", **result})
    return result


async def run_reconciliation_cycle(pool):
    """Run a full reconciliation cycle."""
    job_id = f"RECON-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

    _log_banner("END-OF-DAY RECONCILIATION STARTING")
    _log(f"Job ID: {job_id}")
    _log(f"Interval: {RECONCILIATION_INTERVAL}s")

    overall_start = time.monotonic()
    results = {}
    overall_status = "complete"
    error_message = None

    # 1% chance of full failure
    if random.random() < 0.01:
        _log("FATAL: Database connection lost during reconciliation!")
        _log_json({"job_id": job_id, "status": "failed", "error": "database_connection_lost"})
        await _store_batch_result(pool, job_id, "eod_reconciliation", "failed", {}, "Database connection lost")
        return

    steps = [
        ("trade_reconciliation", step_trade_reconciliation),
        ("position_reconciliation", step_position_reconciliation),
        ("settlement_check", step_settlement_check),
        ("risk_recalculation", step_risk_recalculation),
        ("data_drift_detection", step_data_drift_detection),
    ]

    for step_name, step_fn in steps:
        try:
            result = await step_fn(pool, job_id)
            results[step_name] = result
        except Exception as e:
            _log(f"ERROR in {step_name}: {e}")
            _log(traceback.format_exc())
            results[step_name] = {"status": "error", "error": str(e)}
            overall_status = "partial_failure"
            error_message = f"Step {step_name} failed: {e}"

            # 5% chance of partial failure per step
            if random.random() < 0.05:
                _log(f"WARNING: Partial failure in {step_name}, continuing with remaining steps")

    total_duration = int((time.monotonic() - overall_start) * 1000)

    _log_banner("END-OF-DAY RECONCILIATION COMPLETE")
    _log(f"Job ID: {job_id}")
    _log(f"Status: {overall_status}")
    _log(f"Total duration: {total_duration}ms")

    _log_json({
        "job_id": job_id,
        "status": overall_status,
        "total_duration_ms": total_duration,
        "results": results,
    })

    await _store_batch_result(pool, job_id, "eod_reconciliation", overall_status, results, error_message)


async def run():
    logger.info(f"{_ts()} [BATCH-RECON] Batch Reconciler starting.")
    logger.info(f"{_ts()} [BATCH-RECON] Reconciliation interval: {RECONCILIATION_INTERVAL}s")
    logger.info(f"{_ts()} [BATCH-RECON] Postgres: {POSTGRES_DSN.split('@')[-1] if '@' in POSTGRES_DSN else 'configured'}")

    pool = await _get_db_pool()
    if pool:
        await _ensure_batch_jobs_table(pool)

    while True:
        try:
            await run_reconciliation_cycle(pool)
        except Exception as e:
            logger.error(f"{_ts()} [BATCH-RECON] Reconciliation cycle error: {e}")
            logger.error(traceback.format_exc())

        _log(f"Next reconciliation in {RECONCILIATION_INTERVAL}s")
        await asyncio.sleep(RECONCILIATION_INTERVAL)


if __name__ == "__main__":
    asyncio.run(run())
