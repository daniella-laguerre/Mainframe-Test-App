import asyncio
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")

ABEND_CODES = ["S0C7", "S0C4", "S322", "S806"]
MQ_ERRORS = ["MQRC_Q_FULL(2053)", "MQRC_CONNECTION_BROKEN(2009)", "MQRC_NOT_AUTHORIZED(2035)"]
CICS_FAILURES = ["AEYP", "AKCT", "ASRA", "AEY9"]
ACCOUNTS = [f"INST-{i:03d}" for i in range(1, 51)]
PROGRAMS = ["TRDPROC1", "TRDPROC2", "STLPROC1", "RSKCHK01", "POSUPD01", "MRGNCALC"]
TERMINALS = [f"3270{chr(65 + i)}" for i in range(8)]

_batch_counter = 0
_trade_counter = 0


def _next_batch_id():
    global _batch_counter
    _batch_counter += 1
    return f"{_batch_counter:04d}"


def _next_trade_id():
    global _trade_counter
    _trade_counter += 1
    return f"TRD-{_trade_counter:07d}"


def _timestamp_cobol():
    """YYYYMMDD HHMMSS format."""
    now = datetime.utcnow()
    return now.strftime("%Y%m%d"), now.strftime("%H%M%S")


def _timestamp_julian():
    """Julian date format YYDDD."""
    now = datetime.utcnow()
    return now.strftime("%y") + f"{now.timetuple().tm_yday:03d}"


def _timestamp_epoch_millis():
    return str(int(time.time() * 1000))


def _random_settle_date():
    return (datetime.utcnow() + timedelta(days=random.choice([1, 2, 3]))).strftime("%Y%m%d")


def _generate_cobol_log(trade_id, qty, price, account, status, batch_id):
    """Generate COBOL-style fixed-width batch log lines."""
    date_str, time_str = _timestamp_cobol()
    rc = "0000" if status == "FILLED" else f"00{random.choice(['04', '08', '12'])}"
    settle_dt = _random_settle_date()
    mismatch = random.choice(["N", "N", "N", "Y"])
    margin_call = "Y" if mismatch == "Y" and random.random() < 0.3 else "N"

    lines = [
        f"COBRUN  {date_str} {time_str} BATCH-{batch_id} TRADE-PROC   "
        f"{'OK  ' if rc == '0000' else 'WARN'} "
        f"TRDID={trade_id} QTY={qty:010d} PRC={price:014.4f} "
        f"ACCT={account:<13s} STAT={status:<8s} RC={rc}",

        f"COBRUN  {date_str} {time_str} BATCH-{batch_id} SETTL-CHK    "
        f"{'OK  ' if mismatch == 'N' else 'WARN'} "
        f"TRDID={trade_id} SETTLE-DT={settle_dt} MISMATCH={mismatch} "
        f"MARGIN-CALL={margin_call}       RC={rc}",
    ]
    return lines


def _generate_mq_message(trade_id, batch_id):
    """Generate IBM MQ-style transaction message."""
    date_str, time_str = _timestamp_cobol()
    msg_id = uuid.uuid4().hex[:26].upper()
    prog = random.choice(PROGRAMS)
    term = random.choice(TERMINALS)

    mq_msg = (
        f"MQMD: MsgId={msg_id} PutDate={date_str} PutTime={time_str} Format=MQSTR\n"
        f"CICS-TXN: TXID=TX{batch_id} TERM={term} PROG={prog} ABCODE=     RESP=NORMAL\n"
        f"DATA: TRADE_REF={trade_id} TIMESTAMP={_timestamp_epoch_millis()} JULIAN={_timestamp_julian()}"
    )
    return mq_msg


def _generate_smf_record(batch_id):
    """Generate SMF-style record."""
    now = datetime.utcnow()
    ts = now.strftime("%Y-%m-%d %H:%M:%S.") + f"{now.microsecond // 1000:03d}"
    cpu_sec = random.uniform(0.01, 5.0)
    elapsed = cpu_sec + random.uniform(0.5, 10.0)
    excp = random.randint(0, 500)
    return (
        f"SMF030 {ts} SYSTEM=PROD1 JOBNAME=TRDBATCH STEPNAME=PROC01 "
        f"CPU={int(cpu_sec // 3600):02d}:{int((cpu_sec % 3600) // 60):02d}:{cpu_sec % 60:05.2f} "
        f"ELAPSED={int(elapsed // 3600):02d}:{int((elapsed % 3600) // 60):02d}:{elapsed % 60:05.2f} "
        f"EXCP={excp:07d}"
    )


def _generate_abend_trace(trade_id, abend_code):
    """Generate a multiline COBOL-style stack trace."""
    date_str, time_str = _timestamp_cobol()
    return (
        f"*** ABEND {abend_code} IN PROGRAM TRDPROC1 AT OFFSET X'00001A3C' ***\n"
        f"    PSW=078D1400 80001A3C  ILC=04  INTC=0004\n"
        f"    GPR 0-3: 00000000 00001000 {uuid.uuid4().hex[:8].upper()} 00000004\n"
        f"    GPR 4-7: 00000000 00000000 {uuid.uuid4().hex[:8].upper()} 00000001\n"
        f"    TRADE_REF={trade_id}\n"
        f"    TIMESTAMP={date_str} {time_str}\n"
        f"    RECOVERY ROUTINE ENTERED - ATTEMPTING RETRY\n"
        f"    SNAP DUMP WRITTEN TO SYSUDUMP DD"
    )


def _generate_mq_error(trade_id):
    """Generate MQ error message."""
    err = random.choice(MQ_ERRORS)
    date_str, time_str = _timestamp_cobol()
    return (
        f"MQERR {date_str} {time_str} QMGR=QM.PROD.01 QUEUE=TRD.PROC.IN "
        f"REASON={err} TRADE_REF={trade_id} "
        f"ACTION=MESSAGE_BACKED_OUT BACKOUT_COUNT=3"
    )


def _generate_cics_failure(trade_id, batch_id):
    """Generate CICS transaction failure."""
    abcode = random.choice(CICS_FAILURES)
    term = random.choice(TERMINALS)
    prog = random.choice(PROGRAMS)
    date_str, time_str = _timestamp_cobol()
    return (
        f"CICS-FAIL {date_str} {time_str} TXID=TX{batch_id} TERM={term} "
        f"PROG={prog} ABCODE={abcode} TRADE_REF={trade_id} "
        f"RESP=EXCEPTION RESP2=27 EIBRCODE=E0040044"
    )


async def process_message(msg, producer):
    """Process a single consumed message, generating legacy output."""
    try:
        value = json.loads(msg.value.decode("utf-8"))
    except Exception:
        value = {}

    trade_id = value.get("trade_id") or value.get("order_id") or _next_trade_id()
    qty = value.get("quantity", random.randint(100, 10000))
    price = value.get("price", round(random.uniform(10.0, 500.0), 4))
    account = value.get("account", random.choice(ACCOUNTS))
    status = value.get("status", random.choice(["FILLED", "PARTIAL", "PENDING"]))
    batch_id = _next_batch_id()

    # a) COBOL-style fixed-width batch logs
    cobol_lines = _generate_cobol_log(trade_id, int(qty), float(price), account, status, batch_id)
    for line in cobol_lines:
        print(line, flush=True)

    # b) IBM MQ-style transaction message -> publish to legacy-mq topic
    mq_msg = _generate_mq_message(trade_id, batch_id)
    await producer.send_and_wait(
        "legacy-mq",
        value=mq_msg.encode("utf-8"),
        key=str(trade_id).encode("utf-8"),
    )

    # c) SMF-style record
    print(_generate_smf_record(batch_id), flush=True)

    # d) Inject errors randomly
    roll = random.random()
    if roll < 0.03:
        # 3% ABEND
        abend_code = random.choice(ABEND_CODES)
        print(_generate_abend_trace(trade_id, abend_code), flush=True)
    elif roll < 0.05:
        # 2% MQ error
        print(_generate_mq_error(trade_id), flush=True)
    elif roll < 0.06:
        # 1% CICS failure
        print(_generate_cics_failure(trade_id, batch_id), flush=True)


async def run():
    consumer = AIOKafkaConsumer(
        "orders", "trades",
        bootstrap_servers=KAFKA_BROKERS,
        group_id="legacy-adapter-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        max_poll_records=100,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        linger_ms=10,
        max_batch_size=16384,
    )

    print(f"LEGACY-ADAPTER: Starting mainframe adapter bridge. Kafka={KAFKA_BROKERS}", flush=True)
    print(f"LEGACY-ADAPTER: Julian date={_timestamp_julian()} Epoch={_timestamp_epoch_millis()}", flush=True)

    await producer.start()
    await consumer.start()

    try:
        print("LEGACY-ADAPTER: Consumer connected. Awaiting messages on [orders, trades].", flush=True)
        async for msg in consumer:
            await process_message(msg, producer)
            # Throttle to ~50-100 msgs/sec range
            await asyncio.sleep(random.uniform(0.01, 0.02))
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
