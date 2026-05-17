"""
consumer.py
Reads CDC events from the Debezium SQL Server topic and pretty-prints
the operation type, before/after row values, and event metadata.

Topic (set in connector.json):  cdc.CdcDemo.dbo.customers

Debezium operation codes:
    r = Read      (snapshot — initial dump of existing rows on first connector start)
    c = Create    (INSERT)
    u = Update    (UPDATE)
    d = Delete    (DELETE)

Run:
    pip install -r requirements.txt
    python consumer.py
"""

import json
import os
import signal
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException

# Read from env vars so you can switch clusters without editing code.
# On failover:
#   $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
#   $env:KAFKA_TOPIC = "primary.cdc.CdcDemo.dbo.customers"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "cdc.CdcDemo.dbo.customers")
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
# TOPIC = os.getenv("KAFKA_TOPIC", "primary.cdc.CdcDemo.dbo.customers")

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "cdc-python-consumer-1",
    # `Offset` is the position of a consumer in a partition, and it indicates which messages have been consumed.
    # enable.auto.commit - every messages you read gets its offset committed to Kafka,
    # so when you restart the consumer, it will start from the last committed offset.
    "enable.auto.commit": True, 
    # auto.offset.reset - controls where to start reading when there is no committed offset for the group.
    # "earliest" - start from the very first message in the topic (replays all events on every restart when auto-commit is off).
    # "latest"   - start from new messages only, ignore everything published before the consumer started.
    # Note: this setting is ignored if a committed offset already exists for the group.id — Kafka always resumes from the committed position.
    "auto.offset.reset": "earliest",
}

OPERATION_LABELS = {
    "r": "SNAPSHOT READ",
    "c": "INSERT",
    "u": "UPDATE",
    "d": "DELETE",
}


def _ts(ts_ms):
    if ts_ms is None:
        return "N/A"
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _sep(char="─", width=70):
    print(char * width)


def pretty_print_event(payload):
    """
    With value.converter.schemas.enable=false the message value IS the payload —
    a flat JSON object with op, before, after, source, ts_ms fields directly.
    """
    op     = payload.get("op", "?")
    before = payload.get("before")
    after  = payload.get("after")
    source = payload.get("source", {})

    _sep()
    print(f"  Operation  : {OPERATION_LABELS.get(op, f'UNKNOWN ({op})')}  [{op}]")
    print(f"  Event time : {_ts(payload.get('ts_ms'))}")
    print(f"  Table      : {source.get('schema', '?')}.{source.get('table', '?')}")
    print(f"  LSN        : {source.get('change_lsn', 'N/A')}")
    _sep(char="·")

    if before is not None:
        print("  BEFORE:")
        for k, v in before.items():
            print(f"    {k}: {v}")
    else:
        print("  BEFORE: — (no previous state)")

    print()

    if after is not None:
        print("  AFTER:")
        for k, v in after.items():
            if before is not None and before.get(k) != v:
                print(f"    {k}: {v}  ← changed from: {before.get(k)}")
            else:
                print(f"    {k}: {v}")
    else:
        print("  AFTER:  — (row was deleted)")

    _sep()
    print()


def main():
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([TOPIC])

    running = True

    def stop(sig, frame):
        nonlocal running
        print("\nShutting down...")
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f"Listening on : {TOPIC}")
    print(f"Broker       : {KAFKA_BOOTSTRAP_SERVERS}")
    print("Press Ctrl-C to stop.\n")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"  [Partition {msg.partition()} EOF — waiting for new events]")
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print("  [Topic not found — connector may still be starting up, retrying...]")
                else:
                    raise KafkaException(msg.error())
                continue

            raw = msg.value()
            if raw is None:
                # Tombstone: Debezium emits null-value messages for deletes on
                # log-compacted topics to signal downstream systems to remove the key.
                print("  [Tombstone message — key was deleted from compacted topic]\n")
                continue

            try:
                payload = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                print(f"  [Decode error: {exc}]")
                print(f"  Raw: {raw[:200]}\n")
                continue

            pretty_print_event(payload)

    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
