"""Verify Kafka broker metadata and multi-partition produce/consume roundtrip."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "prod.binance.btc_usdt_1s.v1")
EXPECTED_PARTITIONS = 3
EXPECTED_RETENTION_MS = "86400000"  # 24 hours


def verify_topic_metadata(
    bootstrap_servers: str,
    topic: str,
    expected_partitions: int,
    expected_retention_ms: str,
    timeout: float,
) -> list[int]:
    """Confirm the topic exists with the required partition count and retention."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    metadata = admin.list_topics(topic=topic, timeout=timeout)
    topic_meta = metadata.topics.get(topic)
    if topic_meta is None or topic_meta.error is not None:
        err = topic_meta.error if topic_meta else "topic not found"
        raise RuntimeError(f"Topic '{topic}' metadata error: {err}")

    partitions = sorted(topic_meta.partitions.keys())
    if len(partitions) != expected_partitions:
        raise RuntimeError(
            f"Expected {expected_partitions} partitions for '{topic}', "
            f"found {len(partitions)} ({partitions})."
        )

    resource = ConfigResource(ResourceType.TOPIC, topic)
    futures = admin.describe_configs([resource])
    configs = futures[resource].result(timeout=timeout)
    retention_entry = configs.get("retention.ms")
    actual_retention = retention_entry.value if retention_entry else None
    if actual_retention != expected_retention_ms:
        raise RuntimeError(
            f"Expected retention.ms={expected_retention_ms} for '{topic}', "
            f"found {actual_retention}."
        )

    print(
        f"[OK] Topic '{topic}': {len(partitions)} partitions {partitions}, "
        f"retention.ms={actual_retention}"
    )
    return partitions


def verify_partition_roundtrip(
    bootstrap_servers: str,
    topic: str,
    partitions: list[int],
    timeout: float,
) -> None:
    """Produce and consume one probe message on every partition."""
    run_id = uuid.uuid4().hex
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"verify-kafka-{run_id}",
            "enable.auto.commit": False,
        }
    )

    try:
        # Capture current end offsets before producing so we only read our probes
        start_positions: list[TopicPartition] = []
        for partition in partitions:
            offsets = consumer.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=timeout,
                cached=False,
            )
            if offsets is None:
                raise RuntimeError(
                    f"Timed out fetching watermarks for partition {partition}."
                )
            _, high = offsets
            start_positions.append(TopicPartition(topic, partition, high))
        consumer.assign(start_positions)

        delivery_errors: list[str] = []

        def on_delivery(err, _msg) -> None:
            if err is not None:
                delivery_errors.append(str(err))

        producer = Producer({"bootstrap.servers": bootstrap_servers})

        expected: dict[int, dict[str, object]] = {}
        for partition in partitions:
            payload: dict[str, object] = {
                "verify_run_id": run_id,
                "partition": partition,
                "message": f"smoke-test-partition-{partition}",
            }
            expected[partition] = payload
            producer.produce(
                topic=topic,
                key=f"verify-{partition}".encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                partition=partition,
                on_delivery=on_delivery,
            )
            producer.poll(0)

        remaining = producer.flush(timeout)
        if remaining > 0 or delivery_errors:
            raise RuntimeError(
                f"Failed to deliver probe messages "
                f"(remaining={remaining}, errors={delivery_errors})."
            )
        print(f"[OK] Produced {len(partitions)} probe messages across {partitions}")

        received: dict[int, dict[str, object]] = {}
        deadline = time.monotonic() + timeout
        while len(received) < len(expected) and time.monotonic() < deadline:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            err = msg.error()
            if err is not None:
                raise RuntimeError(f"Consumer error: {err}")

            raw_value = msg.value()
            if not raw_value:
                continue
            try:
                decoded = json.loads(
                    raw_value.decode("utf-8")
                    if isinstance(raw_value, bytes)
                    else raw_value
                )
            except json.JSONDecodeError:
                continue

            if isinstance(decoded, dict) and decoded.get("verify_run_id") == run_id:
                partition = msg.partition()
                if partition is not None:
                    received[partition] = decoded
                    print(
                        f"[OK] Consumed from partition {partition} "
                        f"@ offset {msg.offset()}: {decoded['message']}"
                    )

        missing = sorted(set(expected) - set(received))
        if missing:
            raise RuntimeError(
                f"Timed out waiting for probe messages on partitions {missing}."
            )
        if received != expected:
            raise RuntimeError(
                f"Payload mismatch: expected={expected}, received={received}"
            )
    finally:
        consumer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify Kafka topic configuration and multi-partition roundtrip."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers (default: %(default)s).",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic to verify (default: %(default)s).",
    )
    parser.add_argument(
        "--expected-partitions",
        type=int,
        default=EXPECTED_PARTITIONS,
        help="Expected partition count (default: %(default)s).",
    )
    parser.add_argument(
        "--expected-retention-ms",
        default=EXPECTED_RETENTION_MS,
        help="Expected retention.ms topic config (default: %(default)s).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Timeout in seconds for broker and consumer operations.",
    )
    return parser.parse_args()


def run_kafka_verification(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
    expected_partitions: int = EXPECTED_PARTITIONS,
    expected_retention_ms: str = EXPECTED_RETENTION_MS,
    timeout: float = 10.0,
) -> None:
    """Run metadata and multi-partition roundtrip checks against Kafka."""
    print(f"Verifying Kafka at {bootstrap_servers} for topic '{topic}'...")
    partitions = verify_topic_metadata(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        expected_partitions=expected_partitions,
        expected_retention_ms=expected_retention_ms,
        timeout=timeout,
    )
    verify_partition_roundtrip(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        partitions=partitions,
        timeout=timeout,
    )
    print("[PASS] Kafka broker, topic config, and all partitions verified.")


def main() -> None:
    args = parse_args()
    try:
        run_kafka_verification(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            expected_partitions=args.expected_partitions,
            expected_retention_ms=args.expected_retention_ms,
            timeout=args.timeout,
        )
    except Exception as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
