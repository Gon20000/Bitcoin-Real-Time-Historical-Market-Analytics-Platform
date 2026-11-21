"""Verify Kafka broker metadata, readability, and multi-partition roundtrip."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "prod.binance.btc_usdt_1s.v1")
EXPECTED_PARTITIONS = 3
EXPECTED_RETENTION_MS = "86400000"  # 24 hours
TRANSIENT_PARTITION_ERRORS = {
    KafkaError.UNKNOWN_TOPIC_OR_PART,
    KafkaError.LEADER_NOT_AVAILABLE,
    KafkaError.NOT_LEADER_FOR_PARTITION,
}


def verify_topic_metadata(
    bootstrap_servers: str,
    topic: str,
    expected_partitions: int,
    expected_retention_ms: str,
    timeout: float,
) -> list[int]:
    """Confirm the target topic exists with the required partitions and retention."""
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

    unassigned = [
        pid for pid, pmeta in topic_meta.partitions.items() if pmeta.leader < 0
    ]
    if unassigned:
        raise RuntimeError(
            f"Partitions without an elected leader on '{topic}': {sorted(unassigned)}."
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


def verify_topic_readability(
    bootstrap_servers: str,
    topic: str,
    partitions: list[int],
    timeout: float,
) -> None:
    """Confirm all target topic partitions are readable without writing to them."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"verify-read-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    try:
        watermarks: dict[int, tuple[int, int]] = {}
        for partition in partitions:
            offsets = consumer.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=timeout,
                cached=False,
            )
            if offsets is None:
                raise RuntimeError(
                    f"Timed out reading watermarks for '{topic}' partition {partition}."
                )
            watermarks[partition] = offsets
        print(
            f"[OK] Target topic '{topic}' partitions readable (watermarks={watermarks})"
        )
    finally:
        consumer.close()


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
        watermark_deadline = time.monotonic() + timeout
        start_positions: list[TopicPartition] = []
        for partition in partitions:
            offsets: tuple[int, int] | None = None
            while time.monotonic() < watermark_deadline:
                try:
                    offsets = consumer.get_watermark_offsets(
                        TopicPartition(topic, partition),
                        timeout=min(
                            2.0, max(0.5, watermark_deadline - time.monotonic())
                        ),
                        cached=False,
                    )
                    if offsets is not None:
                        break
                except KafkaException as exc:
                    err = exc.args[0] if exc.args else None
                    if (
                        isinstance(err, KafkaError)
                        and err.code() in TRANSIENT_PARTITION_ERRORS
                    ):
                        time.sleep(0.2)
                        continue
                    raise
                time.sleep(0.2)
            if offsets is None:
                raise RuntimeError(
                    f"Timed out fetching watermarks for partition {partition}."
                )
            _, high = offsets
            start_positions.append(TopicPartition(topic, partition, high))
        consumer.assign(start_positions)

        delivery_errors: list[str] = []

        def on_delivery(err: object, _msg: object) -> None:
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
        print(
            f"[OK] Produced {len(partitions)} probe messages "
            f"to '{topic}' across {partitions}"
        )

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
                text = (
                    raw_value.decode("utf-8")
                    if isinstance(raw_value, bytes)
                    else raw_value
                )
                decoded = json.loads(text)
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


def verify_ephemeral_roundtrip(
    bootstrap_servers: str,
    partitions: list[int],
    timeout: float,
) -> None:
    """Run a multi-partition roundtrip on a temporary topic so production stays clean."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    probe_topic = f"verify.kafka.probe.{uuid.uuid4().hex[:8]}"

    admin.create_topics(
        [
            NewTopic(
                topic=probe_topic,
                num_partitions=len(partitions),
                replication_factor=1,
            )
        ]
    )[probe_topic].result(timeout=timeout)

    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            meta = admin.list_topics(topic=probe_topic, timeout=2.0).topics.get(
                probe_topic
            )
            if (
                meta is not None
                and meta.error is None
                and len(meta.partitions) == len(partitions)
                and all(p.leader >= 0 for p in meta.partitions.values())
            ):
                break
            time.sleep(0.2)
        else:
            raise RuntimeError(
                f"Timed out waiting for temporary topic '{probe_topic}' to be ready."
            )

        verify_partition_roundtrip(
            bootstrap_servers=bootstrap_servers,
            topic=probe_topic,
            partitions=partitions,
            timeout=timeout,
        )
    finally:
        try:
            admin.delete_topics([probe_topic])[probe_topic].result(timeout=timeout)
            print(f"[OK] Cleaned up temporary probe topic '{probe_topic}'")
        except Exception as exc:
            print(
                f"[WARN] Failed to delete temporary probe topic '{probe_topic}': {exc}",
                file=sys.stderr,
            )


def run_kafka_verification(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
    expected_partitions: int = EXPECTED_PARTITIONS,
    expected_retention_ms: str = EXPECTED_RETENTION_MS,
    timeout: float = 10.0,
) -> None:
    """Verify topic config, read-only partition access, and broker roundtrip."""
    print(f"Verifying Kafka at {bootstrap_servers} for topic '{topic}'...")
    partitions = verify_topic_metadata(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        expected_partitions=expected_partitions,
        expected_retention_ms=expected_retention_ms,
        timeout=timeout,
    )
    verify_topic_readability(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        partitions=partitions,
        timeout=timeout,
    )
    verify_ephemeral_roundtrip(
        bootstrap_servers=bootstrap_servers,
        partitions=partitions,
        timeout=timeout,
    )
    print("[PASS] Kafka broker, topic config, and all partitions verified.")


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
