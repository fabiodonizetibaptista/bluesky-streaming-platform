"""Carga sintética e reconciliação Kafka, sem dados reais do Bluesky.

Uso:
    .venv/bin/python scripts/benchmark_kafka.py --events 10000

Cada execução cria um tópico novo, sem tocar em tópicos existentes.
O relatório é preservado em runtime/benchmarks/.
"""

import argparse
import json
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = "127.0.0.1:9092"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--partitions", type=int, default=4)
    args = parser.parse_args()

    if not 1 <= args.events <= 1_000_000:
        raise SystemExit("EVENT_COUNT_OUT_OF_RANGE")

    if not 1 <= args.partitions <= 8:
        raise SystemExit("PARTITION_COUNT_OUT_OF_RANGE")

    run_id = uuid.uuid4().hex[:12]
    topic = f"bluesky.benchmark.{run_id}"

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    print("=== KAFKA BENCHMARK ===", flush=True)
    print("RUN_ID =", run_id)
    print("TOPIC =", topic)
    print("EVENTS_TARGET =", args.events)
    print("PARTITIONS =", args.partitions)

    admin.create_topics([
        NewTopic(
            topic,
            num_partitions=args.partitions,
            replication_factor=1,
            config={"retention.ms": "86400000"},
        )
    ])[topic].result(timeout=30)

    delivered = 0
    failed = 0

    def on_delivery(error, message):
        nonlocal delivered, failed

        if error is not None:
            failed += 1
        else:
            delivered += 1

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 10,
        "batch.size": 131072,
    })

    started = time.monotonic()

    for index in range(args.events):
        event = {
            "source": "bluesky_jetstream_v2",
            "source_seq": index + 1,
            "collection": "app.bsky.feed.post",
            "operation": "create",
            "event_time": "2026-10-21T00:00:00Z",
            "witnessed_at": "2026-10-21T00:00:01Z",
        }

        payload = json.dumps(
            event, separators=(",", ":")
        ).encode("utf-8")

        while True:
            try:
                producer.produce(
                    topic,
                    value=payload,
                    partition=index % args.partitions,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.1)

        if index % 1000 == 0:
            producer.poll(0)

    remaining = producer.flush(120)
    duration = time.monotonic() - started

    print("DELIVERED =", delivered)
    print("FAILED_DELIVERIES =", failed)
    print("UNFLUSHED_MESSAGES =", remaining)
    print("PUBLISH_DURATION_SECONDS =", round(duration, 3))

    if failed or remaining or delivered != args.events:
        raise SystemExit("KAFKA_DELIVERY_VALIDATION_FAILED")

    # Consultar metadados do broker para reconciliar os offsets,
    # sem consumir nem imprimir conteúdos dos eventos.
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"benchmark-audit-{run_id}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })

    try:
        counts = {}
        for partition in range(args.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=15,
                cached=False,
            )
            counts[str(partition)] = high - low
    finally:
        consumer.close()

    persisted = sum(counts.values())
    throughput = delivered / duration if duration else 0

    print("PERSISTED_OFFSETS =", persisted)
    print("PER_PARTITION =", counts)
    print("PUBLISH_THROUGHPUT_EVENTS_SEC =", round(throughput, 2))

    if persisted != args.events:
        raise SystemExit("KAFKA_OFFSET_RECONCILIATION_FAILED")

    report = {
        "run_id": run_id,
        "topic": topic,
        "events": args.events,
        "partitions": args.partitions,
        "delivered": delivered,
        "persisted_offsets": persisted,
        "per_partition": counts,
        "publish_duration_seconds": round(duration, 3),
        "publish_throughput_events_sec": round(throughput, 2),
        "status": "PASSED",
    }

    output = ROOT / "runtime/benchmarks" / f"{run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("REPORT =", output.relative_to(ROOT))
    print("KAFKA_BENCHMARK = PASSED")


if __name__ == "__main__":
    main()
