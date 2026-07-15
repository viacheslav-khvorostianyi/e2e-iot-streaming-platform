from collections.abc import Iterable

from confluent_kafka import KafkaException, Producer
import structlog

from config import settings
from models import HouseholdReading


log = structlog.get_logger()


def handle_delivery_failure_callback(err, msg):
    if not err:
        return
    key = msg.key()
    log.error(
        "delivery_failed",
        topic=msg.topic(),
        key=key.decode("utf-8") if key else None,
        error=str(err or msg.error()),
    )


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": settings.bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "delivery.report.only.error": True,
        "on_delivery": handle_delivery_failure_callback,
    })


def publish_batch(
    producer: Producer, batch: Iterable[tuple[HouseholdReading, str]]
) -> int:
    """Produce one message per reading. Returns total payload bytes handed to the producer."""
    sent_bytes = 0
    for reading, key in batch:
        payload = reading.to_json_bytes()
        try:
            producer.produce(
                topic=settings.topic,
                key=key.encode("utf-8"),
                value=payload,
                on_delivery=handle_delivery_failure_callback,
            )
            sent_bytes += len(payload)
        except KafkaException as e:
            log.exception(
                "batch_publishing_failed",
                topic=settings.topic,
                key=key,
                error=str(e),
            )
    producer.poll(0)
    return sent_bytes
