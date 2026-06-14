import json
import threading

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException

from config import settings
from domain import PeakEvent, parse_peak
from store import PeakStore

log = structlog.get_logger()


def decode(raw: bytes) -> PeakEvent | None:
    try:
        return parse_peak(json.loads(raw.decode("utf-8")))
    except (json.JSONDecodeError, UnicodeDecodeError, KeyError, ValueError, TypeError) as exc:
        log.warning("bad_message_skipped", error=str(exc))
        return None


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.group_id,
            "auto.offset.reset": "earliest" if settings.from_beginning else "latest",
            "enable.auto.commit": True,
        }
    )


def consume_loop(store: PeakStore, stop: threading.Event) -> None:
    consumer = build_consumer()
    consumer.subscribe([settings.input_topic])
    log.info(
        "reporter_consumer_starting",
        input_topic=settings.input_topic,
        group_id=settings.group_id,
    )
    try:
        while not stop.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            event = decode(msg.value())
            if event is not None:
                store.add(event)
    finally:
        consumer.close()
        log.info("reporter_consumer_stopped")