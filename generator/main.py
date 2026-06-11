import signal
import threading
import time

import structlog
from confluent_kafka import KafkaException, Producer

from config import settings
from loader import iter_readings
from models import HouseholdReading, to_json_bytes

log = structlog.get_logger()

BATCH_SIZE = 50

_shutdown = threading.Event()


def _handle_signal(sig, frame):
    log.info("shutdown_signal_received", signal=sig)
    _shutdown.set()


def _delivery_cb(err, msg):
    if err:
        log.error("delivery_failed", error=str(err), topic=msg.topic())


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
        }
    )


def run():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    producer = build_producer()
    total_messages = 0
    total_bytes = 0

    log.info(
        "generator_starting",
        bootstrap_servers=settings.bootstrap_servers,
        topic=settings.topic,
        data_source=settings.data_source,
        events_per_second=settings.events_per_second,
        loop=settings.loop,
    )

    while not _shutdown.is_set():
        batch: list[tuple[HouseholdReading, str]] = []

        for reading, key in iter_readings(settings.data_source):
            if _shutdown.is_set():
                break

            batch.append((reading, key))

            if len(batch) >= BATCH_SIZE:
                _flush_batch(producer, batch)
                total_messages += len(batch)
                total_bytes += sum(len(to_json_bytes(r)) for r, _ in batch)
                time.sleep(len(batch) / settings.events_per_second)
                batch.clear()

        if batch:
            _flush_batch(producer, batch)
            total_messages += len(batch)
            total_bytes += sum(len(to_json_bytes(r)) for r, _ in batch)

        if not settings.loop:
            break

    producer.flush()
    log.info(
        "generator_done",
        total_messages=total_messages,
        total_bytes=total_bytes,
    )


def _flush_batch(producer: Producer, batch: list[tuple[HouseholdReading, str]]):
    for reading, key in batch:
        try:
            producer.produce(
                settings.topic,
                key=key.encode("utf-8"),
                value=to_json_bytes(reading),
                on_delivery=_delivery_cb,
            )
        except KafkaException as e:
            log.error("produce_failed", error=str(e))
    producer.poll(0)


if __name__ == "__main__":
    run()
