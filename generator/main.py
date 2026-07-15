from itertools import batched
import signal
import threading
import time

import structlog

from config import settings
from loader import readings_from_csv
from producer import build_producer, publish_batch


log = structlog.get_logger()

BATCH_SIZE = 50

_shutdown = threading.Event()


def _handle_signal(sig, frame):
    log.info("shutdown_signal_received", signal=sig)
    _shutdown.set()


def stream_readings(producer) -> tuple[int, int]:
    """One pass over the data source: publish every reading, paced to EVENTS_PER_SECOND."""
    messages = 0
    sent_bytes = 0
    for batch in batched(readings_from_csv(settings.data_source), BATCH_SIZE):
        if _shutdown.is_set():
            break

        sent_bytes += publish_batch(producer, batch)
        messages += len(batch)
        time.sleep(len(batch) / settings.events_per_second)
    return messages, sent_bytes


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
        messages, sent_bytes = stream_readings(producer)
        total_messages += messages
        total_bytes += sent_bytes

        if not settings.loop:
            break

    producer.flush()
    log.info(
        "generator_done",
        total_messages=total_messages,
        total_bytes=total_bytes,
    )


if __name__ == "__main__":
    run()
