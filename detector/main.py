import json
import signal
import threading

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from config import settings
from detector import IQRDetector
from models import PeakEvent, to_json_bytes

log = structlog.get_logger()

_shutdown = threading.Event()


def _handle_signal(sig, frame):
    log.info("shutdown_signal_received", signal=sig)
    _shutdown.set()


def _delivery_cb(err, msg):
    if err:
        log.error("delivery_failed", error=str(err), topic=msg.topic())


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )


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

    detector = IQRDetector(
        window_size=settings.window_size,
        min_window=settings.min_window,
        sigma=settings.sigma,
    )
    consumer = build_consumer()
    producer = build_producer()

    consumer.subscribe([settings.input_topic])
    log.info(
        "detector_starting",
        input_topic=settings.input_topic,
        output_topic=settings.output_topic,
        group_id=settings.group_id,
        window_size=settings.window_size,
        min_window=settings.min_window,
        sigma=settings.sigma,
        detection_feed=settings.detection_feed,
    )

    total_consumed = 0
    total_peaks = 0

    try:
        while not _shutdown.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                log.warning("bad_message_skipped", error=str(exc))
                continue

            if payload.get("feed") != settings.detection_feed:
                continue

            total_consumed += 1
            household = payload["household"]
            value_kwh = payload["value_kwh"]

            if detector.detect(household, value_kwh):
                event = PeakEvent(
                    room=household,
                    datetime=payload["utc_timestamp"],
                    level=value_kwh,
                )
                producer.produce(
                    settings.output_topic,
                    key=household.encode("utf-8"),
                    value=to_json_bytes(event),
                    on_delivery=_delivery_cb,
                )
                producer.poll(0)
                total_peaks += 1

    finally:
        consumer.close()
        producer.flush()
        log.info(
            "detector_done",
            total_consumed=total_consumed,
            total_peaks=total_peaks,
        )


if __name__ == "__main__":
    run()
