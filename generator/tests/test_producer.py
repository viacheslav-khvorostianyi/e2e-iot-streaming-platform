from confluent_kafka import Producer
from structlog.testing import capture_logs

from config import settings
from models import HouseholdReading
from producer import handle_delivery_failure_callback, publish_batch


FLUSH_TIMEOUT_SEC = 10


def make_producer(**overrides) -> Producer:
    """Real Producer against librdkafka's in-process mock cluster."""
    conf = {
        "test.mock.num.brokers": 1,
        "acks": "all",
        "delivery.report.only.error": True,
        "on_delivery": handle_delivery_failure_callback,
    }
    conf.update(overrides)
    return Producer(conf)


def reading(**overrides) -> HouseholdReading:
    fields = {
        "household": "residential1",
        "room": "kitchen",
        "feed": "grid_import",
        "utc_timestamp": "2026-07-12T14:00:00+00:00",
        "value_kwh": 0.412,
    }
    fields.update(overrides)
    return HouseholdReading(**fields)


def batch_of_two() -> list[tuple[HouseholdReading, str]]:
    return [
        (reading(), "residential1:kitchen"),
        (reading(room="bedroom", value_kwh=0.12), "residential1:bedroom"),
    ]


def test_batch_is_delivered_without_failures():
    producer = make_producer()
    batch = batch_of_two()

    with capture_logs() as logs:
        sent = publish_batch(producer, batch)
        remaining = producer.flush(FLUSH_TIMEOUT_SEC)

    assert remaining == 0  # every message acked by the (mock) broker
    assert sent == sum(len(r.to_json_bytes()) for r, _ in batch)
    assert logs == []  # no publish errors, no delivery failures


def test_delivery_failure_reaches_callback_with_context():
    # partition 99 doesn't exist -> broker-side failure -> real delivery
    # report drives handle_delivery_failure_callback
    producer = make_producer()

    with capture_logs() as logs:
        producer.produce(
            topic=settings.topic,
            key=b"residential1:kitchen",
            value=reading().to_json_bytes(),
            partition=99,
        )
        producer.flush(FLUSH_TIMEOUT_SEC)

    assert [log["event"] for log in logs] == ["delivery_failed"]
    assert logs[0]["log_level"] == "error"
    assert logs[0]["topic"] == settings.topic
    assert logs[0]["key"] == "residential1:kitchen"
    assert logs[0]["error"]


def test_delivery_failure_of_keyless_message_logs_none_key():
    producer = make_producer()

    with capture_logs() as logs:
        producer.produce(
            topic=settings.topic,
            value=reading().to_json_bytes(),
            partition=99,
        )
        producer.flush(FLUSH_TIMEOUT_SEC)

    assert [log["event"] for log in logs] == ["delivery_failed"]
    assert logs[0]["key"] is None
