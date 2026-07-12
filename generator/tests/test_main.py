import threading

from confluent_kafka import Producer
from conftest import valid_row, write_csv

from loader import readings_from_csv
import main
from main import stream_readings


def make_producer() -> Producer:
    return Producer({"test.mock.num.brokers": 1, "acks": "all"})


def configure(monkeypatch, source: str) -> None:
    monkeypatch.setattr(main.settings, "data_source", source)
    monkeypatch.setattr(
        main.settings, "events_per_second", 1_000_000
    )  # keep sleeps negligible
    monkeypatch.setattr(main, "_shutdown", threading.Event())


def test_streams_whole_file_to_kafka(tmp_path, monkeypatch):
    # 2 rows x 5 meters = 10 readings end-to-end: csv -> loader -> batches -> broker
    source = write_csv(tmp_path / "readings.csv", [valid_row(), valid_row()])
    configure(monkeypatch, source)
    producer = make_producer()

    messages, sent_bytes = stream_readings(producer)

    assert messages == 10
    assert sent_bytes == sum(
        len(reading.to_json_bytes()) for reading, _ in readings_from_csv(source)
    )
    assert producer.flush(10) == 0  # every message acked by the mock broker


def test_shutdown_stops_streaming_after_current_batch(tmp_path, monkeypatch):
    # 20 rows x 5 meters = 100 readings = exactly 2 batches of BATCH_SIZE=50;
    # shutdown requested during batch 1 -> batch 2 must never be published
    source = write_csv(tmp_path / "readings.csv", [valid_row() for _ in range(20)])
    configure(monkeypatch, source)
    producer = make_producer()

    real_publish = main.publish_batch

    def publish_then_request_shutdown(producer, batch):
        result = real_publish(producer, batch)
        main._shutdown.set()  # SIGTERM lands right after batch 1 is handed off
        return result

    monkeypatch.setattr(main, "publish_batch", publish_then_request_shutdown)

    messages, _ = stream_readings(producer)

    assert messages == main.BATCH_SIZE
    assert producer.flush(10) == 0
