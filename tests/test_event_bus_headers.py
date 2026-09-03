"""Kafka partitioning and idempotency use related but distinct keys."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from lab28_platform import event_bus
from lab28_platform.contracts import FeedbackPayload, IngestionEvent


class RecordingProducer:
    def __init__(self) -> None:
        self.call: dict[str, Any] = {}

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.call = {"topic": topic, **kwargs}

    def flush(self, _timeout: float) -> int:
        return 0


def test_ingestion_header_uses_dedup_key_while_record_uses_partition_key(
    monkeypatch: Any,
) -> None:
    event = IngestionEvent(
        idempotency_key="fb:asker-1:content-hash",
        entity_id="asker-1",
        payload=FeedbackPayload(
            asker_id="asker-1", text="Phản hồi đủ dài để kiểm thử", rating=5
        ),
    )
    recorded: dict[str, str] = {}

    def headers(_traceparent: str | None, idempotency_key: str) -> list[tuple[str, bytes]]:
        recorded["idempotency_key"] = idempotency_key
        return [("idempotency-key", idempotency_key.encode())]

    monkeypatch.setattr(event_bus.integration_tasks, "event_headers", headers)
    producer = RecordingProducer()
    publisher = event_bus.EventPublisher.__new__(event_bus.EventPublisher)
    publisher._producer = producer
    publisher._settings = SimpleNamespace(delivery_timeout_seconds=1.0)

    publisher.publish("data.raw", event.entity_id, event)

    assert producer.call["key"] == b"asker-1"
    assert recorded["idempotency_key"] == event.idempotency_key
    assert dict(producer.call["headers"])["idempotency-key"] == event.idempotency_key.encode()
