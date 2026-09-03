"""IT-J2 — the same fact delivered three times still exists once.

At-least-once delivery is not a bug to be avoided, it is the guarantee the whole
pipeline is built on: a producer that retries, a consumer that redelivers after
a crash, an operator who replays a topic. What must not happen is a customer
appearing twice in the lakehouse, being counted twice by a feature, or having
their document returned twice as two separate sources.

This journey exercises both de-duplication layers, because they fail
independently:

*Within one batch* — two copies in the same poll. A Delta ``MERGE`` raises when
two source rows match one target row, so the batch is collapsed before the write
(the pure logic for that is pinned by UT-delta-merge-idempotency).

*Across batches* — a copy that arrives after the row already exists. Here the
MERGE itself has to match and update rather than insert, which only a live table
can prove.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import pytest
import stack
from stack import Airflow

from lab28_platform.contracts import stable_point_id
from lab28_platform.settings import Settings

pytestmark = [pytest.mark.integration, pytest.mark.matrix("IT-J2-idempotent-replay")]


@dataclass
class Replay:
    asker_id: str
    doc_id: str
    accepted: list[dict[str, Any]]
    first_run: dict[str, Any]
    second_run: dict[str, Any]
    rows_after_first: list[dict[str, Any]]
    version_after_first: int

    @property
    def idempotency_key(self) -> str:
        return str(self.accepted[0]["idempotency_key"])


@pytest.fixture(scope="module")
def replay(settings: Settings, gateway: httpx.Client, airflow: Airflow) -> Replay:
    """Submit the same fact twice, process it, submit it again, process again."""
    from lab28_platform import delta_store

    suffix = stack.run_id()
    asker_id = f"it-j2-{suffix}"
    doc_id = f"it-j2-doc-{suffix}"
    feedback = {
        "asker_id": asker_id,
        "text": f"Phản hồi lặp lại cho bài kiểm thử {suffix}.",
        "rating": 4,
        "label": "positive",
    }
    document = {
        "doc_id": doc_id,
        "title": f"Tài liệu lặp lại {suffix}",
        "text": (
            f"Tài liệu {suffix} được gửi nhiều lần để chứng minh việc lập chỉ mục "
            "là idempotent trong kho vector."
        ),
    }

    accepted = [
        gateway.post("/api/v1/feedback", json=feedback).json(),
        gateway.post("/api/v1/feedback", json=feedback).json(),
    ]
    gateway.post("/api/v1/documents", json=document)
    gateway.post("/api/v1/documents", json=document)

    first_run = airflow.wait_for_run(airflow.trigger(note=f"IT-J2 first pass {suffix}"))
    rows_after_first = [
        row
        for row in delta_store.read_rows(settings.feedback_table)
        if row.get("asker_id") == asker_id
    ]
    version_after_first = delta_store.current_version(settings.feedback_table)

    # The redelivery that only a live MERGE can absorb: the row already exists.
    accepted.append(gateway.post("/api/v1/feedback", json=feedback).json())
    gateway.post("/api/v1/documents", json=document)
    second_run = airflow.wait_for_run(airflow.trigger(note=f"IT-J2 replay {suffix}"))

    return Replay(
        asker_id=asker_id,
        doc_id=doc_id,
        accepted=accepted,
        first_run=first_run,
        second_run=second_run,
        rows_after_first=rows_after_first,
        version_after_first=version_after_first,
    )


# -- IP01: identity at the edge ----------------------------------------------


def test_repeated_submissions_share_one_idempotency_key(replay: Replay) -> None:
    """The client sent no key, so the platform derives one from the content."""
    keys = {response["idempotency_key"] for response in replay.accepted}

    assert len(keys) == 1, f"the same fact produced several keys: {keys}"


def test_each_delivery_is_still_its_own_event(replay: Replay) -> None:
    """Distinct event ids are what make the duplicates visible in a trace."""
    event_ids = {response["event_id"] for response in replay.accepted}

    assert len(event_ids) == len(replay.accepted)


def test_the_broker_kept_every_delivery(settings: Settings, replay: Replay) -> None:
    """Kafka must not silently drop duplicates — de-duplication is a data decision.

    Asserting this is what stops the suite from passing for the wrong reason:
    one row downstream is only meaningful if three messages went upstream.
    """
    records = stack.read_topic(settings.kafka.bootstrap_servers, settings.kafka.topic_raw)
    mine = [
        record
        for record in records
        if record.value and record.value.get("idempotency_key") == replay.idempotency_key
    ]

    assert len(mine) == 3, f"expected three deliveries on the topic, found {len(mine)}"
    assert {record.key for record in mine} == {replay.asker_id}, "one entity, one partition key"


# -- IP02: both runs are healthy ---------------------------------------------


def test_both_pipeline_runs_succeeded(replay: Replay) -> None:
    assert replay.first_run["state"] == "success", replay.first_run
    assert replay.second_run["state"] == "success", replay.second_run


# -- IP03: the lakehouse ------------------------------------------------------


def test_the_first_run_collapsed_the_duplicate_batch(replay: Replay) -> None:
    assert len(replay.rows_after_first) == 1, replay.rows_after_first


def test_a_later_delivery_merges_into_the_same_row(
    settings: Settings, replay: Replay
) -> None:
    from lab28_platform import delta_store

    rows = [
        row
        for row in delta_store.read_rows(settings.feedback_table)
        if row.get("asker_id") == replay.asker_id
    ]

    assert len(rows) == 1, f"the replay inserted instead of merging: {len(rows)} rows"
    assert rows[0]["idempotency_key"] == replay.idempotency_key
    assert rows[0]["rating"] == replay.rows_after_first[0]["rating"]


def test_the_table_version_may_advance_even_when_the_row_does_not(
    settings: Settings, replay: Replay
) -> None:
    """Idempotency is a statement about rows, not about the transaction log.

    A MERGE that matches and rewrites the same values still commits a version.
    Asserting version *stability* here would be asserting the wrong invariant and
    would break the moment Delta changed its commit behaviour.
    """
    from lab28_platform import delta_store

    assert delta_store.current_version(settings.feedback_table) >= replay.version_after_first


# -- IP04: the feature store --------------------------------------------------


def test_the_feature_row_counts_the_fact_once(settings: Settings, replay: Replay) -> None:
    """Double counting here is how a duplicate silently becomes a wrong answer."""
    from lab28_platform.feature_store import FeatureClient

    client = FeatureClient(settings.feast)
    try:
        lookup = stack.wait_until(
            f"Feast to serve features for {replay.asker_id}",
            lambda: (
                found
                if (found := client.get_asker_features(replay.asker_id)).features.feedback_count
                else None
            ),
            timeout=120.0,
            interval=3.0,
        )
    finally:
        client.close()

    assert lookup.features.feedback_count == 1, lookup.features.model_dump()


# -- IP05: the vector store ---------------------------------------------------


def test_the_vector_store_holds_exactly_one_point_for_the_document(
    settings: Settings, replay: Replay
) -> None:
    """Three indexing passes, one point — otherwise retrieval returns the same source thrice."""
    count = stack.wait_until(
        f"Qdrant to index {replay.doc_id}",
        lambda: stack.qdrant_count(
            settings.qdrant.url, settings.qdrant.collection, doc_id=replay.doc_id
        ),
        timeout=120.0,
        interval=3.0,
    )

    assert count == 1
    assert stack.qdrant_point(
        settings.qdrant.url, settings.qdrant.collection, stable_point_id(replay.doc_id)
    ) is not None


def test_replay_proof_is_recorded(settings: Settings, replay: Replay) -> None:
    """Persist the asserted before/after values used for the submission."""
    from lab28_platform import delta_store

    rows_after_replay = [
        row
        for row in delta_store.read_rows(settings.feedback_table)
        if row.get("asker_id") == replay.asker_id
    ]
    records = stack.read_topic(settings.kafka.bootstrap_servers, settings.kafka.topic_raw)
    deliveries = [
        record
        for record in records
        if record.value and record.value.get("idempotency_key") == replay.idempotency_key
    ]
    stack.write_evidence(
        "journey-j2-idempotent-replay.json",
        {
            "journey": "IT-J2-idempotent-replay",
            "asker_id": replay.asker_id,
            "doc_id": replay.doc_id,
            "idempotency_key": replay.idempotency_key,
            "distinct_event_ids": sorted(response["event_id"] for response in replay.accepted),
            "kafka_delivery_count": len(deliveries),
            "delta_rows_after_first_run": len(replay.rows_after_first),
            "delta_rows_after_replay": len(rows_after_replay),
            "delta_version_after_first_run": replay.version_after_first,
            "delta_version_after_replay": delta_store.current_version(settings.feedback_table),
            "first_dag_run_id": replay.first_run["dag_run_id"],
            "second_dag_run_id": replay.second_run["dag_run_id"],
            "qdrant_points_for_document": stack.qdrant_count(
                settings.qdrant.url, settings.qdrant.collection, doc_id=replay.doc_id
            ),
            "assertion": "three Kafka deliveries produced one Delta row and one Qdrant point",
        },
    )
