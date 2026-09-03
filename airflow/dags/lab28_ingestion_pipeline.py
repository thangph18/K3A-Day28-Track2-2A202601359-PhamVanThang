"""Kafka → Delta → Feast → Qdrant, orchestrated by Airflow 3 (IP02).

This DAG is the seam the slide calls *pipeline orchestration*, and it is written
to make three things visible rather than merely true.

**Offsets move last.** The first task polls Kafka, merges into Delta and only
then commits. Kill the worker anywhere before that commit and the same events
are redelivered, which — combined with the MERGE in ``delta_store`` — is why a
crash mid-run costs a repeat, not a gap and not a duplicate.

**Assets, not a schedule, express what changed.** Each task declares the asset
it produces, so ``lab28://delta/feedback`` appearing in the asset-event log is
evidence that this run wrote that table. A downstream DAG can be scheduled on
that asset without knowing this DAG's name.

**The trace is carried, not restarted.** The run's ``conf`` may contain a
``traceparent`` from whoever triggered it; every span here is a child of it, so
one HTTP request submitted at the gateway and this pipeline run appear in one
trace.

``schedule=None`` is deliberate. A cron would race the live tests and the demo
for the same Kafka batch — whoever polled first would win, and the other would
see an empty run it did not cause. In production this DAG would be triggered by
a Kafka sensor or a short schedule; in the lab it is triggered explicitly, by
the test, the CLI or a facilitator clicking Trigger.
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.sdk import Asset, dag, task

# The repository is bind-mounted into the Airflow image; ``spark/`` is a plain
# directory of job modules rather than an installed package, because it is
# imported by exactly two callers (this DAG and a shell) and publishing it as a
# distribution would only add a build step to change one SQL statement.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))
if str(REPO_ROOT / "spark") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "spark"))

logger = logging.getLogger(__name__)

DAG_ID = "lab28_ingestion_pipeline"

#: Assets are named for the *logical* dataset, not the storage path. The path
#: is configuration and changes per environment; ``lab28://delta/feedback`` is
#: the contract other DAGs and the live tests schedule against.
FEEDBACK_ASSET = Asset("lab28://delta/feedback")
DOCUMENTS_ASSET = Asset("lab28://delta/documents")
FEATURES_ASSET = Asset("lab28://feast/asker_activity")
VECTORS_ASSET = Asset("lab28://qdrant/lab28_documents")

#: How many events one run drains. Bounded so a backlog produces several
#: observable runs instead of one opaque hour-long task.
MAX_MESSAGES = 500


def _sanitised_run_id(raw: str) -> str:
    """Airflow run ids are free-form; ``contracts.Identifier`` is not.

    A manual run id looks like ``manual__2026-09-03T01:23:45+00:00``. The ``+``
    and the offset colon are outside the identifier charset, and silently
    dropping the event because of it would lose the only link between the DAG
    run and the ``data.processed`` message it produced.
    """
    return re.sub(r"[^A-Za-z0-9._:-]", "-", raw)[:128]


def _traceparent(context: dict[str, Any]) -> str | None:
    conf = getattr(context.get("dag_run"), "conf", None) or {}
    value = conf.get("traceparent")
    return str(value) if value else None


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time: two runs sharing a consumer group would split the same
    # batch across two Delta commits and two asset events, and the live tests
    # could no longer say which run wrote their row.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=20)},
    tags=["lab28", "ingestion", "IP02"],
    doc_md=__doc__,
)
def lab28_ingestion_pipeline() -> None:
    @task(outlets=[FEEDBACK_ASSET, DOCUMENTS_ASSET])
    def drain_kafka_into_delta(**context: Any) -> dict[str, Any]:
        """Poll ``data.raw``, MERGE into Delta, then commit the offsets.

        The Spark merge runs *inside* the durable-processing helper, so a
        dependency outage propagates as an exception and the offsets stay put.
        """
        import delta_merge

        from lab28_platform import metrics, telemetry
        from lab28_platform.contracts import IngestionEvent
        from lab28_platform.event_bus import (
            BatchConsumer,
            EventPublisher,
            process_batch_then_commit,
        )
        from lab28_platform.settings import Settings, TelemetrySettings

        telemetry.configure_telemetry(TelemetrySettings.from_env("lab28-airflow"))
        settings = Settings.from_env()
        traceparent = _traceparent(context)
        tables = {
            "feedback": settings.feedback_table,
            "documents": settings.document_table,
        }

        merged: dict[str, Any] = {}
        spark = delta_merge.connect(settings.spark_remote)
        consumer = BatchConsumer(settings.kafka)
        publisher = EventPublisher(settings.kafka)

        def write(events: list[IngestionEvent]) -> int:
            result = delta_merge.merge_events(
                spark, tables, events, traceparent=traceparent
            )
            merged.update(result.to_dict())
            return result.total_rows

        with telemetry.span(
            telemetry.SPAN_AIRFLOW_DAG,
            parent=telemetry.context_from_traceparent(traceparent),
            attributes={
                "lab28.airflow.dag_id": DAG_ID,
                "lab28.airflow.run_id": str(context["run_id"]),
            },
        ) as active:
            try:
                outcome = process_batch_then_commit(
                    consumer, publisher, write, max_messages=MAX_MESSAGES
                )
            finally:
                consumer.close()
                publisher.close()
                spark.stop()

            active.set_attribute("lab28.batch.polled", outcome["polled"])
            active.set_attribute("lab28.batch.dead_lettered", outcome["dead_lettered"])

        metrics.PIPELINE_BATCHES.labels(
            outcome="empty" if not outcome["polled"] else "processed"
        ).inc()
        logger.info("drained %s: %s", DAG_ID, outcome)

        return {
            "run_id": _sanitised_run_id(str(context["run_id"])),
            "traceparent": traceparent,
            **outcome,
            **merged,
        }

    @task(outlets=[FEATURES_ASSET])
    def refresh_online_features(batch: dict[str, Any]) -> dict[str, Any]:
        """Recompute the offline snapshot, then push it to the online store.

        Two steps on purpose. Spark owns the computation because it reads Delta
        at a pinned version; Feast owns the online write because the materialize
        call is the same HTTP boundary the serving path reads through, so a
        broken feature server fails here rather than at request time.
        """
        import delta_merge
        import feature_export

        from lab28_platform.feature_store import FeatureClient
        from lab28_platform.settings import Settings

        version = (batch.get("versions") or {}).get("feedback")
        if version is None:
            logger.info("no feedback rows in this batch; online features unchanged")
            return {"skipped": "no feedback rows merged"}

        settings = Settings.from_env()
        spark = delta_merge.connect(settings.spark_remote, app_name="lab28-feature-export")
        try:
            export = feature_export.export_asker_features(
                spark, settings.feedback_table, settings.asker_features_path, version
            )
        finally:
            spark.stop()

        client = FeatureClient(settings.feast)
        try:
            materialized = client.materialize_incremental()
        finally:
            client.close()

        return {**export.to_dict(), "materialized": materialized}

    @task(outlets=[VECTORS_ASSET])
    def index_new_documents(batch: dict[str, Any]) -> dict[str, Any]:
        """Embed the documents this batch merged and upsert them into Qdrant.

        The rows are re-read from Delta rather than passed through XCom. XCom is
        a database table: putting document text in it would copy user content
        into a second store with different retention, for no gain — the
        lakehouse already holds the authoritative row.
        """
        from lab28_platform import delta_store
        from lab28_platform.settings import Settings
        from lab28_platform.vector_store import VectorStore, documents_from_rows

        keys = set(batch.get("idempotency_keys") or [])
        if not keys or not (batch.get("rows") or {}).get("documents"):
            logger.info("no documents in this batch; vector store unchanged")
            return {"skipped": "no document rows merged"}

        settings = Settings.from_env()
        rows = [
            row
            for row in delta_store.read_rows(settings.document_table)
            if row.get(delta_store.MERGE_KEY) in keys
        ]

        store = VectorStore(settings.qdrant)
        try:
            store.ensure_collection()
            indexed = store.index(documents_from_rows(rows))
            total = store.count()
        finally:
            store.close()

        return {"indexed": indexed, "collection_points": total}

    @task
    def announce_processed_batch(
        batch: dict[str, Any], features: dict[str, Any], vectors: dict[str, Any]
    ) -> dict[str, Any]:
        """Publish ``data.processed`` once the batch is durable everywhere.

        This event is what makes the pipeline composable: a consumer that wants
        to react to new data subscribes to it instead of polling Delta, and the
        ``delta_version`` it carries is the exact version everything downstream
        should pin its evidence to.
        """
        from lab28_platform.contracts import ProcessedBatchEvent
        from lab28_platform.event_bus import EventPublisher
        from lab28_platform.settings import Settings

        rows = batch.get("rows") or {}
        versions = batch.get("versions") or {}
        if not rows.get("feedback") and not rows.get("documents"):
            logger.info("nothing merged; not announcing an empty batch")
            return {"published": False, "reason": "empty batch"}

        settings = Settings.from_env()
        event = ProcessedBatchEvent(
            run_id=batch["run_id"],
            # The feedback table is the version the whole platform quotes:
            # features are derived from it and the serving evidence cites it.
            delta_version=versions.get("feedback", versions.get("documents", 0)),
            feedback_rows=rows.get("feedback", 0),
            document_rows=rows.get("documents", 0),
            idempotency_keys=batch.get("idempotency_keys") or [],
            entity_ids=batch.get("entity_ids") or [],
            traceparent=batch.get("traceparent"),
        )

        publisher = EventPublisher(settings.kafka)
        try:
            publisher.publish(settings.kafka.topic_processed, event.run_id, event)
        finally:
            publisher.close()

        logger.info(
            "announced %s: %s feedback / %s document rows at delta version %s "
            "(features: %s, vectors: %s)",
            event.run_id,
            event.feedback_rows,
            event.document_rows,
            event.delta_version,
            features,
            vectors,
        )
        return {"published": True, **event.model_dump(mode="json")}

    merged = drain_kafka_into_delta()
    announce_processed_batch(
        merged, refresh_online_features(merged), index_new_documents(merged)
    )


lab28_ingestion_pipeline()
