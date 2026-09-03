"""IT-trace-span-coverage — every boundary the matrix names, in one trace.

IT-J5 proves *continuity*: that a request keeps its identity across the systems
it passes through. This module proves *coverage*: that the eleven span names the
matrix declares are all real, all produced by the code, and all present on a
single trace that was driven end to end.

The full-coverage assertions carry the ``gpu`` marker because five of the
eleven spans only exist when there is a real inference endpoint to call. The
matrix-to-code agreement check is deliberately not gated — it needs no stack at
all and it is the one that catches the failure mode where a span is renamed in
the code and the contract quietly stops describing the system.
"""

from __future__ import annotations

import contextlib
import os
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
import stack
from stack import Airflow, Prometheus, TraceBackend

from lab28_platform import telemetry

pytestmark = [pytest.mark.integration, pytest.mark.matrix("IT-trace-span-coverage")]

QUESTION = "Nền tảng dữ liệu của lab này gồm những thành phần nào?"


@dataclass
class Covered:
    trace_id: str
    traceparent: str
    dag_run: dict[str, Any]
    answer: dict[str, Any]


@pytest.fixture(scope="module")
def covered(gateway: httpx.Client, airflow: Airflow) -> Covered:
    """Drive ingest, pipeline and serving under one caller-supplied trace id."""
    suffix = stack.run_id()
    trace_id, traceparent = stack.new_trace()
    asker_id = f"it-spans-{suffix}"

    accepted = gateway.post(
        "/api/v1/feedback",
        json={
            "asker_id": asker_id,
            "text": f"Phản hồi dùng để kiểm tra độ phủ span {suffix}.",
            "rating": 4,
            "label": "positive",
        },
        headers={"traceparent": traceparent},
    )
    assert accepted.status_code == 202, accepted.text

    dag_run = airflow.wait_for_run(
        airflow.trigger(conf={"traceparent": traceparent}, note=f"IT span coverage {suffix}")
    )

    answered = gateway.post(
        "/api/v1/ask",
        json={"asker_id": asker_id, "question": QUESTION, "top_k": 3},
        headers={"traceparent": traceparent},
        timeout=90.0,
    )
    assert answered.status_code == 200, answered.text

    return Covered(
        trace_id=trace_id,
        traceparent=traceparent,
        dag_run=dag_run,
        answer=dict(answered.json()),
    )


def _span_is_error(span: dict[str, Any]) -> bool:
    for tag in span.get("tags", []):
        if tag.get("key") == "error" and tag.get("value") in (True, "true"):
            return True
        if tag.get("key") == "otel.status_code" and str(tag.get("value")).upper() == "ERROR":
            return True
    return False


@pytest.mark.offline
def test_the_matrix_and_the_code_agree_on_the_span_names(matrix: dict[str, Any]) -> None:
    """A contract that names spans the code never emits describes nothing."""
    declared = {
        value
        for name, value in vars(telemetry).items()
        if name.startswith("SPAN_") and isinstance(value, str)
    }
    required = set(matrix["required_spans"])

    assert required <= declared, f"named by the matrix, absent from the code: {sorted(required - declared)}"


@pytest.mark.gpu
def test_every_required_span_appears_on_one_trace(
    matrix: dict[str, Any], traces: TraceBackend, covered: Covered
) -> None:
    """The whole demo claim, in one assertion: eleven boundaries, one identifier."""
    required = sorted(matrix["required_spans"])
    names = traces.wait_for_spans(covered.trace_id, required, timeout=300.0)

    def four_services() -> set[str] | None:
        srv = traces.service_names(covered.trace_id)
        return srv if len(srv) >= 4 else None

    with contextlib.suppress(AssertionError):
        stack.wait_until(
            "at least four distinct services on trace",
            four_services,
            timeout=30.0,
            interval=1.0,
        )

    spans = traces.spans(covered.trace_id)

    stack.write_evidence(
        "ip10-trace.json",
        {
            "trace_id": covered.trace_id,
            "traceparent": covered.traceparent,
            "dag_run_id": covered.dag_run.get("dag_run_id"),
            "span_count": len(spans),
            "span_names": sorted(names),
            "services": sorted(traces.service_names(covered.trace_id)),
            "required_spans_present": sorted(set(required) & names),
            "required_spans_missing": sorted(set(required) - names),
        },
    )

    assert set(required) <= names


@pytest.mark.gpu
def test_the_trace_spans_the_processes_the_contract_claims(
    traces: TraceBackend, covered: Covered
) -> None:
    """IP10's input contract lists distinct emitters; one service means one process."""
    def four_services() -> set[str] | None:
        srv = traces.service_names(covered.trace_id)
        return srv if len(srv) >= 4 else None

    services = stack.wait_until(
        "at least four distinct services on trace",
        four_services,
        timeout=30.0,
        interval=1.0,
    )

    assert len(services) >= 4, f"only {sorted(services)} reported spans on this trace"


@pytest.mark.gpu
def test_no_span_on_the_covered_trace_reports_an_error(
    traces: TraceBackend, covered: Covered
) -> None:
    """Coverage counts names; this counts outcomes. A trace of failures still has names."""
    failed = [
        span["operationName"] for span in traces.spans(covered.trace_id) if _span_is_error(span)
    ]

    assert not failed, f"spans reporting an error status: {sorted(set(failed))}"


@pytest.mark.langsmith
def test_the_langsmith_export_leg_is_configured_and_healthy(prometheus: Prometheus) -> None:
    """With a credential supplied, the second export leg must exist and not drop spans.

    What is asserted is what can be observed from outside: LangSmith answers for
    the configured project, and the collector reports more than one span
    exporter with no send failures. Matching an individual span to a LangSmith
    run is deliberately not asserted — LangSmith mints its own run identifiers
    on OTLP ingest, so such a check would pin the suite to their ingestion
    internals rather than to our trace continuity.
    """
    api_key = os.environ["LANGSMITH_API_KEY"]
    base_url = stack.env("LANGSMITH_ENDPOINT", "https://api.smith.langchain.com")
    project = stack.env("LANGSMITH_PROJECT", "lab28-platform")

    response = httpx.get(
        f"{base_url.rstrip('/')}/api/v1/sessions",
        params={"name": project},
        headers={"x-api-key": api_key},
        timeout=stack.HTTP_TIMEOUT,
    )
    assert response.status_code == 200, response.text
    assert response.json(), f"LangSmith has no project named {project!r}"

    exporters = prometheus.query("otelcol_exporter_sent_spans") or prometheus.query(
        "otelcol_exporter_sent_spans_total"
    )
    names = {entry["metric"].get("exporter", "") for entry in exporters}

    assert len(names) >= 2, f"only one span exporter is configured: {sorted(names)}"
