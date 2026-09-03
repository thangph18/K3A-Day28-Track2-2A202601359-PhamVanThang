"""IT-J3 — a new release takes over serving, and a rollback takes it back.

A registry that only stores versions is a filing cabinet. What makes it part of
the platform is that moving one alias changes what the running service answers
with, without a deployment, and that the move is reversible in the same way.

These tests run in file order on purpose: promotion has to be observed before
the rollback undoes it. The dependency is expressed through fixtures rather than
left implicit, and the module restores the champion it found on the way in — a
suite that leaves someone else's registry pointing somewhere new is a suite
nobody will run twice.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
import stack

from lab28_platform.model_registry import (
    TAG_COLLECTION,
    TAG_DELTA_VERSION,
    TAG_EMBEDDING_MODEL,
    TAG_FEATURE_SERVICE,
    TAG_PROMPT_VERSION,
    TAG_VLLM_MODEL,
    Release,
    ReleaseRegistry,
    ReleaseSpec,
)
from lab28_platform.settings import Settings

pytestmark = [pytest.mark.integration, pytest.mark.matrix("IT-J3-promotion-rollback")]

PROMPT_TEMPLATE = (
    "Bạn là trợ lý hỗ trợ khách hàng. Chỉ trả lời dựa trên ngữ cảnh.\n\n"
    "Ngữ cảnh:\n{context}\n\nCâu hỏi: {question}\nTrả lời:"
)


@dataclass
class Promotion:
    registry: ReleaseRegistry
    previous_version: str
    release: Release
    prompt_version: str


@pytest.fixture(scope="module")
def registry(settings: Settings) -> ReleaseRegistry:
    return ReleaseRegistry(settings.mlflow)


@pytest.fixture(scope="module")
def mlflow_client(settings: Settings) -> Any:
    """A bare registry client, for the tags and version list the wrapper does not expose."""
    from mlflow.tracking import MlflowClient

    return MlflowClient(tracking_uri=settings.mlflow.tracking_uri)


@pytest.fixture(scope="module")
def promoted(
    settings: Settings, registry: ReleaseRegistry
) -> Iterator[Promotion]:
    """Register a new release, promote it, and restore the original afterwards."""
    from lab28_platform import delta_store

    previous_version = registry.current_version()
    prompt_version = f"it-j3-{stack.run_id()}"
    try:
        delta_version: int | None = delta_store.current_version(settings.feedback_table)
    except Exception:
        delta_version = None

    release = registry.register(
        ReleaseSpec(
            prompt_version=prompt_version,
            prompt_template=PROMPT_TEMPLATE,
            vllm_model_id=settings.vllm.model_id,
            embedding_model_id=settings.qdrant.embedding_model_id,
            qdrant_collection=settings.qdrant.collection,
            feature_service="asker_serving_v1",
            top_k=3,
            delta_version=delta_version,
            evaluation={"groundedness": 0.83, "latency_p95_ms": 640.0},
        ),
        promote=True,
    )

    yield Promotion(
        registry=registry,
        previous_version=previous_version,
        release=release,
        prompt_version=prompt_version,
    )

    if registry.promoted_version() != previous_version:
        registry.promote(previous_version)


@pytest.fixture(scope="module")
def rolled_back(promoted: Promotion) -> Release:
    """Roll the alias back — the operator action IT-J3 exists to prove."""
    return promoted.registry.rollback()


def _ask(gateway: httpx.Client, question: str) -> dict[str, Any]:
    response = gateway.post(
        "/api/v1/ask",
        json={"asker_id": "it-j3-observer", "question": question, "top_k": 3},
        timeout=90.0,
    )
    assert response.status_code == 200, response.text
    return dict(response.json())


# -- IP06: registration -------------------------------------------------------


def test_registering_a_release_creates_a_new_version(promoted: Promotion) -> None:
    assert int(promoted.release.version) > int(promoted.previous_version)
    assert promoted.release.run_id, "a version with no run cannot be reproduced"


def test_the_version_carries_the_provenance_an_incident_review_needs(
    settings: Settings, promoted: Promotion, mlflow_client: Any
) -> None:
    """Which prompt, which model, which data — answerable from the version alone."""
    import mlflow

    entry = mlflow_client.get_model_version(
        settings.mlflow.model_name, promoted.release.version
    )
    model_info = mlflow.models.get_model_info(entry.source)

    assert entry.tags[TAG_PROMPT_VERSION] == promoted.prompt_version
    assert entry.tags[TAG_VLLM_MODEL] == settings.vllm.model_id
    assert entry.tags[TAG_EMBEDDING_MODEL] == settings.qdrant.embedding_model_id
    assert entry.tags[TAG_COLLECTION] == settings.qdrant.collection
    assert entry.tags[TAG_FEATURE_SERVICE] == "asker_serving_v1"
    assert TAG_DELTA_VERSION in entry.tags

    stack.write_evidence(
        "ip06-mlflow-release.json",
        {
            "model_name": settings.mlflow.model_name,
            "version": promoted.release.version,
            "run_id": promoted.release.run_id,
            "alias": settings.mlflow.alias,
            "registry_status": entry.status,
            "model_source": entry.source,
            "artifact_uri": model_info.artifact_path,
            "signature": model_info.signature.to_dict() if model_info.signature else None,
            "tags": dict(entry.tags),
            "promoted_from": promoted.previous_version,
        },
    )


def test_the_champion_alias_moved_to_the_new_version(promoted: Promotion) -> None:
    assert promoted.registry.current_version() == promoted.release.version


def test_the_resolved_release_is_the_one_serving_will_load(promoted: Promotion) -> None:
    resolved = promoted.registry.resolve()

    assert resolved.version == promoted.release.version
    assert resolved.prompt_version == promoted.prompt_version
    assert resolved.prompt_template == PROMPT_TEMPLATE


# -- IP07: serving follows the alias -----------------------------------------


@pytest.mark.gpu
def test_serving_switches_release_without_a_restart(
    gateway: httpx.Client, promoted: Promotion
) -> None:
    """The alias is read per request; that is what makes promotion an operation."""
    answer = _ask(gateway, "Nền tảng dữ liệu của lab này gồm những thành phần nào?")

    assert answer["evidence"]["mlflow_release_version"] == promoted.release.version
    assert answer["evidence"]["mlflow_run_id"] == promoted.release.run_id


@pytest.mark.gpu
def test_the_serving_process_reports_the_release_it_is_using(
    settings: Settings, gateway: httpx.Client, promoted: Promotion
) -> None:
    """One gauge, one series: a dashboard must never show two current releases."""
    _ask(gateway, "Kho dữ liệu nào lưu phản hồi khách hàng?")
    exposition = stack.scrape(f"{settings.api_url}/metrics")

    series = [
        line
        for line in exposition.splitlines()
        if line.startswith("lab28_release_version_info{")
    ]

    assert len(series) == 1, f"expected exactly one release series, got {series}"
    assert f'version="{promoted.release.version}"' in series[0]


# -- IP06: rollback -----------------------------------------------------------


def test_rollback_moves_the_alias_to_the_previous_version(
    settings: Settings, promoted: Promotion, rolled_back: Release, mlflow_client: Any
) -> None:
    """Rollback targets the highest version below the current one, by definition."""
    below = [
        int(entry.version)
        for entry in mlflow_client.search_model_versions(
            f"name='{settings.mlflow.model_name}'"
        )
        if int(entry.version) < int(promoted.release.version)
    ]

    assert rolled_back.version == str(max(below))
    assert promoted.registry.current_version() == rolled_back.version


def test_the_rolled_back_release_still_resolves_completely(
    promoted: Promotion, rolled_back: Release
) -> None:
    """A rollback target that cannot be resolved is not a rollback, it is an outage."""
    resolved = promoted.registry.resolve()

    assert resolved.version == rolled_back.version
    assert resolved.prompt_template
    assert resolved.vllm_model_id

    stack.write_evidence(
        "journey-j3-promotion-rollback.json",
        {
            "journey": "IT-J3-promotion-rollback",
            "model_name": resolved.name,
            "alias": resolved.alias,
            "starting_version": promoted.previous_version,
            "promoted_version": promoted.release.version,
            "promoted_run_id": promoted.release.run_id,
            "rolled_back_version": rolled_back.version,
            "resolved_after_rollback": resolved.to_dict(),
            "assertion": "the alias moved to a new release and resolved after rollback",
        },
    )


@pytest.mark.gpu
def test_serving_follows_the_rollback(
    gateway: httpx.Client, rolled_back: Release
) -> None:
    answer = _ask(gateway, "Nền tảng dữ liệu của lab này gồm những thành phần nào?")

    assert answer["evidence"]["mlflow_release_version"] == rolled_back.version
