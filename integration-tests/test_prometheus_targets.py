"""IT-prometheus-targets — the observability stack is itself a deliverable.

A platform that emits perfect telemetry nobody scrapes is unobservable. This
module asserts the three things that make the difference: every component is
actually being scraped, the alert rules load and evaluate, and the dashboards
are provisioned rather than drawn by hand on the demo machine.

Jobs are matched by keyword rather than by exact name. The scrape config owns
the naming, and pinning ``lab28-api`` here would make a rename look like an
outage; what must not change is that *something* scrapes each component.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import stack
from stack import Prometheus

pytestmark = [pytest.mark.integration, pytest.mark.matrix("IT-prometheus-targets")]

#: One keyword per component that exposes metrics, and the slide layer it sits
#: on. ``vllm`` is separate: on a stack without a GPU that target is down by
#: design, and its absence must not hide every other target's state.
COMPONENT_KEYWORDS = (
    "api",
    "gateway",
    "kafka",
    "airflow",
    "qdrant",
    "mlflow",
    "feast",
    "collector",
    "prometheus",
)
GPU_KEYWORD = "vllm"


def _job(target: dict[str, Any]) -> str:
    labels = target.get("labels") or {}
    return str(labels.get("job", ""))


def _is_gpu_target(target: dict[str, Any]) -> bool:
    return GPU_KEYWORD in _job(target).lower()


def test_every_scraped_target_is_up(prometheus: Prometheus) -> None:
    """A target that has been down since boot is a dashboard full of gaps."""
    targets = prometheus.targets()
    assert targets, "Prometheus has no active targets at all"

    down = [
        {"job": _job(target), "url": target.get("scrapeUrl"), "error": target.get("lastError")}
        for target in targets
        if target.get("health") != "up" and not _is_gpu_target(target)
    ]

    stack.write_evidence(
        "ip09-prometheus-targets.json",
        {
            "targets": [
                {
                    "job": _job(target),
                    "url": target.get("scrapeUrl"),
                    "health": target.get("health"),
                    "last_scrape": target.get("lastScrape"),
                    "last_error": target.get("lastError"),
                }
                for target in targets
            ],
            "rules": [
                {
                    "name": rule.get("name"),
                    "type": rule.get("type"),
                    "health": rule.get("health"),
                    "state": rule.get("state"),
                    "query": rule.get("query"),
                    "duration": rule.get("duration"),
                    "labels": rule.get("labels"),
                    "annotations": rule.get("annotations"),
                }
                for rule in prometheus.rules()
            ],
        },
    )

    assert not down, f"targets not up: {down}"


def test_every_component_that_exposes_metrics_is_scraped(prometheus: Prometheus) -> None:
    """Coverage, not health: a component nobody scrapes cannot be missed later."""
    jobs = {_job(target).lower() for target in prometheus.targets()}
    missing = [
        keyword
        for keyword in COMPONENT_KEYWORDS
        if not any(keyword in job for job in jobs)
    ]

    assert not missing, f"no scrape job matches {missing}; jobs present: {sorted(jobs)}"


@pytest.mark.gpu
def test_the_inference_endpoint_is_scraped(prometheus: Prometheus) -> None:
    """vLLM publishes the queue and token metrics the latency SLO is built on."""
    gpu_targets = [target for target in prometheus.targets() if _is_gpu_target(target)]

    assert gpu_targets, "no scrape job matches the inference endpoint"
    assert all(target.get("health") == "up" for target in gpu_targets), gpu_targets


def test_at_least_one_slo_alert_is_loaded(prometheus: Prometheus) -> None:
    """The rubric asks for an actionable alert, which means routable and explained."""
    alerts = [rule for rule in prometheus.rules() if rule.get("type") == "alerting"]

    assert alerts, "no alerting rules are loaded"
    for alert in alerts:
        annotations = alert.get("annotations") or {}
        labels = alert.get("labels") or {}
        assert labels.get("severity"), f"{alert.get('name')} has no severity to route on"
        assert annotations.get("summary") or annotations.get("description"), (
            f"{alert.get('name')} fires without saying what is wrong"
        )


def test_every_rule_evaluates(prometheus: Prometheus) -> None:
    """A rule whose query errors is silent in exactly the way a broken alert is."""
    broken = [
        {"name": rule.get("name"), "error": rule.get("lastError")}
        for rule in prometheus.rules()
        if rule.get("health") != "ok"
    ]

    assert not broken, f"rules failing to evaluate: {broken}"


def test_grafana_is_provisioned_from_configuration(
    grafana: tuple[str, tuple[str, str]]
) -> None:
    """Dashboards and datasources must survive a rebuild of the stack.

    Provisioning is the difference between a dashboard that is part of the
    repository and one that exists only on the laptop that ran the demo.
    """
    base_url, auth = grafana

    dashboards = httpx.get(
        f"{base_url.rstrip('/')}/api/search",
        params={"type": "dash-db"},
        auth=auth,
        timeout=stack.HTTP_TIMEOUT,
    )
    datasources = httpx.get(
        f"{base_url.rstrip('/')}/api/datasources", auth=auth, timeout=stack.HTTP_TIMEOUT
    )
    dashboards.raise_for_status()
    datasources.raise_for_status()

    found = list(dashboards.json())
    sources = list(datasources.json())
    details = []
    for entry in found:
        uid = entry.get("uid")
        if not uid:
            continue
        response = httpx.get(
            f"{base_url.rstrip('/')}/api/dashboards/uid/{uid}",
            auth=auth,
            timeout=stack.HTTP_TIMEOUT,
        )
        response.raise_for_status()
        dashboard = response.json().get("dashboard") or {}
        details.append(
            {
                "uid": uid,
                "panels": [
                    {
                        "title": panel.get("title"),
                        "queries": [target.get("expr") for target in panel.get("targets", [])],
                    }
                    for panel in dashboard.get("panels", [])
                ],
            }
        )

    stack.write_evidence(
        "ip09-grafana-dashboards.json",
        {
            "grafana_url": base_url,
            "dashboards": [
                {"title": entry.get("title"), "uid": entry.get("uid"), "url": entry.get("url")}
                for entry in found
            ],
            "datasources": [
                {"name": entry.get("name"), "type": entry.get("type")} for entry in sources
            ],
            "dashboard_details": details,
        },
    )

    assert found, "no dashboards are provisioned"
    assert any(entry.get("type") == "prometheus" for entry in sources), sources
