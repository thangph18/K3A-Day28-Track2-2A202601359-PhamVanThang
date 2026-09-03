"""IT-gateway-rate-limit — the gateway is a policy layer, not a reverse proxy.

Everything asserted here is a property of the edge that the application behind
it cannot provide for itself: a burst is refused before it costs the API
anything, the refusal is still correlatable, the limiter refills instead of
latching, and the gateway's own health route is answered without waking the
upstream at all.

The burst is aimed at ``/health``, the one route that is documented to touch
nothing. That keeps the measurement about the limiter rather than about how fast
the application can serve real work, and it leaves no data behind.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx
import pytest
import stack

from lab28_platform.settings import Settings

pytestmark = [pytest.mark.integration, pytest.mark.matrix("IT-gateway-rate-limit")]

#: Requests per second the gateway is configured to allow. Overridable so the
#: test follows the Envoy config instead of duplicating a magic number.
RATE_LIMIT_RPS = int(stack.env("LAB28_GATEWAY_RATE_LIMIT_RPS", "10"))
BURST = RATE_LIMIT_RPS * 3
PROBE_ROUTE = "/health"


@dataclass
class Burst:
    statuses: list[int] = field(default_factory=list)
    accepted: httpx.Response | None = None
    rejected: httpx.Response | None = None

    @property
    def rejections(self) -> int:
        return sum(1 for status in self.statuses if status == 429)


@pytest.fixture(scope="module")
def burst(gateway: httpx.Client) -> Burst:
    """Send more requests than the limit allows, as fast as the loop can."""
    result = Burst()
    for _ in range(BURST):
        response = gateway.get(PROBE_ROUTE)
        result.statuses.append(response.status_code)
        if response.status_code == 200 and result.accepted is None:
            result.accepted = response
        if response.status_code == 429 and result.rejected is None:
            result.rejected = response
    return result


def test_the_burst_is_refused_at_the_edge(burst: Burst) -> None:
    """Without this, one misbehaving client is an availability incident."""
    assert burst.accepted is not None, f"nothing got through: {burst.statuses}"
    assert burst.rejections >= 1, (
        f"{BURST} requests against a {RATE_LIMIT_RPS}/s limit produced no 429: {burst.statuses}"
    )


def test_a_refusal_is_still_a_correlatable_response(burst: Burst) -> None:
    """A 429 with no request id is a support ticket nobody can answer."""
    assert burst.rejected is not None
    assert burst.rejected.headers.get("x-request-id"), dict(burst.rejected.headers)


def test_the_bucket_refills(gateway: httpx.Client, burst: Burst) -> None:
    """A rate limiter that does not recover is an outage with extra steps."""
    recovered = stack.wait_until(
        "the rate limiter to admit traffic again",
        lambda: gateway.get(PROBE_ROUTE).status_code == 200,
        timeout=30.0,
        interval=1.0,
    )

    assert recovered


def test_the_gateway_counts_what_it_refused(
    gateway: httpx.Client, gateway_admin: str, burst: Burst, settings: Settings
) -> None:
    """The rejections have to be visible where the gateway's own stats are read."""
    exposition = stack.scrape(f"{gateway_admin.rstrip('/')}/stats/prometheus")
    rate_limited = stack.metric_total(exposition, "envoy_http_local_rate_limit_rate_limited")
    before = _health_requests(settings)
    gateway_health = gateway.get("/healthz")
    after = _health_requests(settings)
    recovered = gateway.get(PROBE_ROUTE)

    assert rate_limited > 0, "the limiter fired but published nothing to alert on"
    assert gateway_health.status_code == 200
    assert after == before, "the gateway's health route reached the application"
    assert recovered.status_code == 200, "the rate-limit bucket did not refill"

    stack.write_evidence(
        "ip08-gateway.json",
        {
            "gateway_url": settings.gateway_url,
            "route": PROBE_ROUTE,
            "configured_rps": RATE_LIMIT_RPS,
            "requests_sent": BURST,
            "accepted": sum(1 for status in burst.statuses if status == 200),
            "rejected": burst.rejections,
            "rate_limited_stat": rate_limited,
            "bucket_refilled": recovered.status_code == 200,
            "gateway_healthz": {
                "status": gateway_health.status_code,
                "served_without_upstream": after == before,
            },
            "sample_200": {
                "status": burst.accepted.status_code if burst.accepted else None,
                "x-request-id": (
                    burst.accepted.headers.get("x-request-id") if burst.accepted else None
                ),
            },
            "sample_429": {
                "status": burst.rejected.status_code if burst.rejected else None,
                "x-request-id": (
                    burst.rejected.headers.get("x-request-id") if burst.rejected else None
                ),
            },
        },
    )


def _health_requests(settings: Settings) -> float:
    """Requests the API has served on a health-ish route.

    The label value is deliberately left unterminated: it matches ``/health``
    and ``/healthz`` alike, so a gateway that proxies its own health route is
    caught whether the app answers it or 404s it. Counting *all* routes would
    not work — Prometheus scrapes ``/metrics`` on its own schedule and would
    move the total between the two readings.
    """
    exposition = stack.scrape(f"{settings.api_url}/metrics")
    return stack.metric_total(exposition, 'lab28_requests_total{route="/health')


def test_the_gateway_answers_its_own_health_route(
    settings: Settings, gateway: httpx.Client
) -> None:
    """``/healthz`` is the gateway's liveness, so it must not depend on the app.

    A health route that is proxied reports the upstream's health, which means an
    orchestrator restarts the gateway for someone else's outage.
    """
    before = _health_requests(settings)
    response = gateway.get("/healthz")
    after = _health_requests(settings)

    assert response.status_code == 200
    assert after == before, "the gateway's health route reached the application"
