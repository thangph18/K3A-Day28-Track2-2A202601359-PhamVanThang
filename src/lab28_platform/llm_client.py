"""Client for the vLLM OpenAI-compatible inference server (IP07).

The slide's integration point is "Model → vLLM/SGLang serving", so a mock that
returns canned strings does not satisfy it. This client can *prove* what it is
talking to: :func:`probe_identity` asks the server for evidence only a real vLLM
build produces — its own ``/version`` endpoint and a ``/metrics`` page carrying
the ``vllm:`` metric family.

When ``require_real`` is set (the default) the serving path refuses to answer
through an endpoint that fails that probe. That is deliberate: an unavailable
GPU should surface as an honestly failed gate, not as a green test.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx
from opentelemetry import propagate

from lab28_platform import metrics
from lab28_platform.settings import VLLMSettings
from lab28_platform.telemetry import SPAN_VLLM_CHAT, span

#: Metric families the vLLM OpenAI server exposes. Any one of them is enough to
#: distinguish a real vLLM process from an OpenAI-shaped proxy.
VLLM_METRIC_PREFIX = "vllm:"


class InferenceUnavailable(RuntimeError):
    """The inference endpoint is unreachable, unhealthy or not a real vLLM."""


class NotRealVLLM(InferenceUnavailable):
    """The endpoint answers OpenAI calls but cannot prove it is vLLM."""


@dataclass(frozen=True)
class VLLMIdentity:
    """Evidence that the endpoint is a genuine vLLM server.

    Serialised into ``evidence/ip07-vllm-identity.json`` for the demo.
    """

    reachable: bool
    version: str | None
    served_models: tuple[str, ...]
    vllm_metric_names: tuple[str, ...]
    detail: str

    @property
    def is_real_vllm(self) -> bool:
        """True only when the server identifies itself as vLLM *and* exposes
        vLLM's own metric family. Either signal alone is forgeable by accident;
        both together are not produced by anything but the real server."""
        return self.reachable and bool(self.version) and bool(self.vllm_metric_names)

    def to_dict(self) -> dict[str, Any]:
        return {
            "reachable": self.reachable,
            "version": self.version,
            "served_models": list(self.served_models),
            "vllm_metric_names": list(self.vllm_metric_names)[:12],
            "vllm_metric_count": len(self.vllm_metric_names),
            "is_real_vllm": self.is_real_vllm,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class Completion:
    """One chat completion plus the accounting the audit trail needs."""

    text: str
    model_id: str
    prompt_tokens: int | None
    completion_tokens: int | None
    latency_ms: float
    finish_reason: str | None


def _auth_headers(settings: VLLMSettings) -> dict[str, str]:
    """Build the auth header. The value is read at call time and never logged."""
    key = settings.api_key
    return {"Authorization": f"Bearer {key}"} if key else {}


def probe_identity(settings: VLLMSettings, *, timeout: float = 5.0) -> VLLMIdentity:
    """Collect proof-of-vLLM evidence from the configured endpoint.

    Never raises: readiness reporting needs the negative answer as much as the
    positive one.
    """
    root = settings.root_url
    headers = _auth_headers(settings)
    version: str | None = None
    models: list[str] = []
    metric_names: list[str] = []
    notes: list[str] = []

    try:
        with httpx.Client(timeout=timeout, headers=headers) as client:
            try:
                health = client.get(f"{root}/health")
                if health.status_code != 200:
                    return VLLMIdentity(
                        reachable=False,
                        version=None,
                        served_models=(),
                        vllm_metric_names=(),
                        detail=f"/health returned {health.status_code}",
                    )
            except httpx.HTTPError as error:
                return VLLMIdentity(
                    reachable=False,
                    version=None,
                    served_models=(),
                    vllm_metric_names=(),
                    detail=f"unreachable: {type(error).__name__}",
                )

            # /version is a vLLM-specific endpoint; the OpenAI API has no such route.
            try:
                response = client.get(f"{root}/version")
                if response.status_code == 200:
                    version = str(response.json().get("version") or "").strip() or None
                else:
                    notes.append(f"/version returned {response.status_code}")
            except (httpx.HTTPError, ValueError) as error:
                notes.append(f"/version failed: {type(error).__name__}")

            try:
                response = client.get(f"{settings.base_url.rstrip('/')}/models")
                if response.status_code == 200:
                    models = [item["id"] for item in response.json().get("data", [])]
                else:
                    notes.append(f"/v1/models returned {response.status_code}")
            except (httpx.HTTPError, ValueError, KeyError) as error:
                notes.append(f"/v1/models failed: {type(error).__name__}")

            try:
                response = client.get(f"{root}/metrics")
                if response.status_code == 200:
                    metric_names = _vllm_metric_names(response.text)
                else:
                    notes.append(f"/metrics returned {response.status_code}")
            except httpx.HTTPError as error:
                notes.append(f"/metrics failed: {type(error).__name__}")
    except Exception as error:
        return VLLMIdentity(
            reachable=False,
            version=None,
            served_models=(),
            vllm_metric_names=(),
            detail=f"probe error: {type(error).__name__}",
        )

    detail = "; ".join(notes) if notes else "vLLM identity confirmed"
    return VLLMIdentity(
        reachable=True,
        version=version,
        served_models=tuple(models),
        vllm_metric_names=tuple(metric_names),
        detail=detail,
    )


def _vllm_metric_names(exposition: str) -> list[str]:
    """Extract ``vllm:`` metric names from a Prometheus exposition page."""
    names: set[str] = set()
    for line in exposition.splitlines():
        if line.startswith("# HELP ") or line.startswith("# TYPE "):
            parts = line.split(maxsplit=3)
            if len(parts) >= 3 and parts[2].startswith(VLLM_METRIC_PREFIX):
                names.add(parts[2])
        elif line.startswith(VLLM_METRIC_PREFIX):
            names.add(line.split("{", 1)[0].split(maxsplit=1)[0])
    return sorted(names)


class VLLMClient:
    """Chat completions against a verified vLLM endpoint."""

    def __init__(self, settings: VLLMSettings) -> None:
        self._settings = settings
        self._client = httpx.Client(
            timeout=httpx.Timeout(settings.timeout_seconds),
            headers=_auth_headers(settings),
        )
        self._identity: VLLMIdentity | None = None

    @property
    def model_id(self) -> str:
        return self._settings.model_id

    def identity(self, *, refresh: bool = False) -> VLLMIdentity:
        """Cached identity probe. Refreshed on demand by the readiness report."""
        if self._identity is None or refresh:
            self._identity = probe_identity(self._settings)
        return self._identity

    def ensure_real(self) -> VLLMIdentity:
        """Raise unless the endpoint proves it is a real vLLM server."""
        identity = self.identity(refresh=True)
        if not identity.reachable:
            raise InferenceUnavailable(f"vLLM endpoint unreachable: {identity.detail}")
        if self._settings.require_real and not identity.is_real_vllm:
            raise NotRealVLLM(
                "endpoint did not prove it is vLLM "
                f"(version={identity.version!r}, vllm metrics={len(identity.vllm_metric_names)}); "
                f"{identity.detail}"
            )
        return identity

    def complete(self, system_prompt: str, user_prompt: str) -> Completion:
        """Run one chat completion, recording latency and token accounting."""
        payload = {
            "model": self._settings.model_id,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": self._settings.max_tokens,
            "temperature": self._settings.temperature,
        }
        url = f"{self._settings.base_url.rstrip('/')}/chat/completions"

        with span(
            SPAN_VLLM_CHAT,
            attributes={
                "gen_ai.system": "vllm",
                "gen_ai.request.model": self._settings.model_id,
                "gen_ai.request.max_tokens": self._settings.max_tokens,
            },
        ) as active:
            started = time.perf_counter()
            headers = dict(self._client.headers)
            propagate.inject(headers)
            try:
                response = self._client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                body = response.json()
            except httpx.TimeoutException as error:
                self._record_failure("timeout", started)
                raise InferenceUnavailable(
                    f"vLLM timed out after {self._settings.timeout_seconds}s"
                ) from error
            except httpx.HTTPStatusError as error:
                self._record_failure("http_error", started)
                raise InferenceUnavailable(
                    f"vLLM returned {error.response.status_code}"
                ) from error
            except httpx.HTTPError as error:
                self._record_failure("unreachable", started)
                raise InferenceUnavailable(f"vLLM unreachable: {type(error).__name__}") from error

            elapsed_ms = (time.perf_counter() - started) * 1000
            try:
                choice = body["choices"][0]
                text = choice["message"]["content"] or ""
                finish_reason = choice.get("finish_reason")
            except (KeyError, IndexError, TypeError) as error:
                self._record_failure("malformed", started)
                raise InferenceUnavailable(
                    "vLLM response did not match the OpenAI schema"
                ) from error

            usage = body.get("usage") or {}
            prompt_tokens = usage.get("prompt_tokens")
            completion_tokens = usage.get("completion_tokens")
            served_model = body.get("model") or self._settings.model_id

            metrics.LLM_SECONDS.labels(model_id=served_model, outcome="ok").observe(
                elapsed_ms / 1000
            )
            if prompt_tokens:
                metrics.LLM_TOKENS.labels(model_id=served_model, direction="prompt").inc(
                    prompt_tokens
                )
            if completion_tokens:
                metrics.LLM_TOKENS.labels(
                    model_id=served_model, direction="completion"
                ).inc(completion_tokens)

            active.set_attribute("gen_ai.response.model", served_model)
            active.set_attribute("gen_ai.usage.input_tokens", prompt_tokens or 0)
            active.set_attribute("gen_ai.usage.output_tokens", completion_tokens or 0)
            if finish_reason:
                active.set_attribute("gen_ai.response.finish_reason", finish_reason)

            return Completion(
                text=text,
                model_id=served_model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                latency_ms=elapsed_ms,
                finish_reason=finish_reason,
            )

    def _record_failure(self, outcome: str, started: float) -> None:
        metrics.LLM_SECONDS.labels(
            model_id=self._settings.model_id, outcome=outcome
        ).observe(time.perf_counter() - started)

    def close(self) -> None:
        self._client.close()
