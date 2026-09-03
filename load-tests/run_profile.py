"""Cross-platform HTTP load probe using only the Python standard library."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1)]


def request(
    url: str,
    *,
    path: str,
    method: str,
    body: bytes | None,
    headers: dict[str, str],
) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        target = f"{url.rstrip('/')}/{path.lstrip('/')}"
        probe = urllib.request.Request(target, data=body, headers=headers, method=method)
        with urllib.request.urlopen(probe, timeout=10) as response:
            status = response.status
        error_type = None
        error_detail = None
    except HTTPError as error:
        # HTTP errors such as Envoy's 429 are valid responses, not transport
        # failures. Keeping their real code is essential for bottleneck claims.
        status = error.code
        error_type = "http_error"
        error_detail = str(error.reason)
    except (URLError, TimeoutError, OSError) as error:
        status = 0
        error_type = type(error).__name__
        error_detail = str(getattr(error, "reason", error))
    return {
        "duration_ms": (time.perf_counter() - started) * 1000,
        "status": status,
        "error_type": error_type,
        "error_detail": error_detail,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--path", default="/ready")
    parser.add_argument("--method", choices=("GET", "POST"), default="GET")
    parser.add_argument("--body-file", type=Path)
    parser.add_argument("--out", type=Path, help="Optional JSON evidence output path.")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--warmup", type=int, default=5)
    args = parser.parse_args()
    body = args.body_file.read_bytes() if args.body_file else None
    headers = {"Content-Type": "application/json"} if body is not None else {}
    invoke = partial(
        request,
        args.url,
        path=args.path,
        method=args.method,
        body=body,
        headers=headers,
    )
    for _ in range(args.warmup):
        invoke()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(lambda _: invoke(), range(args.requests)))
    durations = [float(result["duration_ms"]) for result in results]
    successful = [
        float(result["duration_ms"]) for result in results if 200 <= int(result["status"]) < 400
    ]
    statuses: dict[str, int] = {}
    errors: dict[str, int] = {}
    for result in results:
        status = str(result["status"])
        statuses[status] = statuses.get(status, 0) + 1
        if result["error_type"]:
            error = f"{result['error_type']}: {result['error_detail']}"
            errors[error] = errors.get(error, 0) + 1

    def latency(values: list[float]) -> dict[str, float] | None:
        if not values:
            return None
        return {
            "p50": percentile(values, 0.50),
            "p95": percentile(values, 0.95),
            "p99": percentile(values, 0.99),
        }

    repository = Path(__file__).resolve().parent.parent
    git_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    working_tree_dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=repository,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
    )
    report = {
        "evidence_schema_version": "1",
        "captured_at": datetime.now(UTC).isoformat(),
        "git_sha": git_sha or "unavailable",
        "working_tree_dirty": working_tree_dirty,
        "target": f"{args.url.rstrip('/')}/{args.path.lstrip('/')}",
        "method": args.method,
        "requests": args.requests,
        "workers": args.workers,
        "warmup_requests": args.warmup,
        "status_counts": statuses,
        "error_counts": errors,
        "success_rate": len(successful) / len(results) if results else 0.0,
        "latency_ms": {
            "all_responses": latency(durations),
            "successful_responses": latency(successful),
        },
        "environment": {
            "platform": platform.platform(),
            "python": sys.version.split()[0],
            "logical_cpus": os.cpu_count(),
        },
    }
    rendered = json.dumps(report, indent=2)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
