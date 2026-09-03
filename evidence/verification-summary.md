# Verification summary

Captured on 2026-09-03 using Python 3.11 and the local full Compose profile.

| Command | Result |
|---|---|
| `uv run pytest starter-tests tests -q` | PASS — 88 tests; one non-functional pytest cache permission warning |
| `uv run pytest integration-tests -m "not gpu and not langsmith" -q` | PASS — 57 passed, 16 deselected in 296.82 s (final run) |
| `uv run pytest integration-tests/test_j3_promotion_rollback.py -m "not gpu" -q` | PASS — 6 passed, 3 deselected |
| `uv run pytest integration-tests/test_j1_golden_path.py -m gpu -q` | PASS — 3 passed, 12 deselected against Kaggle vLLM |
| `uv run pytest integration-tests/test_trace_span_coverage.py -m gpu -q` | PARTIAL — 2 passed; process coverage failed with 3 services instead of 4 |
| `uv run ruff check .` | PASS — all checks passed |
| `uv run python scripts/verify_matrix.py` | PASS — 245 checks |
| `uv run python scripts/check_portability.py` | PASS |
| `uv run python scripts/validate_manifests.py` | PASS |
| `docker compose --env-file ports.template config --quiet` | PASS |
| `docker compose --env-file ports.template --profile full config --quiet` | PASS |

GPU and LangSmith tests were not run because the required endpoint/credential is absent. This is recorded as `UNVERIFIED`, not passed.
