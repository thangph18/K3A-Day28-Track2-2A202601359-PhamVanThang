# Submission evidence report

**Candidate:** Phạm Văn Thắng  
**Profile:** full local stack + real vLLM 0.28.0 on Kaggle T4; no LangSmith credential  
**Conclusion:** IP07 passes; IP10 passes with 11/11 spans and all four services (lab28-airflow, lab28-api, lab28-gateway, lab28-vllm) in the shared trace backend.

## Required artefacts

| Submission requirement | Artefact | Status |
|---|---|---|
| Integration report | `integration-report.json` | Present; `ready=true` (score 100%, 6/6 probed endpoints ready). |
| Ten integration points | `evidence/ip01-*.json` through `ip10-*.json` | Present; IP07 PASS and IP10 PASS with full 4-process coverage. |
| Architecture and ownership | `ANSWERS.md`, `docs/images/lab28-architecture-overview.png` | Present. |
| Happy path IDs | IP01/IP02/IP04/IP07/IP10 evidence | Full RAG path passed against Kaggle vLLM. |
| Failure/recovery and no data loss | `evidence/journey-j4-failure-recovery.json` | PASS. |
| Idempotent replay | `evidence/journey-j2-idempotent-replay.json` | PASS: 3 deliveries → 1 row/point. |
| Promotion/rollback | `evidence/journey-j3-promotion-rollback.json` | PASS for registry alias. |
| Load profile | `evidence/load-profile-ready-w8.json`, `load-profile-ready-w16.json`, `load-profile-ask-w8.json` | Collected. |
| Kubernetes/GitOps | `evidence/verification-summary.md`, manifests/runbook | Static validation PASS; live drift/rollback UNVERIFIED. |
| Reflection/contribution | `ANSWERS.md` | Present and scoped to actual changes. |

## Gate interpretation

- IP07 is `PASS`: real vLLM 0.28.0, pinned model and native metrics were verified.
- IP10 is `PASS`: 11/11 required spans are present across four services (lab28-airflow, lab28-api, lab28-gateway, lab28-vllm) in the shared Jaeger backend.
- `integration-report.json.score` is a probe percentage, not a rubric score.

See `evidence/README.md` for the evidence-to-claim index and exact validation results.
