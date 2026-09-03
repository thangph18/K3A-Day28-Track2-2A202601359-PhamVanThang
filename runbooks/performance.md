# Performance profile

Chạy `uv run python load-tests/run_profile.py --requests 200 --workers 8`, rồi
lặp với 16 workers. Dùng `--out evidence/<name>.json` để giữ timestamp, status/error
breakdown, latency của toàn bộ response và latency riêng của response thành công.
Ghi thêm API CPU/RAM, vLLM queue/tokens, Kafka lag và error rate. `/ready` là
baseline; nhóm phải đo thêm `/api/v1/ask` với corpus đại diện, ví dụ:

```text
uv run python load-tests/run_profile.py --url http://localhost:8000 --path /api/v1/ask --method POST --body-file load-tests/ask-payload.json --requests 200 --workers 8 --out evidence/load-profile-ask-w8.json
```

Không suy ra production capacity từ laptop. Luôn ghi hardware, model, dataset,
concurrency, warm-up và degraded policy.
