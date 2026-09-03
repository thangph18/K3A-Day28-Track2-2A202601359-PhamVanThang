# Báo cáo kỹ thuật — Day 28 Track 2

**Người thực hiện:** Phạm Văn Thắng  
**Hình thức:** Cá nhân  
**Mốc evidence gần nhất:** 2026-09-03 12:56 UTC  
**Trạng thái tổng quát:** IP07 đã xác minh bằng vLLM 0.28.0 trên Kaggle T4; IP10 có đủ 11 span nhưng còn thiếu process vLLM trong cùng trace backend và chưa có LangSmith credential.

## 1. Kiến trúc, ownership và trạng thái tích hợp

![Kiến trúc Lab 28 với 10 integration point](docs/images/lab28-architecture-overview.png)

| Nhóm trách nhiệm | Boundary |
|---|---|
| Ingestion & Orchestration | IP01–IP02: FastAPI → Kafka → Airflow |
| Data & ML | IP03–IP04–IP06: Delta, Feast và MLflow Registry |
| Serving & Retrieval | IP05–IP07: Qdrant và inference vLLM |
| Platform & Observability | IP08–IP10: Envoy, Prometheus/Grafana và OTLP/Jaeger |
| Presenter / Incident Commander | Evidence, replay, failure/recovery, rollback và Q&A |

### Ma trận kết quả

| IP | Trạng thái | Bằng chứng và giới hạn |
|---|---|---|
| IP01 | PASS | Kafka record có partition key theo `entity_id`; header `idempotency-key` khớp logical key trong payload; W3C trace ID được giữ. Xem `evidence/ip01-kafka-consume.json`. |
| IP02 | PASS | DAG thành công, bốn task thành công và phát asset event Delta/Feast/Qdrant. Xem `evidence/ip02-airflow-run.json`. |
| IP03 | PASS | Snapshot có schema/version, MERGE history và time travel cho cả hai bảng; J2 chứng minh ba delivery chỉ tạo một row. Xem `evidence/ip03-delta-history.json` và `evidence/journey-j2-idempotent-replay.json`. |
| IP04 | PASS | Feast trả đúng entity/service, feature status `PRESENT`, freshness và Delta version. Xem `evidence/ip04-feast-online.json`. |
| IP05 | PASS | Qdrant có deterministic document ID, pinned embedding revision và hybrid scores; J2 còn chứng minh replay chỉ để lại một point. |
| IP06 | PASS (registry) | Release có signature, artifact URI, provenance tags và alias; J3 chứng minh promotion rồi rollback. Việc serving đổi theo alias thuộc GPU gate và chưa được xác minh. |
| IP07 | PASS | Kaggle endpoint xác nhận vLLM 0.28.0, phục vụ `Qwen/Qwen3-1.7B`, có 111 metric `vllm:`; GPU-gated golden path pass. |
| IP08 | PASS | Envoy trả cả 200/429 với request ID, counter tăng, bucket refill và `/healthz` được phục vụ không chạm upstream. |
| IP09 | PASS (non-GPU) | Chín target non-GPU up; dashboard và hai actionable alert được provision/evaluate. Target vLLM down đúng với IP07. |
| IP10 | PARTIAL | Trace có đủ 11/11 required spans và không có span lỗi. Còn 3/4 process vì vLLM Kaggle chưa export OTLP về cùng backend; LangSmith chưa có credential. |

`integration-report.json` là readiness report của sáu probe mà tiến trình API có thể chạy, không phải điểm theo rubric. Trường `score` trong file chỉ là tỷ lệ pass trên các probe đó.

## 2. Happy path và tính truy xuất

Happy path đã xác minh qua cả nhánh ingestion, data và serving:

1. Envoy nhận request và gắn `x-request-id`.
2. FastAPI tạo `IngestionEvent`, logical idempotency key và W3C trace context.
3. Kafka giữ partition ordering theo entity, đồng thời mang logical key trong header/payload.
4. Airflow drain batch và gọi Spark Delta MERGE.
5. Delta được materialize sang Feast; documents được index vào Qdrant.
6. MLflow alias `champion` resolve được release có provenance.
7. `/api/v1/ask` gọi vLLM 0.28.0 trên Kaggle T4 và trả completion từ đúng model đã ghim.

Trace hiện đã có đủ 11 required span. Phần còn thiếu của IP10 là telemetry do process vLLM tự phát phải xuất hiện trong cùng backend (test yêu cầu tối thiểu bốn service) và nhánh LangSmith nếu lớp yêu cầu credential.

## 3. Các đánh đổi kỹ thuật

### Kafka ordering và logical idempotency

Kafka record key dùng `entity_id` để các event của cùng entity vào một partition và giữ thứ tự. Logical `idempotency_key` được truyền riêng trong header và payload để Delta MERGE nhận diện cùng một fact. Cách này tăng số lớp contract cần kiểm tra, nhưng tránh đánh đổi ordering để lấy deduplication.

J2 đo được ba Kafka deliveries với ba `event_id` khác nhau nhưng chỉ còn một Delta row và một Qdrant point. Bảo đảm này phụ thuộc vào việc producer tạo cùng logical key và MERGE predicate không thay đổi; đây không phải tuyên bố exactly-once tuyệt đối.

### Batch orchestration và độ trễ

Kafka → Airflow → Spark hấp thụ replay và tạo transaction history rõ ràng, đổi lại dữ liệu không xuất hiện tức thời ở Delta/Feast/Qdrant. Thiết kế phù hợp với ingestion bất đồng bộ, nhưng không phù hợp nếu feature freshness phải ở mức mili-giây.

### Offline/online feature path

Feast dùng file offline store và SQLite online store trong lab. Evidence đo một lookup khoảng 180 ms ở lần chạy trước, cao hơn budget 5 ms ghi trong metric description; vì vậy 5 ms là mục tiêu cần tối ưu, không phải kết quả đã đạt. Production cần đo warm/cold riêng và dùng online store phù hợp tải thật.

### Readiness và serving failure

Readiness phân biệt dependency bắt buộc và có thể suy giảm. Trước khi nối Kaggle, `/ready` trả `degraded` và `/api/v1/ask` trả 503. Sau khi nối vLLM thật, toàn bộ năm dependency đều ready và GPU-gated golden path trả lời thành công.

### Gateway token bucket

Envoy chặn burst trước API để bảo vệ downstream. Profile cuối với 200 request cho thấy 172 HTTP 429 ở 8 workers và 187 HTTP 429 ở 16 workers. Đây là chính sách overload protection đang hoạt động, không phải capacity của serving path; SLO phải tách accepted latency và rejected latency.

## 4. Khoảng cách trước production

1. **Inference:** Kaggle Quick Tunnel chỉ phù hợp demo; production cần endpoint vLLM ổn định, private networking, queueing, autoscaling và timeout/circuit breaker.
2. **Tracing:** 11 span đã đủ nhưng vLLM cần export OTLP vào cùng backend; cấu hình hiện lấy mẫu 100%, production cần tail sampling theo lỗi và độ trễ.
3. **Performance:** profile 503 cũ được ghi khi chưa có vLLM; cần chạy lại trên endpoint GPU với warm-up, CPU/RAM, queue, token throughput và corpus đại diện.
4. **Feature serving:** thay SQLite bằng online store có HA phù hợp, đo lại budget và theo dõi freshness SLO.
5. **Delta maintenance:** lập lịch compaction/VACUUM với retention policy và kiểm tra khả năng time travel trước khi xóa version.
6. **Secrets và identity:** dùng External Secrets/Vault, workload identity, TLS/mTLS và rotation thay cho credential local.
7. **GitOps:** manifests đã qua static validation nhưng chưa có cluster record cho drift/self-heal và desired-state rollback.

## 5. Đóng góp cá nhân

| Hạng mục | Đóng góp |
|---|---|
| Core tasks | Hoàn thiện `event_headers`, `dedupe_latest`, `feast_online_request`, `readiness_status`. |
| Kafka/Airflow reliability | Sửa logical idempotency header tách khỏi partition key; bổ sung chờ consumer assignment; sửa import path cho Airflow DAG. |
| Build portability | Bổ sung retry/timeout cho dependency installation trong API/Airflow images. |
| Evidence | Chạy live integration suite; bổ sung timestamp/Git SHA; tạo record J2 replay, J3 rollback và J4 recovery/no-data-loss. |
| Performance | Sửa profiler để giữ HTTP 429 thay vì gộp thành status 0; tách latency all/success và ghi môi trường chạy. |
| Validation | Chạy fast tests, Ruff, matrix, portability, manifests, Compose validation và 57 live tests không GPU/LangSmith. |

Các cấu hình kiến trúc còn lại là scaffold của bài lab mà tôi đã triển khai, chạy và xác minh; không nhận là phần được viết mới hoàn toàn.
