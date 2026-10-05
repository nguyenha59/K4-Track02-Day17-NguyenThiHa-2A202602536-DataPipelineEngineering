# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Thị Hạ / 2A202602536
**Repo:** https://github.com/nguyenha59/K4-Track02-Day17-NguyenThiHa-2A202602536-DataPipelineEngineering
**Commit bài nộp:** `7d18db1`
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5) — đọc repo, đề xuất 3 bản sửa; tôi đã review và tự giải thích từng thay đổi.
**Nguồn tham khảo khác (nếu có):** slide Ngày 17, README/docs của repo.

## 1. Ba lỗi

Baseline trên bản chưa sửa: `verify` **8/18**, Gold combined `f90edc98…` ≠ bản đúng `39e115c5…`.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có **24 hàng cho 12 ticket**; T-91 có 3 hàng `low/open`, `high/open`, `high/closed/bug` | `gold_feature_daily` lệch full recompute (`c50b8851` ≠ `8630e04a`); u05 ngày 08-12 ra `(2, 0)` thay vì `(5, 1)` | T-97 vẫn `is_deleted = false`, còn `user_id`, `subject`, `body`; còn trong snapshot mới nhất (1 hàng) và RAG index (2 chunk) |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT` thuần: chỉ dedup *trong* batch, không ghi theo khoá *giữa* các batch, không có điều kiện thứ tự | `LOOKBACK_DAYS = 0` (giả định "event tới trong vài giây"), nên run ngày 08-15 chỉ tính lại partition 08-15; event 08-12 tới muộn bị bỏ qua | `ticket_changes_sql` lấy `ticket_id` từ `after`; với `op='d'` thì `after = null` → `ticket_id` null → bản ghi delete bị `WHERE ticket_id IS NOT NULL` lọc mất |
| **Cách sửa** | `silver.py`: `MERGE INTO silver_tickets ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT` | `config.py`: `LOOKBACK_DAYS = 3` = ceil(P99 = 3.00 ngày) đo bằng `main.py --lateness` | `staging.py`: `coalesce(after->>'ticket_id', before->>'ticket_id')`; delete đi qua MERGE thành tombstone (cột `after` đều null, giữ `_lsn`) |
| **Khái niệm trên slide** | Silver — có khoá; MERGE/upsert idempotent; LSN guard (batch cũ không thắng) | Data về muộn: event time ≠ ingest time; lookback = ceil(P99) đo từ Bronze; overwrite-partition | CDC log-based (phong bì Debezium, delete ≠ Kafka tombstone); "Xoá phải lan" xuống Gold |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50 = 0, p95 = 2.90, max = 3, n = 43) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY** (`silver_tickets` 3c15dfd43701, `gold_feature_daily` 8630e04a61d1)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- **MERGE theo khoá** cho `silver_tickets` vì đây là bảng thực thể: mỗi batch chỉ chứa ticket có thay đổi, và guard `_lsn` khiến replay ngày cũ không ghi đè được. **Overwrite-partition** cho `gold_feature_daily` vì đây là aggregate: tính lại `[day−3, day]` từ Silver luôn ra cùng kết quả.
- **Tombstone thay vì xoá hẳn**: hàng giữ `_lsn` của lệnh xoá. Nếu xoá hẳn, replay batch 08-12 sẽ `NOT MATCHED` và hồi sinh T-97. Đánh đổi: hàng tồn tại mãi, nhưng PII đã bị xoá.
- **Snapshot as-of từ Bronze, không sửa bản cũ**: tái lập được, và kiểm chứng được bằng checksum. Dữ liệu mới thì tạo version mới.
- **DuckDB/dbt thay vì Spark**: vài trăm bản ghi mỗi ngày chạy trong vài giây trên một máy; Spark chỉ thêm chi phí cluster. dbt cho merge/microbatch khai báo, contract và test.

## 4. Hai câu hỏi suy ngẫm

1. Quyền xoá được ưu tiên hơn tính bất biến. Tôi giữ một *deletion ledger* (các `ticket_id` bị yêu cầu xoá). Snapshot chứa key trong ledger được dựng lại thành `v…-r1` từ Bronze, loại các key đó, rồi retire bản cũ và ghi rõ lý do. Ở Bronze dùng crypto-shredding (mã hoá PII theo khoá riêng từng user, xoá khoá là xoá dữ liệu). Model đã huấn luyện trên snapshot cũ được gắn cờ để huấn luyện lại.
2. Đặt chốt ở **Silver**, lối ra duy nhất của Bronze: NER tiếng Việt kết hợp từ điển họ để thay tên bằng `<NAME>`; độ tin cậy thấp thì đưa vào quarantine. Đo bằng tập vàng gán nhãn tay (recall ≥ 0.95), thêm một contract quét Gold tìm PII sót, và theo dõi tỉ lệ bị che theo ngày.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell (lệnh tương đương theo SUBMISSION.md).

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 11.67s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ .\.venv\Scripts\python.exe main.py --land-only
$ cd dbt_project; ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
03:00:31  Running with dbt=1.12.5
03:00:32  Registered adapter: duckdb=1.11.0
03:00:33  Unable to do partial parsing because saved manifest not found. Starting full parse.
03:00:36  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
03:00:36  
03:00:36  Concurrency: 1 threads (target='dev')
03:00:36  
03:00:37  1 of 19 START sql view model main.stg_events ................................... [RUN]
03:00:37  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.18s]
03:00:37  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
03:00:37  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.09s]
03:00:37  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
03:00:38  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.29s]
03:00:38  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
03:00:38  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.28s]
03:00:38  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
03:00:38  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.27s]
03:00:38  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
03:00:38  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.10s]
03:00:38  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
03:00:38  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.06s]
03:00:38  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
03:00:39  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.07s]
03:00:39  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
03:00:39  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.05s]
03:00:39  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
03:00:39  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.04s]
03:00:39  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
03:00:39  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.06s]
03:00:39  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
03:00:39  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.05s]
03:00:39  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
03:00:39  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.06s]
03:00:39  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
03:00:39  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.05s]
03:00:39  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
03:00:39  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.07s]
03:00:39  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
03:00:39  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
03:00:39  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.07s]
03:00:39  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
03:00:39  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.14s]
03:00:39  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
03:00:39  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.12s]
03:00:39  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
03:00:39  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.11s]
03:00:39  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
03:00:40  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.10s]
03:00:40  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
03:00:40  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.12s]
03:00:40  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
03:00:40  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.10s]
03:00:40  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.86s]
03:00:40  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
03:00:40  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.05s]
03:00:40  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
03:00:40  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.04s]
03:00:40  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
03:00:40  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.04s]
03:00:40  
03:00:40  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 3.53 seconds (3.53s).
03:00:40  
03:00:40  Completed successfully
03:00:40  
03:00:40  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Baseline trước khi sửa (`scripts.verify`, rút gọn các dòng fail):

```text
  [XX ] Silver  silver_tickets has exactly one row per ticket_id  (24 rows for 12 tickets)
  [XX ] Silver  T-91 shows its latest state: high / closed / bug  (got [('low', 'open', None), ('high', 'open', None), ('high', 'closed', 'bug')])
  [XX ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left  (got [(False, 'u06', ...), (False, 'u06', ...)])
  [XX ] Gold    gold_feature_daily reconciles with a full recompute from Silver  (c50b8851affe != 8630e04a61d1)
  [XX ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12  (got (2, 0), expected (5, 1))
  [XX ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)  (LOOKBACK_DAYS=0 < 3)
  [XX ] Gold    latest training snapshot excludes the deleted ticket T-97  (1 row(s))
  [XX ] Gold    deletes propagate to the RAG index: no chunk of T-97  (2 chunk(s))
  [XX ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks  (22 rows / 9 chunks, embedded 0)
  [XX ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build
RESULT: 8/18 checks — FAILURES ABOVE
```

### Bonus

**B1 — Bước LLM có cache** (`pipeline/llm_label.py`): khoá cache `llm_label_cache` =
`sha256(input) + model + prompt_version`; mọi câu trả lời thô đều được cache (kể cả câu
sai schema, để chạy lại không phải trả tiền lần nữa); ước lượng token/chi phí cho phần
*chưa có trong cache* trước khi gọi; nhãn hợp lệ → `gold_ticket_labels` (kèm
`input_hash`, `model`, `prompt_version`), nhãn sai schema → `llm_label_quarantine`.

```text
$ .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

**B2 — Brainstorm:** [`bonus/DESIGN.md`](../bonus/DESIGN.md) — flywheel dữ liệu cho chatbot
CSKH tiếng Việt (6 quyết định có đánh đổi, 1 phương án bị loại, sơ đồ kiến trúc).
