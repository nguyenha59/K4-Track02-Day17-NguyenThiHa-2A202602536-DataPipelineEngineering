# B2 — Brainstorm: Flywheel dữ liệu cho chatbot CSKH tiếng Việt

**Nguyễn Thị Hạ — 2A202602536**

## 1. Bài toán và ràng buộc thực

Một ví điện tử Việt Nam có chatbot CSKH (RAG trên FAQ, chính sách và lịch sử ticket)
trả lời khoảng 40.000 hội thoại mỗi ngày. Khi bot không chắc thì chuyển sang nhân viên.
Mục tiêu là biến **trace sản xuất** (hội thoại, tài liệu được truy xuất, 👍/👎, việc
chuyển sang người, nhãn của nhân viên khi đóng ticket) thành hai sản phẩm:

1. **Eval set** cố định theo version để chấm mỗi lần đổi prompt, model hoặc index.
2. **Dữ liệu fine-tune** (SFT và cặp DPO) cho model phân loại ý định và model trả lời.

Bài toán khó vì bốn lý do:

- Hội thoại chứa rất nhiều PII: số điện thoại, số tài khoản, CCCD, họ tên. Nghị định
  13/2023 bắt buộc xử lý được yêu cầu xoá.
- Tiếng Việt viết không dấu, teencode và xen tiếng Anh.
- Feedback thưa: chỉ khoảng 3% hội thoại có 👍/👎, và người bấm 👎 thường đang giận
  vì chính sách, không phải vì câu trả lời sai.
- Rủi ro lớn nhất là **tự đầu độc**: eval bị rò vào dữ liệu train, nên điểm eval
  tăng mà chất lượng thật không tăng.

## 2. Các câu hỏi then chốt và quyết định

### Q2 — Batch hay streaming?
**Quyết định: batch hằng đêm, theo ngày (giống Lab 17), không dùng streaming.**
Người dùng flywheel là đội ML, mỗi tuần huấn luyện lại một lần. Độ tươi "đủ" ở đây
là T+1 ngày. Streaming (Kafka → Flink) cho dữ liệu tươi trong vài giây, nhưng đổi lại
phải vận hành cluster, xử lý state và exactly-once. Toàn bộ chi phí đó không mua thêm
giá trị gì khi model chỉ đổi mỗi tuần một lần. Thứ duy nhất cần gần real-time là
cảnh báo khi tỉ lệ 👎 tăng vọt sau một lần deploy. Việc này giải quyết bằng một
metric trên hệ thống monitoring sẵn có, không cần pipeline dữ liệu riêng.

### Q4 — Hợp đồng và chất lượng: dòng xấu đi đâu?
**Quyết định: Pydantic contract ở Silver, dòng xấu vào quarantine có lý do. Chặn run
nếu quarantine vượt 2% batch.** Trace phải có `conversation_id`, `turn_idx`, `model_version`,
`prompt_version` và `retrieved_doc_ids`. Thiếu `prompt_version` thì trace đó vô dụng
cho eval, vì không biết nó sinh ra từ cấu hình nào. Đánh đổi là *không bao giờ dừng
run* (như lab) hay *dừng khi vượt ngưỡng*. Tôi chọn có ngưỡng: vài dòng hỏng là nhiễu,
nhưng 20% dòng hỏng gần như chắc chắn là app vừa đổi schema. Khi đó huấn luyện tiếp
trên phần còn lại sẽ lệch phân phối mà không ai biết. Khi chặn run, hệ thống báo cho
on-call của đội app, không phải đội data, vì đội app là bên đổi schema.

### Q5 và Q7 — Train/serve parity và flywheel không tự đầu độc
**Quyết định: tách eval và train theo `conversation_id` *và* theo thời gian, có bước
decontamination bằng near-duplicate.** Eval set lấy từ hội thoại của các tuần
W−4..W−1, đóng băng thành `eval_v<tuần>` theo kiểu snapshot bất biến của lab. Dữ liệu
train chỉ lấy hội thoại *trước* cửa sổ eval. Tôi cũng loại mọi mẫu có MinHash
Jaccard ≥ 0.8 với bất kỳ câu nào trong eval: người dùng hỏi gần như y hệt nhau, nên
tách theo ID thôi vẫn rò. Đánh đổi: mất khoảng 10–15% dữ liệu train vì trùng gần, đổi
lại con số eval đáng tin. Feature ngữ cảnh (hạng thành viên, số giao dịch 30 ngày)
phải join **point-in-time** tại `turn_time` bằng ASOF join, không dùng trạng thái hiện
tại. Nếu không, model học được "khách đã khiếu nại thì sau này có tag VIP-care",
tức là rò dữ liệu tương lai.

### Q6 — RAG hay knowledge graph?
**Quyết định: giữ RAG vector cho FAQ, thêm một bảng tra chính sách có cấu trúc.
Không dùng KG.** 90% câu hỏi là tra cứu một bước ("phí rút tiền về ngân hàng X là bao
nhiêu?"). Phần multi-hop thực sự ("gói A có được hoàn phí nếu nạp qua kênh B trước
ngày C không?") rơi vào khoảng 40 quy tắc. Các quy tắc này nên nằm trong một bảng
`policy_rules` có version, chứ không cần một graph sinh tự động bằng LLM. Embedding
được cache theo `hash(chunk) + model_version`, như `gold_doc_chunks`.

### Q8 — Failure semantics và quyền xoá
**Quyết định: Bronze mã hoá PII theo khoá riêng từng user (crypto-shredding).
Silver và Gold giữ tombstone. Eval và train snapshot có deletion ledger.** Khi có yêu
cầu xoá, ta huỷ khoá của user. Bronze vẫn bất biến về file nhưng không còn đọc được
PII. Silver chuyển thành tombstone (giữ LSN để replay không hồi sinh dữ liệu). Mọi
snapshot chứa `conversation_id` đó sinh ra phiên bản `-r<n>`, và model huấn luyện từ
snapshot cũ được gắn cờ để huấn luyện lại ở chu kỳ sau. Đánh đổi: tốn thêm chi phí
quản lý khoá (KMS) và một bước giải mã khi dựng Silver, đổi lại không phải viết lại
hàng TB Parquet mỗi lần có yêu cầu xoá.

### Q10 — Bối cảnh Việt Nam
**Quyết định: chuẩn hoá Unicode NFC và khôi phục dấu *chỉ ở cột phụ*, che PII bằng
regex cộng NER tiếng Việt.** Văn bản gốc (đã che PII) được giữ nguyên. Thêm cột
`text_norm` có NFC, chữ thường và dấu đã khôi phục để dedup, MinHash và embedding.
Ghi đè lên văn bản gốc sẽ làm mất tín hiệu "người dùng gõ không dấu" mà model cần học.
Chốt PII gồm regex (SĐT `0|+84`, CCCD 12 số, STK) cộng NER cho tên người. Đo bằng
500 câu gán nhãn tay, yêu cầu recall ≥ 0.95. Mẫu có độ tin cậy thấp vào quarantine
thay vì vào train.

## 3. Phương án bị loại

**Loại: dùng LLM-as-judge chấm toàn bộ trace mỗi đêm làm nhãn train.** Cách này
hấp dẫn vì có nhãn cho 100% hội thoại thay vì 3%. Tôi loại vì ba lý do.
(1) Chi phí: 40k hội thoại × ~1.500 token mỗi đêm, chạy lại mỗi lần đổi prompt judge.
(2) Vòng lặp tự củng cố: judge và model trả lời cùng họ, nên dạy model chiều theo sở
thích của judge chứ không phải của khách hàng.
(3) Không tái lập được nếu không cache theo `hash + model + prompt_version` (đúng
bài B1). Tôi chỉ dùng judge cho **eval** trên một mẫu cố định 2.000 câu, có cache,
và hiệu chỉnh định kỳ với 200 nhãn của người.

## 4. Kiến trúc

```
 App chatbot ── trace JSON ─┐
 Ticket DB ─── Debezium CDC ┼─▶ Bronze (Parquet, bất biến, PII mã hoá theo user-key)
 Feedback 👍/👎 ── Kafka ────┘              │
                                           ▼
                          Silver: contract + quarantine (ngưỡng 2%)
                                  che PII (regex + NER), text_norm (NFC, dấu)
                                  conversations / turns / feedback, tombstone khi xoá
                                           │
              ┌────────────────────────────┼─────────────────────────────┐
              ▼                            ▼                             ▼
   gold_eval_v<tuần>             gold_train_sft / dpo           gold_doc_chunks
   đóng băng, deletion ledger    trước cửa sổ eval,             cache embedding
   + LLM-judge (mẫu cố định,     MinHash decontam,              hash + model_version
   cache theo prompt version)    ASOF join feature
              │                            │                             │
              └──────── offline eval ◀── fine-tune hằng tuần ──▶ RAG index ─▶ chatbot
```

Nếu làm prototype, tôi chọn **bước decontamination**: thêm một stage vào `pipeline/`
tính MinHash trên `text_norm` của `gold_training_set` và loại mẫu trùng gần với eval.
Đây là quyết định có tác động lớn nhất đến độ tin cậy của cả flywheel.
