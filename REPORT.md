# Báo cáo LAB 17: Data Pipeline Engineering

**Họ tên:** Nguyễn Hải Quân  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 86.5s
  run 2/3 … 86.1s
  run 3/3 … 114.0s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
    số file parquet                           ✗ 5,000 → 5,000
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

Hai dòng `dashboard rows scanned` và `số file parquet` thuộc bài mở rộng A trong
`EXTRA.md`. Tôi không làm phần mở rộng nên hai dòng này giữ nguyên trạng thái ban đầu,
và chúng không nằm trong bốn tiêu chí chấm.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
| --- | --- |
| **Triệu chứng** | Sau sự cố mạng, người trực bấm Clear Task cho job chạy lại. `gold_training_set` tăng số hàng sau mỗi lượt chạy, rồi chạy thêm lần nữa lại tăng tiếp. Hệ thống không phát sinh lỗi nào. |
| **Nguyên nhân** | Tôi thấy model không khai cột khóa nên dbt không nhận ra dòng trùng, nên nó sinh câu lệnh chỉ biết thêm dòng mới. Phép ghi không lặp lại được nên mọi lần chạy lại đều thành 1 lần nhân bản. Trên nguồn có bản ghi cập nhật làm dấu thời gian của ticket dịch chuyển, nên cùng 1 ticket nó lọt qua bộ lọc ở 2 ngày khác nhau luôn. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: bổ sung `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào khối `config()`. `dags/ai_training_pipeline.py`: đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 13.790 → 26.270 → 38.750 hàng qua ba lượt, số ticket riêng biệt đứng yên ở 12.480 · sau: 12.480 hàng ở cả bốn lượt · checksum ba lượt trước khi sửa là `7c461563f4` / `d11657ff21` / `2b76a4f850`, sau khi sửa là `8dd7c98653` cả ba lượt |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
| --- | --- |
| **Triệu chứng** | `gold_feature_daily` thiếu khoảng 5% số cặp (ngày, khách hàng) so với đối chiếu thủ công. Phần thiếu chỉ nằm ở các ngày đã chạy xong từ trước, các ngày mới thì đủ. |
| **P99 độ trễ đo được** | **2,73 ngày** (P50 = 0,13 ngày · P95 = 1,81 ngày · max = 2,94 ngày · 5,05% bản ghi tới kho muộn hơn một ngày so với lúc sự kiện xảy ra) |
| **Lookback đã chọn** | 3 ngày, vì P99 là 2,73 ngày nên cửa sổ 3 ngày phủ hết phần đuôi của phân bố, đồng thời cũng phủ luôn giá trị max 2,94 ngày. |
| **Nguyên nhân** | Điều kiện để lọc lấy mốc là ngày sự kiện lớn nhất đã có trong bảng đích. Cái mốc này chỉ tiến chứ không lùi, nên mọi sự kiện xảy ra trong quá khứ mà về kho muộn sẽ vĩnh viễn nằm dưới mốc lọc và không được xử lý. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: đổi điều kiện lọc từ `event_date > (select max(event_date) from {{ this }})` thành `event_date >= (select max(event_date) from {{ this }}) - interval 3 day`, và bổ sung `unique_key = ['event_date', 'customer_id']` cùng `incremental_strategy = 'merge'` để lần tính sau ghi đè lần tính trước thay vì cộng dồn. |
| **Bằng chứng** | trước: 8.645 hàng, thiếu 455 hàng, phân bố ở 11 ngày cũ từ 03/08 tới 13/08 · sau: 9.100 hàng, khớp `expected/gold_feature_daily.count`, checksum giống nhau ở cả ba lượt |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Lùi theo max phủ 100% nhưng cái giá trả là ở mọi lượt chạy về sau chứ không phải trả 1 lần. Mỗi ngày lùi thêm là mỗi lượt phải đọc và tính lại thêm 1 ngày dữ liệu x650 khách hàng. P99 phủ 99% với chi phí thấp hơn. Ở đây P99 và max sát nhau nên 3 ngày phủ cả 2.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
| --- | --- |
| **Triệu chứng** | Từ 10/08 team backend đổi kiểu cột `priority` từ số sang chuỗi. Pipeline không dừng và `dbt test` vẫn báo pass 9/9, nhưng mô hình phân loại dự đoán kém hẳn kể từ ngày đó. |
| **Nguyên nhân** | Tôi thấy nguồn đổi cách biểu diễn nhưng tầng chuyển đổi vẫn giữ nguyên giả định cũ. Phép ép kiểu sai theo 2 hướng khác nhau: loại nhầm nhãn chữ hợp lệ, đồng thời cho lọt 0 5 -1 vì chúng vô tình đúng kiểu số. Contract tắt nên không có gì chặn lại được. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Nhóm 1, số hợp lệ `1` `2` `3` `4`: đúng contract ban đầu, giữ nguyên. Nhóm 2, nhãn chữ `urgent` `high` `medium` `low`: nguồn đổi cách biểu diễn nhưng ý nghĩa không đổi, quy về 1, 2, 3, 4 theo tài liệu API. Nhóm 3, giá trị hỏng `0` `5` `-1` `P1` `P2` `unknown` chuỗi rỗng và NULL: đưa vào `quarantine_tickets`, tổng cộng 312 bản ghi. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng khối `CASE` xử lý đủ ba nhóm, trả về NULL cho nhóm 3, và viết thêm `priority_reject_reason` để phân biệt bốn loại lỗi. `dbt/models/silver/silver_tickets.sql`: tách CTE để lọc bỏ bản ghi hỏng trước khi xếp hạng bằng `row_number`, nhờ đó loại bản ghi chứ không loại cả ticket. `dbt/models/silver/quarantine_tickets.sql`: thay `where false` bằng điều kiện macro trả về NULL. `dbt/models/silver/schema.yml`: bật `contract: enforced: true` và thêm test `not_null` cùng `accepted_values [1, 2, 3, 4]` cho cột `priority`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng, khớp `expected/quarantine_tickets.count` · `dbt test` 11/11 pass, nhiều hơn 9 test của bản gốc · `silver_tickets.priority` không còn NULL và luôn thuộc 1..4, trước đó có 6.606 hàng sai trong đó 6.488 hàng NULL · `silver_tickets` vẫn giữ đủ 12.480 ticket |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> Chặn ở silver, vì bronze phải giữ nguyên bản thô kể cả khi hỏng, vì nếu bronze từ chối thì sau này điều tra sự cố không còn bằng chứng nguồn đã gửi gì. Không dừng pipeline vì bản ghi hỏng không có quyền chặn ticket, event và chunk bình thường được. Lỗi dữ liệu nên được định tuyến, chứ không nên dùng làm phanh.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
| --- | --- |
| **Bài đã làm** | Không làm |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
| --- | --- |
| 1 | Tôi chạy 2 lượt liên tục trước tiên rồi so số hàng |
| 2 | Sau đó đo khoảng cách giữa lúc sự kiện xảy ra và lúc nó tới kho |
| 3 | Tiếp theo là so phân bố giá trị ở tầng thô với tầng đã làm sạch |
