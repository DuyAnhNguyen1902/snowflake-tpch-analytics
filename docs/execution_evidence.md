# Execution Evidence

Tài liệu này ghi lại số liệu thực tế của lần chạy project trên Snowflake. Các
mục không được thực hiện riêng trong lần chạy được ghi rõ, không thay bằng số
liệu ước lượng.

## Thông tin lần chạy

- Ngày/giờ: 2026-10-16; task graph chạy khoảng 20:53 ICT (Asia/Saigon)
- Account/region: redacted in the public portfolio copy
- User: redacted in the public portfolio copy
- Role: `TPCH_ADMIN`
- Warehouse và size: `TPCH_WH`, X-Small, auto-suspend 60 giây, auto-resume bật
- Database: `TPCH_ANALYTICS_DB`
- Dataset/scale factor: `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`, SF1
- Người thực hiện: Nguyễn Duy Anh

## Row counts

| Table | Expected | Actual | Pass/Fail |
|---|---:|---:|---|
| REGION | 5 | 5 | PASS |
| NATION | 25 | 25 | PASS |
| SUPPLIER | 10,000 | 10,000 | PASS |
| CUSTOMER | 150,000 | 150,000 | PASS |
| PART | 200,000 | 200,000 | PASS |
| PARTSUPP | 800,000 | 800,000 | PASS |
| ORDERS | 1,500,000 | 1,500,000 | PASS |
| LINEITEM | ~6,001,215 | 6,001,215 | PASS |

Bằng chứng: `../evidence/01_bronze_row_counts.png`.

Đây là baseline trước kiểm thử incremental. Sau khi nạp file kiểm thử,
`STAGING.ORDERS` và `SILVER.ORDERS_SILVER` tăng lên 1,500,001 dòng; phần tăng
thêm chính là `O_ORDERKEY = 900000001`.

## Medallion row counts

| Layer | Table | Actual rows |
|---|---|---:|
| Silver | CUSTOMER_SILVER | 150,000 |
| Silver | ORDERS_SILVER | 1,500,000 |
| Silver | LINEITEM_SILVER | 6,001,215 |
| Gold | CUSTOMER_LTV | 150,000 |
| Gold | DAILY_SALES_SUMMARY | 2,406 |
| Analytics | CUSTOMER_RFM_SCORES | 150,000 |
| Analytics | SALES_TREND_MONTHLY | 80 |

## Data quality

| Check | Violation count | Notes |
|---|---:|---|
| Required NULLs | 0 | 8/8 bảng không có NULL ở các trường bắt buộc |
| Duplicate keys | 0 | 8/8 kiểm tra business key đạt yêu cầu |
| Orphan foreign keys | 0 | 6/6 quan hệ được kiểm tra không có bản ghi mồ côi |
| Invalid domains/ranges | 0 | Trạng thái, giá, discount, tax, quantity và ngày giao hàng hợp lệ |
| Bronze-Silver-Gold reconciliation | 0 mismatch | 4/4 phép đối soát có `TEST_PASSED = TRUE` |

Lưu ý: `COMMIT_AFTER_RECEIPT` không được tính là lỗi bắt buộc trong bảng trên vì
dữ liệu TPC-H có thể chứa tình huống giao/commit trễ phục vụ truy vấn phân tích.

## Pipeline evidence

- Stage/list files: internal stage `TPCH_ANALYTICS_DB.STAGING.TPCH_DATA_STAGE`
  và file format `TPCH_PIPE_FORMAT` đã được tạo. Initial load dùng
  `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`; sau đó file
  `orders/incremental_orders_900000001.tbl` được upload thành công vào stage để
  kiểm thử luồng ingest file thực tế.
- Pipe status: 8 pipe đã được định nghĩa cho REGION, NATION, CUSTOMER, SUPPLIER,
  PART, PARTSUPP, ORDERS và LINEITEM. Bộ bằng chứng có lần ingest file incremental
  vào `STAGING.ORDERS`; initial load vẫn dùng sample data SF1.
- Stream before task: không chụp trạng thái trước khi task graph chạy.
- Task history/return values: 5/5 task đạt `SUCCEEDED` theo đúng thứ tự:
  `TASK_BRONZE_TO_SILVER_ORDERS` → `TASK_BRONZE_TO_SILVER_CUSTOMERS` →
  `TASK_BRONZE_TO_SILVER_LINEITEM` → `TASK_SILVER_TO_GOLD_CUSTOMER_LTV` →
  `TASK_SILVER_TO_GOLD_DAILY_SALES`. Chuỗi chạy hoàn tất từ
  `2026-10-16 06:53:38.730 -0700` đến `06:53:58.381 -0700`; không có error code
  hoặc error message.
- Stream after task: `ORDERS_HAS_DATA = FALSE`, `CUSTOMER_HAS_DATA = FALSE`,
  `LINEITEM_HAS_DATA = FALSE`; trạng thái này cũng được xác nhận lại sau lần
  incremental, chứng minh thay đổi mới đã được task tiêu thụ hết.
- Task graph: 5 task có predecessor đúng; root task chạy lịch 5 phút và hiện
  được `SUSPENDED` sau kiểm thử để tránh phát sinh credit.
- Incremental test key and result: file mới được `COPY INTO STAGING.ORDERS` với
  `STATUS = LOADED`, `ROWS_PARSED = 1`, `ROWS_LOADED = 1` và không có lỗi.
  `O_ORDERKEY = 900000001` xuất hiện trong `SILVER.ORDERS_SILVER` với customer 1,
  trạng thái `OPEN`, giá trị 100.00, ngày 1998-08-03, năm 1998, tháng 8 và quý 3.
  Task graph chạy đủ 5 bước với trạng thái `SUCCEEDED`. Gold được cập nhật:
  customer 1 có 7 orders, total spent 587,862.91, last order date 1998-08-03 và
  tier `VIP`; `DAILY_SALES_SUMMARY` ngày 1998-08-03 có 1 order, 1 customer,
  revenue 100.00 và average order value 100.00.

## Business insights

1. **Customer geography:** kết quả có 25 quốc gia. Indonesia đứng đầu với
   6,161 customers và average account balance 4,533.43; France và Romania cùng
   có 6,100 customers. Vietnam có 6,008 customers và average account balance
   4,507.66.
2. **Order status mix:** sau incremental có tổng 1,500,001 orders. `OPEN` có
   732,045 orders, total value 110,017,774,540.76 và average 150,288.27;
   `FULFILLED` có 729,413 orders, total value 109,702,414,613.69 và average
   150,398.22; `PENDING` có 38,543 orders, total value 7,109,117,393.01 và
   average 184,446.39.
3. **Top products:** sản phẩm có lượng bán cao nhất trong kết quả Top 10 là
   `P_PARTKEY = 125009` (`dodger sienna orange khaki lace`) với 1,642 đơn vị và
   doanh thu thuần 1,599,618.68. Trong Top 10, `P_PARTKEY = 49981` có doanh thu
   thuần 2,858,197.98 với 1,553 đơn vị.
4. **Monthly trend:** bảng Snowpark có 80 tháng, từ 1992-01-01 đến 1998-08-01,
   bao phủ đủ 1,500,000 orders với tổng doanh thu 226,829,306,447.46. Tháng
   1998-08 chỉ có 1,199 orders, doanh thu 181,794,522.70 và MoM -93.81% vì đây
   là tháng cuối chưa đầy đủ của tập TPC-H.
5. **Customer tiers/RFM:** Snowpark tạo 150,000 dòng RFM. Dashboard ghi nhận
   40K Champion customers, 60K At Risk customers và average customer value
   khoảng 1.5M. Nhóm điểm cao nhất có `RFM_SCORE = 555` và segment `CHAMPIONS`;
   ví dụ customer 95257 có frequency 36 và monetary 6,563,511.23.

## Power BI dashboard evidence

Dashboard được refresh từ bốn bảng Snowflake: `CUSTOMER_LTV`,
`CUSTOMER_RFM_SCORES`, `DAILY_SALES_SUMMARY` và `SALES_TREND_MONTHLY`.

| Page | Nội dung | Kết quả nổi bật |
|---|---|---|
| Executive Overview | Tổng quan doanh thu/đơn hàng/khách hàng và xu hướng tháng | Revenue 226.8B; 1.50M orders; 150K customers; AOV 151.2K |
| Customer RFM Analysis | Segment, tier, khu vực và Top 10 customers | 40K Champions; 60K At Risk; average customer value 1.5M |

Phân bổ customer value theo vùng trên dashboard lần lượt khoảng Europe 45.8B,
Asia 45.6B, America 45.3B, Africa 45.2B và Middle East 44.9B. Bộ lọc tương tác
gồm năm ở trang tổng quan và region/market segment/RFM segment ở trang RFM.

Bằng chứng: `../assets/dashboard_executive_overview.png`,
`../assets/dashboard_customer_rfm_analysis.png`, file chỉnh sửa
`../powerbi/tpch_analytics_dashboard.pbix` và bản xuất
`../powerbi/dashboard_report.pdf`.

## Snowpark and UDF evidence

- Python Worksheet chạy bằng role `TPCH_ADMIN` và warehouse `TPCH_WH` trả về
  `STATUS = SUCCESS`, `RFM_ROWS = 150000`, `TREND_ROWS = 80`.
- UDF tests: 7/7 test đạt `TEST_PASSED = TRUE`, bao gồm phân loại revenue,
  kiểm tra phone và kiểm tra email.

## Performance evidence

Không thực hiện clustering experiment trên SF1, vì vậy cột "After experiment"
được ghi là N/A và không tuyên bố cải thiện hiệu năng.

| Metric | Baseline | After experiment | Change |
|---|---:|---:|---:|
| Total elapsed time | 1.2 s | N/A | N/A |
| Compilation time | 264 ms | N/A | N/A |
| Queued provisioning | 63 ms | N/A | N/A |
| Execution time | 831 ms | N/A | N/A |
| Bytes scanned | 16.25 MB | N/A | N/A |
| Percentage scanned from cache | 0.00% | N/A | N/A |
| Partitions scanned | 12 | N/A | N/A |
| Partitions total | 12 | N/A | N/A |
| Local/remote spilling | Không hiển thị/không ghi nhận | N/A | N/A |

Query ID baseline: `01c7c534-000e-1459-0001-63b6001ee866`

Query ID after experiment: N/A — không thực hiện clustering experiment.

Một lần chạy trước đó trong Query History có Query ID
`01c7c504-000e-1466-0001-63b6001f01fe`, total elapsed 546 ms, execution 334 ms,
bytes scanned 94,445,056 và 25 rows produced. Đây chỉ là quan sát giữa các lần
chạy, không được coi là kết quả tối ưu hóa có kiểm soát.

Kết luận dựa trên Query Profile: truy vấn hoàn tất thành công và tạo 25 dòng.
Hai TableScan là các node tốn nhiều nhất (47.2% và 44.4%); Join chiếm 2.8%.
Profile overview ghi nhận Processing 5.6%, Remote disk I/O 33.3%,
Synchronization 55.6% và Initialization 5.6%. Truy vấn quét 12/12 partitions,
không dùng result cache; với SF1 và warehouse X-Small, không có đủ bằng chứng để
tuyên bố clustering sẽ cải thiện hiệu năng.

## Screenshot references

- Setup/Bronze: `../evidence/01_bronze_row_counts.png`,
  `../evidence/01_internal_stage_incremental_file.png`,
  `../evidence/01_roles.png`, `../evidence/01_developer_grants.png`
- Pipeline: `../evidence/02_task_history.png`,
  `../evidence/02_incremental_silver_order.png`,
  `../evidence/02_incremental_customer_ltv.png`,
  `../evidence/02_incremental_daily_sales.png`
- Quality/performance: `../evidence/03_layer_reconciliation.png`,
  `../evidence/03_query_profile.png`
- Snowpark/UDF: `../evidence/04_snowpark_success.png`,
  `../evidence/04_udf_tests.png`
- Dashboard: `../assets/dashboard_executive_overview.png`,
  `../assets/dashboard_customer_rfm_analysis.png`,
  `../powerbi/dashboard_report.pdf`

