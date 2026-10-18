# Incremental file loading evidence

Tài liệu này dùng file `test_data/incremental_orders_900000001.tbl` để kiểm
chứng luồng bắt buộc trong đề:

`new file -> Internal Stage -> COPY/Snowpipe -> Bronze -> Stream -> Task -> Silver/Gold`

Khóa kiểm thử là `O_ORDERKEY = 900000001`. Chỉ thực hiện một lần với tên file
này. Nếu đã load trước đó, hãy đổi cả tên file và khóa để Snowflake coi đó là
một file mới.

## 1. Upload file bằng SnowSQL hoặc Snowflake CLI

Lệnh `PUT` không chạy trong Snowsight worksheet. Chạy từ thư mục gốc project:

```sql
PUT file://test_data/incremental_orders_900000001.tbl
  @TPCH_ANALYTICS_DB.STAGING.TPCH_DATA_STAGE/orders/
  AUTO_COMPRESS = TRUE
  OVERWRITE = FALSE;
```

## 2. Kiểm tra Stage và nạp Bronze

Chạy trong Snowsight:

```sql
USE ROLE TPCH_ADMIN;
USE WAREHOUSE TPCH_WH;
USE DATABASE TPCH_ANALYTICS_DB;
USE SCHEMA STAGING;

LIST @TPCH_DATA_STAGE/orders/;

COPY INTO ORDERS
FROM @TPCH_DATA_STAGE/orders/
FILE_FORMAT = (FORMAT_NAME = TPCH_PIPE_FORMAT)
PATTERN = '.*incremental_orders_900000001.*'
ON_ERROR = 'ABORT_STATEMENT';

SELECT * FROM ORDERS WHERE O_ORDERKEY = 900000001;
SELECT SYSTEM$STREAM_HAS_DATA('ORDERS_STREAM') AS ORDERS_STREAM_HAS_DATA;
```

Chụp ảnh kết quả `LIST`, `COPY INTO`, bản ghi Bronze và trạng thái Stream.

## 3. Kích hoạt task graph

```sql
EXECUTE TASK TPCH_ANALYTICS_DB.STAGING.TASK_BRONZE_TO_SILVER_ORDERS;
```

Đợi task graph hoàn tất rồi chạy:

```sql
SELECT NAME, STATE, SCHEDULED_TIME, QUERY_START_TIME, COMPLETED_TIME,
       ERROR_CODE, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
  RESULT_LIMIT => 100
))
WHERE DATABASE_NAME = 'TPCH_ANALYTICS_DB'
  AND NAME LIKE 'TASK_%'
ORDER BY SCHEDULED_TIME DESC;

SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS_DESC, O_TOTALPRICE, O_ORDERDATE
FROM SILVER.ORDERS_SILVER
WHERE O_ORDERKEY = 900000001;

SELECT C_CUSTKEY, TOTAL_ORDERS, TOTAL_SPENT, LAST_ORDER_DATE, CUSTOMER_TIER
FROM GOLD.CUSTOMER_LTV
WHERE C_CUSTKEY = 1;

SELECT SUMMARY_DATE, TOTAL_ORDERS, TOTAL_REVENUE
FROM GOLD.DAILY_SALES_SUMMARY
WHERE SUMMARY_DATE = '1998-08-03';

SELECT SYSTEM$STREAM_HAS_DATA('STAGING.ORDERS_STREAM') AS ORDERS_HAS_DATA,
       SYSTEM$STREAM_HAS_DATA('STAGING.CUSTOMER_STREAM') AS CUSTOMER_HAS_DATA,
       SYSTEM$STREAM_HAS_DATA('STAGING.LINEITEM_STREAM') AS LINEITEM_HAS_DATA;
```

Ảnh bằng chứng cần nhìn thấy khóa `900000001` trong Silver, các task
`SUCCEEDED`, Gold đã được tính lại và Stream trở về `FALSE` sau khi tiêu thụ.

## 4. Role/grant và profiling còn thiếu ảnh

Chạy và chụp thêm các khối sau:

```sql
SHOW ROLES LIKE 'TPCH_%';
SHOW GRANTS TO ROLE TPCH_DEVELOPER;
```

Hai truy vấn “Customers by country and region” và “Order mix by status” đã có
ở đầu `sql/03_data_quality_and_profiling.sql`. Chạy từng truy vấn riêng và chụp bảng
kết quả để bổ sung vào hồ sơ.
