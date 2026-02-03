-- # Use correct DB, Schema, Warehouse --

use role accountadmin;
use database sales_db;
use warehouse ingest_wh;
use schema raw; 

-- # Create File Format (CSV) --

create or replace file format sales_csv_format
    type = 'csv'
    field_delimiter = ','
    skip_header = true
    null_if = (" ", NULL, null)
    field_optionally_enclosed_by = '"'
    trim_space = true; 

-- # Create Storage Integration --

create or replace storage integration s3_sales_integration
    type = external_stage
    enabled = true
    storage_provider = aws
    storage_aws_role_arn = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/<ROLE_NAME>'
    storage_allowed_locations = ('s3://<bucket-name>/sales/');

DESC integration s3_sales_integration; // describe it (to get External ID / IAM details


-- # Create Stage Object --

create or replace stage s3_sales_stage
    url = 's3://<bucket-name>/sales/'
    storage_integration = s3_sales_integration
    file_format = sales_csv_format;

list @s3_sales_stage // list out the files in the stage 



-- # Create a raw table LAND everything in STAGING as STRING columns.

CREATE OR REPLACE TABLE STG_ORDERS_LANDING (
  ORDER_ID        STRING,
  CUSTOMER_ID     STRING,
  PRODUCT_ID      STRING,
  QUANTITY        STRING,
  PAYMENT_METHOD  STRING,
  CREATED_AT      STRING,
  LOAD_TIME       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- # Create a target raw table to store curated raw data(Target typed table)

CREATE OR REPLACE TABLE target_table_orders (
    ORDER_ID         STRING,
    CUSTOMER_ID      STRING,
    PRODUCT_ID       STRING,
    QUANTITY         NUMBER,
    PAYMENT_METHOD   STRING,
    CREATED_AT       TIMESTAMP_NTZ,
    LOAD_TIME       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() 
);

// create a Quarantine table for rejected_records
CREATE OR REPLACE TABLE raw_sales_orders_rejected (
  ORDER_ID STRING,
  CUSTOMER_ID STRING,
  PRODUCT_ID STRING,
  QUANTITY STRING,
  PAYMENT_METHOD STRING,
  CREATED_AT STRING,
  LOAD_TIME TIMESTAMP_NTZ,
  ERROR_REASON STRING,
  REJECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- # Validate our data before loading into a table

copy into target_table_orders 
from @s3_sales_stage
file_format=(format_name = sales_csv_format)
validation_mode = 'return_errors';



// copy into raw sales table
copy into STG_ORDERS_LANDING
from @s3_sales_stage )
file_format = (format_name = sales_csv_format)
on_error = continue; // bad rows skipped, pipeline doesn’t fail.


// copy data into raw_target table
INSERT INTO target_table_orders (
    ORDER_ID,
    CUSTOMER_ID,
    PRODUCT_ID,
    QUANTITY,
    PAYMENT_METHOD,
    CREATED_AT
)
SELECT
    ORDER_ID,
    CUSTOMER_ID,
    PRODUCT_ID,
    TRY_TO_NUMBER(QUANTITY) AS QUANTITY,
    PAYMENT_METHOD,
    TRY_TO_TIMESTAMP_NTZ(CREATED_AT) AS CREATED_AT
FROM STG_ORDERS_LANDING
WHERE
    TRY_TO_NUMBER(QUANTITY) IS NOT NULL
    AND TRY_TO_TIMESTAMP_NTZ(CREATED_AT) IS NOT NULL; // check number and quantity if they are not null and okay then and then record is inserted because rest others are already string we cant convert to them.


// save rejected records into rejected table so we can cure it later 

INSERT INTO raw_sales_orders_rejected
(
  ORDER_ID, CUSTOMER_ID, PRODUCT_ID, QUANTITY, PAYMENT_METHOD, CREATED_AT, LOAD_TIME, ERROR_REASON
)
SELECT
  ORDER_ID,
  CUSTOMER_ID,
  PRODUCT_ID,
  QUANTITY,
  PAYMENT_METHOD,
  CREATED_AT,
  LOAD_TIME,
  CONCAT(
    IFF(TRY_TO_NUMBER(QUANTITY) IS NULL, 'Invalid QUANTITY; ', ''),
    IFF(TRY_TO_TIMESTAMP_NTZ(CREATED_AT) IS NULL, 'Invalid CREATED_AT; ', '')
  ) AS ERROR_REASON // two iff is concatenated as error reason
FROM STG_ORDERS_LANDING
WHERE
  TRY_TO_NUMBER(QUANTITY) IS NULL
  OR TRY_TO_TIMESTAMP_NTZ(CREATED_AT) IS NULL; // only those rows are choosen from landing table which one's are try to check(with the help of try_to) and fail during check.
