/*========================================================
  1. MAIN TABLE (CLEAN DATA)
========================================================*/
CREATE OR REPLACE TABLE CUSTOMERS (
    ID INT PRIMARY KEY,
    NAME VARCHAR(20),
    AGE INT,
    CITY VARCHAR(30),
    FILE_NAME VARCHAR(100),
    LOADED_BY VARCHAR(50),
    LOAD_TIME DATETIME
);


/*========================================================
  2. TEMP TABLE (RAW DATA + METADATA)
========================================================*/
CREATE OR REPLACE TABLE TEMP_CUSTOMERS (
    ID VARCHAR(30),
    NAME VARCHAR(20),
    AGE VARCHAR(20),
    CITY VARCHAR(30),
    FILE_NAME VARCHAR(100),
    LOADED_BY VARCHAR(50),
    LOAD_TIME VARCHAR(50)
);


/*========================================================
  3. FILE FORMAT
========================================================*/
CREATE OR REPLACE FILE FORMAT CUSTOMERS_FILE_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
TRIM_SPACE = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL','N/A','null','n/a','',' ')
EMPTY_FIELD_AS_NULL = TRUE;


/*========================================================
  4. STORAGE INTEGRATION (AWS ↔ SNOWFLAKE)
========================================================*/
CREATE OR REPLACE STORAGE INTEGRATION CUSTOMERS
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::352326956168:role/S3_CUSTOMER'
STORAGE_ALLOWED_LOCATIONS = (
    's3://demo-bit-bucket123/',
    's3://learning-aws-s3-ingestion/'
);

-- Verify integration (use External ID & ARN in AWS trust policy)
DESC STORAGE INTEGRATION CUSTOMERS;


/*========================================================
  5. STAGE CREATION (ONE PER BUCKET PATH)
========================================================*/
CREATE OR REPLACE STAGE STG_CUSTOMERS_1
STORAGE_INTEGRATION = CUSTOMERS
URL = 's3://demo-bit-bucket123/'
FILE_FORMAT = (FORMAT_NAME = 'CUSTOMERS_FILE_FORMAT');

CREATE OR REPLACE STAGE STG_CUSTOMERS_2
STORAGE_INTEGRATION = CUSTOMERS
URL = 's3://learning-aws-s3-ingestion/'
FILE_FORMAT = (FORMAT_NAME = 'CUSTOMERS_FILE_FORMAT');


/*========================================================
  6. VERIFY FILES IN STAGE
========================================================*/
LIST @STG_CUSTOMERS_1;
LIST @STG_CUSTOMERS_1/customer_folder;
LIST @STG_CUSTOMERS_1/folder2/student_data.csv;


/*========================================================
  7. VALIDATE FILE BEFORE LOAD
========================================================*/
COPY INTO TEMP_CUSTOMERS
FROM @stg_customers_1/customer_folder/customers.csv
VALIDATION_MODE = RETURN_ERRORS;


/*========================================================
  8. LOAD RAW DATA INTO TEMP TABLE (WITH METADATA)
========================================================*/
COPY INTO TEMP_CUSTOMERS
FROM (
    SELECT 
        $1 AS ID,
        $2 AS NAME,
        $3 AS AGE,
        $4 AS CITY,
        METADATA$FILENAME AS FILE_NAME,
        CURRENT_USER() AS LOADED_BY,
        CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM @stg_customers_1/customer_folder/customers.csv
)
ON_ERROR = CONTINUE
FORCE = FALSE;
/* Optional:
   FILE_FORMAT override can be used here if needed
   PURGE = TRUE (use carefully)
*/


/*========================================================
  9. FILE LOAD TRACKER 
========================================================*/
CREATE OR REPLACE TABLE LOAD_FILE_TRACKER (
    FILE_NAME VARCHAR(100),
    LOAD_TIME DATETIME
);

INSERT INTO LOAD_FILE_TRACKER
SELECT DISTINCT 
    FILE_NAME,
    CURRENT_TIMESTAMP()
FROM TEMP_CUSTOMERS;

SELECT * FROM LOAD_FILE_TRACKER;


/*========================================================
  10. VERIFY TEMP DATA
========================================================*/
SELECT * FROM TEMP_CUSTOMERS;


/*========================================================
  11. LOAD CLEAN DATA INTO MAIN TABLE (DEDUP + CLEAN)
========================================================*/
INSERT INTO CUSTOMERS
SELECT 
    ID,
    NAME,
    AGE,
    CITY,
    FILE_NAME,
    LOADED_BY,
    LOAD_TIME
FROM (
    SELECT 
        ID,
        NAME,
        AGE,
        CITY,
        FILE_NAME,
        LOADED_BY,
        LOAD_TIME,
        ROW_NUMBER() OVER (
            PARTITION BY ID 
            ORDER BY LOAD_TIME DESC
        ) AS RW
    FROM (
        SELECT 
            TRY_TO_NUMBER(ID) AS ID,
            TRIM(NAME) AS NAME,
            TRY_TO_NUMBER(AGE) AS AGE,
            COALESCE(
                TRIM(SPLIT_PART(CITY, ',', 1)),
                'UNKNOWN'
            ) AS CITY,
            FILE_NAME,
            LOADED_BY,
            TRY_TO_TIMESTAMP(LOAD_TIME) AS LOAD_TIME
        FROM TEMP_CUSTOMERS
    ) T
) TT
WHERE 
    TT.RW = 1
    AND ID IS NOT NULL
    AND NAME IS NOT NULL
    AND AGE BETWEEN 1 AND 110;


/*========================================================
  12. VERIFY FINAL TABLE
========================================================*/
SELECT * FROM CUSTOMERS;