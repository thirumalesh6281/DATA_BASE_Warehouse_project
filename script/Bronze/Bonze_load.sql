/*
===============================================================================
Bronze Layer Load Process Script
===============================================================================
Script Purpose:
    This script manages the Bronze layer data ingestion process in the
    DATABASEWAREHOUSE environment using Snowflake.

Actions Performed:
    1. Drops and recreates the BRONZE.LOAD_LOG table for execution tracking.
    2. Creates or replaces the stored procedure bronze.load_bronze().
    3. Truncates existing Bronze tables before loading new data.
    4. Loads CSV files from the internal stage (my_stage) into Bronze tables.
    5. Captures start time, end time, and duration for each table load.
    6. Inserts load execution details into BRONZE.LOAD_LOG.

Bronze Tables Loaded:
        - crm_cust_info
        - crm_prd_info
        - crm_sales_details
        - erp_cust_az12
        - erp_loc_a101
        - erp_px_cat_g1v2

Logging & Monitoring:
    Execution metadata (table name, start time, end time, duration)
    is stored in DATABASEWAREHOUSE.BRONZE.LOAD_LOG for auditing
    and performance monitoring purposes.

Load Type:
    Full Refresh (TRUNCATE + COPY INTO)

Stage Dependency:
    Source files must exist in:
        @DATABASEWAREHOUSE.PUBLIC.my_stage

Execution:
        CALL bronze.load_bronze();

Environment:
    Designed for Snowflake-based Data Warehouse architecture
    following Stage → Bronze ETL pattern.

===============================================================================
*/

-- using public Schema
USE SCHEMA PUBLIC;

-- Insert all files into stage area

-- create stage

DROP STAGE IF EXISTS my_stage;
CREATE STAGE my_stage;
-- Insert all files into stage area


-- Show list of stage
LIST @my_stage;


DROP TABLE IF EXISTS DATABASEWAREHOUSE.BRONZE.load_log;
-- creates log to insert load start time and end time on this log table
CREATE TABLE DATABASEWAREHOUSE.BRONZE.load_log (
    table_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds NUMBER
);

--CREATING STORED PROCEDURE TABLE

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration NUMBER;
BEGIN

    -- ================= Loading Bronze Layer =================

    -- ================= LOADING CRM TABLES =================
    -- ================= CRM CUSTOMER INFO =================

    v_start_time := CURRENT_TIMESTAMP();

    TRUNCATE TABLE DATABASEWAREHOUSE.BRONZE.crm_cust_info;

    COPY INTO DATABASEWAREHOUSE.BRONZE.crm_cust_info
    FROM @DATABASEWAREHOUSE.PUBLIC.my_stage/cust_info.csv
    FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 1
    );

    v_end_time := CURRENT_TIMESTAMP();
    v_duration := DATEDIFF('second', v_start_time, v_end_time);

    INSERT INTO DATABASEWAREHOUSE.BRONZE.load_log
    VALUES ('crm_cust_info', :v_start_time, :v_end_time, :v_duration);


    -- ================= CRM PRODUCT INFO =================
    
    v_start_time := CURRENT_TIMESTAMP();

    TRUNCATE TABLE DATABASEWAREHOUSE.BRONZE.crm_prd_info;

    COPY INTO DATABASEWAREHOUSE.BRONZE.crm_prd_info
    FROM @DATABASEWAREHOUSE.PUBLIC.my_stage/prd_info.csv
    FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 1
    );

    v_end_time := CURRENT_TIMESTAMP();
    v_duration := DATEDIFF('second', v_start_time, v_end_time);

    INSERT INTO DATABASEWAREHOUSE.BRONZE.load_log
    VALUES ('crm_prd_info', :v_start_time, :v_end_time, :v_duration);

    -- ================= CRM SALES INFO =================

    v_start_time := CURRENT_TIMESTAMP();

    TRUNCATE TABLE DATABASEWAREHOUSE.BRONZE.crm_sales_details;

    COPY INTO DATABASEWAREHOUSE.BRONZE.crm_sales_details
    FROM @DATABASEWAREHOUSE.PUBLIC.my_stage/sales_details.csv
    FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 1
    );

    v_end_time := CURRENT_TIMESTAMP();
    v_duration := DATEDIFF('second', v_start_time, v_end_time);

    INSERT INTO DATABASEWAREHOUSE.BRONZE.load_log
    VALUES ('crm_sales_details', :v_start_time, :v_end_time, :v_duration);


    -- ================= LOADING ERP TABLES =================
    -- ================= ERP CUSTOMER INFO =================
    
    v_start_time := CURRENT_TIMESTAMP();

    TRUNCATE TABLE DATABASEWAREHOUSE.BRONZE.erp_cust_az12;

    COPY INTO DATABASEWAREHOUSE.BRONZE.erp_cust_az12
    FROM @DATABASEWAREHOUSE.PUBLIC.my_stage/CUST_AZ12.csv
    FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 1
    );

    v_end_time := CURRENT_TIMESTAMP();
    v_duration := DATEDIFF('second', v_start_time, v_end_time);

    INSERT INTO DATABASEWAREHOUSE.BRONZE.load_log
    VALUES ('erp_cust_az12', :v_start_time, :v_end_time, :v_duration);

    -- ================= ERP CUSTOMER INFO =================

    v_start_time := CURRENT_TIMESTAMP();

    TRUNCATE TABLE DATABASEWAREHOUSE.BRONZE.erp_loc_a101;

    COPY INTO DATABASEWAREHOUSE.BRONZE.erp_loc_a101
    FROM @DATABASEWAREHOUSE.PUBLIC.my_stage/LOC_A101.csv
    FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 1
    );

    v_end_time := CURRENT_TIMESTAMP();
    v_duration := DATEDIFF('second', v_start_time, v_end_time);

    INSERT INTO DATABASEWAREHOUSE.BRONZE.load_log
    VALUES ('erp_loc_a101', :v_start_time, :v_end_time, :v_duration);

    -- ================= ERP SALES INFO =================

    v_start_time := CURRENT_TIMESTAMP();

    TRUNCATE TABLE DATABASEWAREHOUSE.BRONZE.erp_px_cat_g1v2;

    COPY INTO DATABASEWAREHOUSE.BRONZE.erp_px_cat_g1v2
    FROM @DATABASEWAREHOUSE.PUBLIC.my_stage/PX_CAT_G1V2.csv
    FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 1
    );

    v_end_time := CURRENT_TIMESTAMP();
    v_duration := DATEDIFF('second', v_start_time, v_end_time);

    INSERT INTO DATABASEWAREHOUSE.BRONZE.load_log
    VALUES ('erp_px_cat_g1v2', :v_start_time, :v_end_time, :v_duration);
    
    RETURN 'Bronze Load Completed Successfully';

END;
$$;



CALL bronze.load_bronze();




