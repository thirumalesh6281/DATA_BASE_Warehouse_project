/*
===============================================================================
Silver Layer Transformation & Cleansing Script
===============================================================================
Script Purpose:
    This script transforms and cleans raw data from the Bronze layer and
    loads structured, standardized data into the Silver schema.

Transformation Logic Applied:
    1. Removes duplicates using ROW_NUMBER() logic.
    2. Trims and standardizes text fields (TRIM, UPPER).
    3. Normalizes categorical values (Gender, Marital Status, Product Line).
    4. Cleans and validates date fields.
    5. Recalculates incorrect sales and price values when necessary.
    6. Handles NULL, missing, and invalid data scenarios.
    7. Standardizes country codes and customer identifiers.
    8. Implements SCD-Type logic for product end dates using LEAD().

Tables Processed:
    CRM:
        - crm_cust_info
        - crm_prd_info
        - crm_sales_details
    ERP:
        - erp_cust_az12
        - erp_loc_a101
        - erp_px_cat_g1v2

Load Strategy:
    Full Refresh (TRUNCATE + INSERT SELECT)

Data Flow:
    Bronze (Raw Data) → Silver (Cleaned & Standardized Data)

Environment:
    Designed for Snowflake Data Warehouse architecture following
    Stage → Bronze → Silver ETL pattern.

===============================================================================
*/


-- ============================= CRM ===================
-- ================= Loading crm_cust_info ================

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cts_id,
    cts_key,
    cts_firstname,
    cts_lastname,
    cts_gender,
    cts_marital_status,
    cts_create_date
)

SELECT
    cts_id,
    cts_key,
    TRIM(cts_firstname) AS cts_firstname,
    TRIM(cts_lastname) AS cts_lastname,
    CASE 
        WHEN UPPER(TRIM(cts_gender)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cts_gender)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cts_gender,
    CASE 
        WHEN UPPER(TRIM(cts_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cts_marital_status)) = 'S' THEN 'Single'
        ELSE 'n/a'
    END AS cts_marital_status,
    cts_create_date
From (SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cts_id ORDER BY cts_create_date DESC) AS Flag_test
    FROM bronze.crm_cust_info
    WHERE cts_id IS NOT NULL
) t WHERE Flag_test = 1;

SELECT * FROM silver.crm_cust_info;

-- =====================Loading crm_prd_info ====================

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,       
    prd_end_dt
)


SELECT 
    prd_id,
    REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTR(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    CASE TRIM(UPPER(prd_line))
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'M' THEN 'Moutains'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        DATEADD( day, -1,
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
        ) AS DATE
    ) AS prd_end_dt
FROM bronze.crm_prd_info;

SELECT * FROM silver.crm_prd_info;

-- ==========================  Loading crm_sales_details ===================

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price

)

SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
    	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
    	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE 
    	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
    	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE 
    	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
    	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE 
    	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
    	THEN sls_quantity * ABS(sls_price)
    	ELSE sls_sales
    END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
    sls_quantity,
    CASE 
    	WHEN sls_price IS NULL OR sls_price <= 0 
    	THEN sls_sales / NULLIF(sls_quantity, 0)
    	ELSE sls_price  -- Derive price if original value is invalid
    END AS sls_price
FROM bronze.crm_sales_details;


SELECT * FROM silver.crm_sales_details;


-- ============================= ERP ===================
-- ================= Loading erp_cust_az12 ================

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
		ELSE cid
	END AS cid, 
	CASE
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate, -- Set future birthdates to NULL
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen -- Normalize gender values and handle unknown cases
FROM bronze.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;


-- ================= Loading erp_loc_a101 ==========================

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT
	REPLACE(cid, '-', '') AS cid, 
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    	ELSE TRIM(cntry)
	END AS cntry -- Normalize and Handle missing or blank country codes
FROM bronze.erp_loc_a101;

SELECT * FROM silver.erp_loc_a101;


-- ================= Loading erp_px_cat_g1v2 ==========================

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenance
)


SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;

