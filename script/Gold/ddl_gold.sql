/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- ============== DIM_CUSTOMER TABLE ========================

DROP VIEW IF EXISTS  gold.dim_customers;


CREATE VIEW gold.dim_customers AS

SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cts_id) AS customer_key,  -- Surrogate key
    ci.cts_id               Customer_id,
    ci.cts_key              Customer_number,
    ci.cts_firstname        FurstName,
    ci.cts_lastname         LastName,
    la.cntry                Country,
    ci.cts_marital_status   Marital_Status,
    CASE 
        WHEN ci.cts_gender != 'n/a' THEN ci.cts_gender
        ELSE COALESCE(ca.gen, 'n/a')
    END as                  gender,
    ca.bdate                BirthDate,
    ci.cts_create_date      Creat_Date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cts_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cts_key = la.cid;

SELECT * FROM gold.dim_customers;

-- ============== DIM_PRODUCT TABLE ========================

DROP VIEW IF EXISTS  gold.dim_products;

CREATE VIEW gold.dim_products AS

SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;

SELECT * FROM gold.dim_products;

-- =================== FACT_SALES  TABLE =======================

DROP VIEW IF EXISTS gold.fact_sales;

CREATE VIEW gold.fact_sales AS

SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;


SELECT * FROM gold.fact_sales;




