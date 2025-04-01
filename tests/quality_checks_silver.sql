/*
===============================================================================
Quality Checks - Silver Layer
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/



/**
	DATA VALIDATION :: Script to check silver layer table crm_cust_info 
    Check duplicates, missing values, extra spaces, check for join condition with the table crm_sales_details
**/

-- 1) Check for NULLs or DUPLICATES in the Primary key. A primary key must be unique and not null.
-- Expected: No Results
select count(*) from silver.crm_cust_info; -- 18,484 total rows in the table
select count(distinct cst_id) from silver.crm_cust_info; -- 18,484

select count(*) from silver.crm_cust_info
where cst_id is null OR cst_id = '';  -- 0 rows with blanks here

select cst_id
	, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1  -- no rows with duplicate primary key exist!
;


-- Check for nulls ...
select * from silver.crm_cust_info
where cst_id is null OR cst_id = ''; -- no rows found



-- 2) Check for unwanted SPACES in columns...
-- Expected: No Results

-- SELECT * FROM silver.crm_cust_info
-- LIMIT 100;

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
; -- no rows

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname)
; -- no rows

SELECT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status <> TRIM(cst_marital_status)
; -- no rows returned

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr <> TRIM(cst_gndr)
; -- no rows returned


-- 3) Data Standardisation & Consistency checks ..
-- cst_gndr
SELECT 
	DISTINCT cst_gndr 
FROM silver.crm_cust_info
;

-- cst_marital_status
SELECT 
	DISTINCT cst_marital_status
FROM silver.crm_cust_info
;


SELECT * FROM silver.crm_cust_info
LIMIT 100;



/** 
	DATA VALIDATION :: Script to check silver layer table crm_prd_info
    Check duplicates, missing values, extra spaces, check for join condition with the table crm_sales_details
**/

Use silver;


SELECT
	prd_id
    , cat_id
    , prd_key 
    , prd_nm
    , prd_cost
    , prd_line
    , prd_start_dt
    , prd_end_dt
FROM silver.crm_prd_info LIMIT 100;

-- 1) Check for NULLs or DUPLICATES in the Primary key. A primary key must be unique and not null.
-- Expected: No Results
select count(*) from silver.crm_prd_info; -- 397 total rows in the table
select count(distinct prd_id) from silver.crm_prd_info; -- 397

select count(*) from silver.crm_prd_info
where prd_id is null OR prd_id = '';  -- 0 rows with blanks here

select prd_id
	, count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1  -- no rows with duplicate primary key exist!
;


-- Check prd_line data standardisation - should have user friendly terms
select distinct prd_line
from silver.crm_prd_info
;  -- Mountain, Road, Other Sales, Touring, n/a

select prd_line
from silver.crm_prd_info
where prd_line <> TRIM(prd_line)
;

-- Check prd_cost : int type - any negatives or null values here ?
SELECT
	*
FROM silver.crm_prd_info
where prd_cost IN (
	SELECT prd_cost
	FROM silver.crm_prd_info
	where prd_cost < 0 OR prd_cost is null
)
; 


-- -- Check the last 2 date columns: End date must not be earlier than the start date

select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt
;   -- No such rows!!


select * from silver.crm_prd_info;



/** 
	Script to check silver layer table crm_sales_details
    Check duplicates, handle missing values, handle extra spaces
**/

Use silver;

SELECT * FROM silver.crm_sales_details;
SELECT count(*) FROM silver.crm_sales_details;  -- 60,398 rows


-- Check for spaces in sls_ord_num
SELECT
	sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, sls_order_dt
	, sls_ship_dt
	, sls_due_dt
	, sls_sales
	, sls_quantity
	, sls_price
FROM silver.crm_sales_details
where TRIM(sls_ord_num) <> sls_ord_num
;  -- no rows returned


-- Check for spaces in sls_prd_key
SELECT
	sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, sls_order_dt
	, sls_ship_dt
	, sls_due_dt
	, sls_sales
	, sls_quantity
	, sls_price
FROM silver.crm_sales_details
where TRIM(sls_prd_key) <> sls_prd_key
;  -- no rows returned

 
-- Do we have rows with duplicate values for both: <sls_ord_num, sls_prd_key> ?
select 
	sls_ord_num
	, sls_prd_key
    , count(*)
FROM silver.crm_sales_details
group by sls_ord_num, sls_prd_key
having count(*) > 1
; -- no duplicates here!

-- The combination <sls_ord_num, sls_prd_key> uniquely identifies a row in this table.



-- Check referential integrity for sls_prd_key: do we have any sls_prd_key values that do not exist in silver.crm_prd_info <prd_key> ?
-- NOTE we will use the silver layer here as we cleaned the table crm_prd_info and separated out the columns to join!
SELECT
	sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, sls_order_dt
	, sls_ship_dt
	, sls_due_dt
	, sls_sales
	, sls_quantity
	, sls_price
FROM silver.crm_sales_details
where sls_prd_key NOT IN (select distinct prd_key from silver.crm_prd_info)
;  -- 0 rows returned!



-- Check referential integrity for sls_cust_id: do we have any sls_cust_id values that do not exist in silver.crm_cust_info <cust_id> ?
-- NOTE we will use the silver layer here as we cleaned the table crm_cust_info!
SELECT
	sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, sls_order_dt
	, sls_ship_dt
	, sls_due_dt
	, sls_sales
	, sls_quantity
	, sls_price
FROM silver.crm_sales_details
where sls_cust_id NOT IN (select distinct cst_id from silver.crm_cust_info)
;  -- 0 rows returned!

-- Quick check for extra spaces ?
SELECT
	*
FROM silver.crm_sales_details
where TRIM(sls_cust_id) <> sls_cust_id
; -- no rows


-- sls_order_dt
SELECT * FROM silver.crm_sales_details LIMIT 100;
SELECT
	sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, sls_order_dt
	, sls_ship_dt
	, sls_due_dt
	, sls_sales
	, sls_quantity
	, sls_price
FROM silver.crm_sales_details
where sls_order_dt <= 0
;  -- no rows


-- check for date boundaries as per business rules
SELECT
	sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, sls_order_dt
FROM silver.crm_sales_details
where sls_order_dt > 20250331
; -- no rows



-- Check sls_ship_dt
SELECT
	sls_ship_dt
FROM silver.crm_sales_details
where sls_ship_dt <= 0
;  -- no such rows


-- check for date boundaries as per business rules
SELECT
	sls_ship_dt
FROM silver.crm_sales_details
where sls_ship_dt > 20250331
; -- no rows



-- Check sls_due_dt
SELECT
	sls_due_dt
FROM silver.crm_sales_details
where sls_due_dt <= 0
;  -- no such rows


-- check for date boundaries as per business rules
SELECT
	sls_due_dt
FROM silver.crm_sales_details
where sls_due_dt > 20250331
; -- no rows


-- Check shipping dates & due dates are same or after order dates
SELECT * FROM silver.crm_sales_details
WHERE (sls_ship_dt < sls_order_dt) 
	OR (sls_due_dt < sls_order_dt)
;  -- no such rows!


-- sls_sales = sls_quantity * sls_price
-- None of the columns for sales, qty or price can be nulls or zeros..
SELECT * FROM silver.crm_sales_details LIMIT 100;

SELECT
	*
FROM silver.crm_sales_details
where sls_sales <> (sls_quantity * sls_price)
	OR sls_sales is null OR sls_sales <= 0
	OR sls_quantity is null OR sls_quantity <= 0
	OR sls_price is null OR sls_price <= 0
order by sls_sales, sls_quantity, sls_price
;  -- 0 rows returned.



/** 
	Script to check silver layer table erp_loc_a101
    Check duplicates, missing values, check join condition
**/

Use silver;

select count(*) from silver.erp_loc_a101;  -- 18,484 rows

SELECT * FROM silver.erp_loc_a101
;

-- Check join condition with crm_cus_info
SELECT 
	cid
	, cntry
FROM silver.erp_loc_a101
where cid NOT IN (select distinct cst_key from silver.crm_cust_info)
;   -- no such rows, so join is good!


-- -- Check country
select distinct cntry from silver.erp_loc_a101
order by cntry;



select 
	distinct cntry as old_cntry
    , REPLACE(REPLACE(cntry, '\r', '*'), ' ', '?' ) as exposed_cntry 	-- column has carriage return and spaces!
    , CASE 
			WHEN TRIM(REPLACE(UPPER(cntry), '\r', '')) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(REPLACE(UPPER(cntry), '\r', '')) = 'DE' THEN 'Germany'
            WHEN TRIM(REPLACE(UPPER(cntry), '\r', '')) = '' THEN 'n/a'
		ELSE TRIM(REPLACE(cntry, '\r', ''))
      END as new_cntry
from silver.erp_loc_a101
order by cntry
;




/** 
	Script to check silver layer table erp_cust_az12
    Check duplicates, handle missing values, handle extra spaces
**/

Use silver;

SELECT 
	cid
	, bdate
	, gen
FROM silver.erp_cust_az12
;

select * from silver.crm_cust_info;

select count(*) from silver.crm_cust_info;  -- 18,484


-- Check for nulls in cid
SELECT 
	cid
	, bdate
	, gen
FROM silver.erp_cust_az12
where cid is null OR cid = ''
; -- no nulls or blanks here

-- Duplicates in cid ?
SELECT 
	cid
	, count(*)
FROM silver.erp_cust_az12
group by cid
having count(*) > 1
;  -- no duplicates exist




-- Check referential integrity: all cid values here exist in crm_cust_info <cst_key> ?
SELECT 
	*
FROM silver.erp_cust_az12
where cid NOT IN (select distinct cst_key from silver.crm_cust_info)
;  -- no such rows!


-- Check bdate ...
select * from silver.erp_cust_az12
where bdate < '1924-01-01'
;

select min(bdate), max(bdate) from silver.erp_cust_az12;
-- 1916-02-10 to 1986-06-25



-- Check gender
select distinct gen from silver.erp_cust_az12
;  -- Male, Female, n/a











