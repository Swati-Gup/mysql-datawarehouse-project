/** 
	Script to check Bronze layer table crm_cust_info
    Check duplicates, handle missing values, handle extra spaces,
    check join condition with the tables erp_loc_a101, erp_cust_az12
 
  This script forms the basis for loading data into the Silver layer.
**/


USE bronze;

SELECT * FROM bronze.crm_cust_info LIMIT 100;


-- -- -- --------- CLEANING bronze.crm_cust_info -- -- -----------------

-- 1) Check for NULLs or DUPLICATES in the Primary key. A primary key must be unique and not null.
-- Expected: No Results
select count(*) from bronze.crm_cust_info; -- 18,494 total rows in the table
select count(distinct cst_id) from bronze.crm_cust_info; -- 18,485

select count(*) from bronze.crm_cust_info
where cst_id is null OR cst_id = '';  -- 4 blanks here

select cst_id
	, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1  -- rows with duplicate primary key exist!
;

-- What is this data with duplicate cst_id ?
select 
	*
from bronze.crm_cust_info
where cst_id IN (select distinct cst_id 
						from (select cst_id, count(*) from bronze.crm_cust_info
								group by cst_id
								having count(*) > 1
							) a
				)
ORDER BY cst_id, cst_create_date DESC
; 

-- REMOVE the duplicate rows -------
SELECT 
	*
FROM( 
		select  
			*
			, ROW_NUMBER() OVER (PARTITION BY c.cst_id ORDER BY c.cst_create_date DESC) flag_recent
		from bronze.crm_cust_info c
) temp
WHERE
flag_recent = 1
;

select * from bronze.crm_cust_info
where cst_id is null OR cst_id = ''; 



-- 2) Check for unwanted SPACES in columns...
-- Expected: No Results
SELECT * FROM bronze.crm_cust_info
LIMIT 100;

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
; -- 15 customers with spaces in first name

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname)
; -- 17 customers with spaces in last name

SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status <> TRIM(cst_marital_status)
; -- 0 rows returned

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr <> TRIM(cst_gndr)
; -- 0 rows returned


-- 3) Data Standardisation & Consistency
-- cst_gndr
SELECT 
	CASE when cst_gndr = 'M' then 'Male'
		when cst_gndr = 'F' then 'Female'
		else 'n/a'
    end
    , count(*)
FROM bronze.crm_cust_info
group by 
	CASE when cst_gndr = 'M' then 'Male'
		when cst_gndr = 'F' then 'Female'
		else 'n/a'
    end
;

-- cst_marital_status
SELECT 
	CASE when UPPER(cst_marital_status) = 'M' then 'Married'
		when UPPER(cst_marital_status) = 'S' then 'Single'
		else 'n/a'
    end cst_marital_status
    , count(*)
FROM bronze.crm_cust_info
group by 
	CASE when UPPER(cst_marital_status) = 'M' then 'Married'
		when UPPER(cst_marital_status) = 'S' then 'Single'
		else 'n/a'
    end
;

-- Nothing to do for cst_create_date since we created the column as a date datatype only!


-- Check join conditions with other customer data tables ..
-- Do we have any customers with no location or no birthdate ?

SELECT DISTINCT SUBSTRING(cid, 4, LENGTH(cid)) from bronze.erp_cust_az12;

SELECT DISTINCT cid FROM bronze.erp_loc_a101;
SELECT REPLACE(cid, '-', '') AS cid FROM bronze.erp_loc_a101;

SELECT DISTINCT cst_key FROM bronze.crm_cust_info;

WITH cte_unique_cst_id AS(
	SELECT 
		*
	FROM( 
 		SELECT 
			*
 			, ROW_NUMBER() OVER (PARTITION BY c.cst_id ORDER BY c.cst_create_date DESC) flag_recent
		FROM bronze.crm_cust_info c
		) temp
 	WHERE flag_recent = 1
)
, clean_crm_cust_info AS(
	SELECT
		cst_id
		, cst_key
		, TRIM(cst_firstname) AS cst_firstname
		, TRIM(cst_lastname) AS cst_lastname
		, CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'n/a'
		END cst_marital_status
		, CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END cst_gndr
		, cst_create_date
	FROM cte_unique_cst_id
	WHERE
		cst_id <> 0 AND cst_id IS NOT NULL
)
SELECT
	*
FROM clean_crm_cust_info
WHERE cst_key NOT IN (SELECT DISTINCT SUBSTRING(cid, 4, LENGTH(cid)) from bronze.erp_cust_az12)
;  	-- For 7442 customers, we don't have birth dates in the table erp_cust_az12. And that's fine!

-- SELECT
-- 	*
-- FROM clean_crm_cust_info
-- WHERE cst_key NOT IN (SELECT DISTINCT REPLACE(cid, '-', '') from bronze.erp_loc_a101)
-- ;  -- All customers have locations in the table erp_loc_a101


-- A quick check for any unreferenced bad data in the erp_loc_a101 ... ?
-- SELECT * FROM bronze.erp_loc_a101
-- WHERE REPLACE(cid, '-', '') NOT IN (SELECT DISTINCT cst_key FROM clean_crm_cust_info);
-- Thankfully, no such rows!



/** 
	Script to check Bronze layer table crm_prd_info
    Check duplicates, handle missing values, handle extra spaces, check for join condition with the table crm_sales_details
**/

Use bronze;


SELECT
	prd_id
    , prd_key 
    , prd_nm
    , prd_cost
    , prd_line
    , prd_start_dt
    , prd_end_dt
FROM bronze.crm_prd_info LIMIT 100;

-- 1) Check for NULLs or DUPLICATES in the Primary key. A primary key must be unique and not null.
-- Expected: No Results
select count(*) from bronze.crm_prd_info; -- 397 total rows in the table
select count(distinct prd_id) from bronze.crm_prd_info; -- 397

select count(*) from bronze.crm_prd_info
where prd_id is null OR prd_id = '';  -- 0 rows with blanks here

select prd_id
	, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1  -- no rows with duplicate primary key exist!
;


-- Check for NULLs or DUPLICATES in prd_key.
-- Expected: No Results with nulls; duplicates can exist.
select count(*) from bronze.crm_prd_info; -- 397 total rows in the table
select count(distinct prd_key) from bronze.crm_prd_info; -- 295

select count(*) from bronze.crm_prd_info
where prd_key is null OR prd_key = '';  -- 0 rows with blanks here

select prd_key
	, count(*)
from bronze.crm_prd_info
group by prd_key
having count(*) > 1  -- 77 prd_key values with duplicate rows.
;


-- Split prd_key to separate out category id.. to be able to join this table with table erp_px_cat_g1v2 (refer data integration model !!)
-- After the transformation, check for unmatched rows from crm_prd_info with cat_id that does not match any value in the id column in erp_px_cat_g1v2.
SELECT
	prd_id
    , prd_key
    , REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id 	-- Extract 1st 5 characters
	, SUBSTRING(prd_key, 7) AS prd_key 							-- Extract all characters from the 7th position till the end
    , prd_nm
    , prd_cost
    , prd_line
    , prd_start_dt
    , prd_end_dt
FROM bronze.crm_prd_info
WHERE 
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') 
		NOT IN (
					SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2
				)
;  
-- "CO_PE" cat_id does not exist in the id column in erp_px_cat_g1v2 : 7 such rows.
-- That's expected as the id column in erp_px_cat_g1v2 is referencing the cat_id in crm_prd_info.

select id from erp_px_cat_g1v2;



-- -- Separate out the prd_key characters from the prd_key column
SELECT
	prd_id
    , prd_key
    , REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id 	-- Extract 1st 5 characters
	, SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key 	-- Extract all characters from the 7th position till the end
--     , SUBSTRING(prd_key, 7) AS prd_key		-- Alternate
    , prd_nm
    , prd_cost
    , prd_line
    , prd_start_dt
    , prd_end_dt
FROM bronze.crm_prd_info
;
select sls_prd_key from bronze.crm_sales_details;


-- Again to check our join condition between the 2 tables, we look for prd_key values that are in crm_prd_info but not in crm_sales_details ...
-- That is, do we have any products with no sales/orders ?
SELECT
	prd_id
    , prd_key
    , REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id 	-- Extract 1st 5 characters
	, SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key 	-- Extract all characters from the 7th position till the end
--     , SUBSTRING(prd_key, 7) AS prd_key		-- Alternate
    , prd_nm
    , prd_cost
    , prd_line
    , prd_start_dt
    , prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LENGTH(prd_key)) 
	NOT IN (
			select DISTINCT sls_prd_key from bronze.crm_sales_details					
		)
;  -- 220 rows where the products don't seem to have corresponding sales !!


select DISTINCT sls_prd_key from bronze.crm_sales_details;	

-- Let's verify using specific prd_key values .. 
select * from bronze.crm_sales_details
where 
	sls_prd_key LIKE 'FK%'
	-- sls_prd_key LIKE 'FR-M94%'
;	-- Verified: we have products with no sales/orders so far. And that's ok!


-- Also, check if there are any sales with no corresponding products? -- Bad data ??
-- Expected: should be none!!
select sls_prd_key from bronze.crm_sales_details
where sls_prd_key NOT IN (select SUBSTRING(prd_key, 7, LENGTH(prd_key)) from bronze.crm_prd_info)
;  -- Thankfully, 0 such rows!



-- -- Check for extra white spaces in prd_nm
SELECT
    prd_nm
FROM bronze.crm_prd_info
where prd_nm <> TRIM(prd_nm)
;   -- no such rows!



-- Check prd_cost : int type - any negatives or null values here ?
SELECT
	prd_id
    , prd_key
    , REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id 	-- Extract 1st 5 characters
	, SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key 	-- Extract all characters from the 7th position till the end
    , prd_nm
    , IFNULL(prd_cost, 0) AS prd_cst  -- Replace NULL with 0
    , prd_line
    , prd_start_dt
    , prd_end_dt
FROM bronze.crm_prd_info
where prd_cost IN (
	SELECT prd_cost
	FROM bronze.crm_prd_info
	where prd_cost < 0 OR prd_cost is null OR prd_cost = ''
)
;   -- 2 rows with nulls



-- Check prd_line : requires data standardisation to have user friendly terms
select distinct prd_line
from bronze.crm_prd_info
;  -- R,S,M,T, Nulls

select prd_line
from bronze.crm_prd_info
where prd_line <> TRIM(prd_line)
;

SELECT
	prd_id
    , prd_key
    , REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id 	-- Extract 1st 5 characters
	, SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key 	-- Extract all characters from the 7th position till the end
    , prd_nm
    , IFNULL(prd_cost, 0) AS prd_cst  -- Replace NULL with 0
    , CASE UPPER(TRIM(prd_line)) 	-- Quick case statement
		WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line
    , prd_start_dt
    , prd_end_dt
FROM bronze.crm_prd_info
;


-- -- Check the last 2 date columns: End date must not be earlier than the start date

select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt
;  -- OOps! all rows here seem to have incorrect end dates??

-- Possible solutions:
-- #1: Replace start date with end date & vice versa.. 
-- but this option will corrupt the logic in the data as, in the same year, this will have 2 costs for the same product line.
-- #2: Generate an end date using the start date of the next row of that product line & subtract it by 1 day.. 
-- That is, End Date = Start Date of the Next Record - 1

-- Let's build the logic with 2 specific product keys ...
select 
	prd_id
    , prd_key
    , prd_nm
    , IFNULL(prd_cost, 0) AS prd_cst  -- Replace NULL with 0
    , CAST(prd_start_dt AS DATE) AS prd_start_dt
    , prd_end_dt
    , CAST(SUBDATE(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY) as DATE) AS prd_end_dt_new
from bronze.crm_prd_info
where prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509-B')
;

-- Next note there is no time information in these 2 date columns.. so we can cast it to a simple date




/** 
	Script to check Bronze layer table crm_sales_details
    Check duplicates, handle missing values, handle extra spaces
**/

Use bronze;

SELECT * FROM bronze.crm_sales_details;
SELECT count(*) FROM bronze.crm_sales_details;  -- 60,398 rows


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
FROM bronze.crm_sales_details
where TRIM(sls_ord_num) <> sls_ord_num
;  -- no rows returned

-- Do we have duplicate values of sls_ord_num ?
select 
	sls_ord_num
    , count(*)
FROM bronze.crm_sales_details
group by sls_ord_num
having count(*) > 1
; -- multiple duplicates


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
FROM bronze.crm_sales_details
where TRIM(sls_prd_key) <> sls_prd_key
;  -- no rows returned

-- Do we have duplicate values of sls_prd_key ?
select 
	sls_prd_key
    , count(*)
FROM bronze.crm_sales_details
group by sls_prd_key
having count(*) > 1
; -- multiple duplicates

 
-- Do we have rows with duplicate values for both: <sls_ord_num, sls_prd_key> ?
select 
	sls_ord_num
	, sls_prd_key
    , count(*)
FROM bronze.crm_sales_details
group by sls_ord_num, sls_prd_key
having count(*) > 1
; -- no duplicates here!

-- Hence, the combination <sls_ord_num, sls_prd_key> uniquely identifies a row in this table.


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
FROM bronze.crm_sales_details
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
FROM bronze.crm_sales_details
where sls_cust_id NOT IN (select distinct cst_id from silver.crm_cust_info)
;  -- 0 rows returned!

-- Quick check for extra spaces ?
SELECT
	*
FROM bronze.crm_sales_details
where TRIM(sls_cust_id) <> sls_cust_id
; -- no rows


-- sls_order_dt : need to change format from int to date
SELECT * FROM bronze.crm_sales_details LIMIT 100;
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
FROM bronze.crm_sales_details
where sls_order_dt <= 0
;  -- 17 rows with 0 in dates! These need to be replaced with nulls before we can transform the column to a date

-- Check for invalid dates: check the length.. should be 8
SELECT
	sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
where length(sls_order_dt) != 8
; -- 2 rows with bad data that are not date values

-- check for date boundaries as per business rules
SELECT
	sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
where sls_order_dt > 20250331
; -- no rows

SELECT
	sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, CASE WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
		ELSE CONVERT(sls_order_dt, DATE)
    END AS sls_order_dt
	, sls_ship_dt
	, sls_due_dt
	, sls_sales
	, sls_quantity
	, sls_price
FROM bronze.crm_sales_details
;

-- Check sls_ship_dt
SELECT
	sls_ship_dt
FROM bronze.crm_sales_details
where sls_ship_dt <= 0
;  -- no such rows

-- Check for invalid dates: check the length.. should be 8
SELECT
	NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
where length(sls_ship_dt) != 8
; -- no such rows


-- check for date boundaries as per business rules
SELECT
	NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
where sls_ship_dt > 20250331
; -- no rows


SELECT
	sls_ord_num
	, sls_prd_key
	, sls_cust_id
	, CASE WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
		ELSE CONVERT(sls_order_dt, DATE)
    END AS sls_order_dt
    , CASE WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
		ELSE CONVERT(sls_ship_dt, DATE)
    END AS sls_ship_dt
	, sls_due_dt
	, sls_sales
	, sls_quantity
	, sls_price
FROM bronze.crm_sales_details
;


-- Check sls_due_dt
SELECT
	sls_due_dt
FROM bronze.crm_sales_details
where sls_due_dt <= 0
;  -- no such rows

-- Check for invalid dates: check the length.. should be 8
SELECT
	NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
where length(sls_due_dt) != 8
; -- no such rows


-- check for date boundaries as per business rules
SELECT
	NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
where sls_due_dt > 20250331
; -- no rows


-- Check shipping dates & due dates are same or after order dates
SELECT * FROM 
(
	SELECT
		sls_ord_num
		, sls_prd_key
		, sls_cust_id
		, CASE WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
			ELSE CONVERT(sls_order_dt, DATE)
		END AS sls_order_dt
		, CASE WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
			ELSE CONVERT(sls_ship_dt, DATE)
		END AS sls_ship_dt
		, CASE WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
			ELSE CONVERT(sls_due_dt, DATE)
		END AS sls_due_dt
		, sls_sales
		, sls_quantity
		, sls_price
	FROM 
		bronze.crm_sales_details
) temp
WHERE (temp.sls_ship_dt < temp.sls_order_dt) 
	OR (temp.sls_due_dt < temp.sls_order_dt)
;  -- no such rows!


-- sls_sales = sls_quantity * sls_price
-- None of the columns for sales, qty or price can be nulls or zeros..
SELECT * FROM bronze.crm_sales_details LIMIT 100;

SELECT
	sls_ord_num
	, sls_sales
	, sls_quantity
	, sls_price
FROM bronze.crm_sales_details
where sls_sales <> (sls_quantity * sls_price)
	OR sls_sales is null OR sls_sales <= 0
	OR sls_quantity is null OR sls_quantity <= 0
	OR sls_price is null OR sls_price <= 0
order by sls_sales, sls_quantity, sls_price
;  -- 35 such rows where sales are either null/negatives or invalid; quantity is fine; price also has zeros & negatives.

-- Correct the sales coulmn using sls_sales = sls_quantity * sls_price


SELECT
	sls_ord_num
    , sls_sales AS sls_sales_old
	, sls_quantity AS sls_quantity_old
	, sls_price AS sls_price_old
    , CASE WHEN sls_sales < 0 AND (ABS(sls_sales) = sls_quantity * sls_price) THEN ABS(sls_sales)
			WHEN (ABS(sls_sales) != sls_quantity * sls_price) AND (sls_price is not null AND sls_price != 0) THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
	END AS sls_sales
    , sls_quantity
    , CASE WHEN sls_price IS NULL OR sls_price = 0 THEN ROUND(sls_sales/NULLIF(sls_quantity, 0), 0) -- change to null if qty is 0 to avoid divide by zero problem.
			WHEN sls_price < 0 THEN ABS(sls_price)
			ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
where 
	sls_sales <> (sls_quantity * sls_price)
 	OR sls_sales is null OR sls_sales <= 0
 	OR sls_quantity is null OR sls_quantity <= 0
	OR sls_price is null OR sls_price <= 0
   -- sls_ord_num IN ('SO51259', 'SO51298', 'SO51387', 'SO51942', 'SO52187')
order by sls_sales, sls_quantity, sls_price
;

-- Check that my logic works on all rows now:
select * from (
	SELECT
		sls_ord_num
		, sls_sales AS sls_sales_old
		, sls_quantity AS sls_quantity_old
		, sls_price AS sls_price_old
		, CASE WHEN sls_sales < 0 AND (ABS(sls_sales) = sls_quantity * sls_price) THEN ABS(sls_sales)
				WHEN (ABS(sls_sales) != sls_quantity * sls_price) AND (sls_price is not null AND sls_price != 0) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
		END AS sls_sales
		, sls_quantity
		, CASE WHEN sls_price IS NULL OR sls_price = 0 THEN ROUND(sls_sales/NULLIF(sls_quantity, 0), 0) -- change to null if qty is 0 to avoid divide by zero problem.
				WHEN sls_price < 0 THEN ABS(sls_price)
				ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details
	) temp
where temp.sls_sales != temp.sls_quantity * temp.sls_price
; -- no rows returned!




/** 
	Script to check Bronze layer table erp_cust_az12
    Check duplicates, handle missing values, handle extra spaces
**/

Use bronze;

SELECT 
	cid
	, bdate
	, gen
FROM bronze.erp_cust_az12
;

select * from silver.crm_cust_info;

select count(*) from silver.crm_cust_info;  -- 18,484


-- Check for nulls in cid
SELECT 
	cid
	, bdate
	, gen
FROM bronze.erp_cust_az12
where cid is null OR cid = ''
; -- no nulls or blanks here

-- Duplicates in cid ?
SELECT 
	cid
	, count(*)
FROM bronze.erp_cust_az12
group by cid
having count(*) > 1
;  -- no duplicates exist


SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		ELSE cid
	END AS cid
	, COUNT(*)
FROM bronze.erp_cust_az12
group by CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END
having count(*) > 1
;  -- no duplicates exist!


-- Check referential integrity: all cid values here exist in crm_cust_info <cst_key> ?
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		ELSE cid
	END AS cid
	, bdate
	, gen
FROM bronze.erp_cust_az12
where CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		ELSE cid
	END NOT IN (select distinct cst_key from silver.crm_cust_info)
;  -- no such rows!


-- Check bdate: do we have rows with wierd bdates ??
select * from bronze.erp_cust_az12
where bdate < '1924-01-01'
;

select min(bdate), max(bdate) from bronze.erp_cust_az12;
-- 1916-02-10 to 9999-11-20. Wow!
-- bad data: bdate can't be in the future! So these need to be handled..

select * from bronze.erp_cust_az12
where bdate > now()
;  -- 16 such rows


-- Replace future birthdates with NULLs
select * from (
	SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END AS cid
		, CASE WHEN bdate > now() THEN NULL
			ELSE bdate
		END AS bdate
		, gen
	FROM bronze.erp_cust_az12
) temp
where temp.bdate is null OR bdate > now()   -- Verifying the fix
;



-- Check gender
select distinct gen from bronze.erp_cust_az12
;

SELECT
	gen AS gen_old
    , CASE 
			WHEN UPPER(TRIM(gen)) LIKE '%F%' THEN 'Female'
			WHEN UPPER(TRIM(gen)) LIKE '%M%' THEN 'Male'
			ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12
;



/** 
	Script to check Bronze layer table erp_loc_a101
    Check duplicates, handle missing values, check join condition
**/

Use bronze;

select count(*) from bronze.erp_loc_a101;  -- 18,484 rows

SELECT 
	cid
	, cntry
FROM bronze.erp_loc_a101
;

-- Check join condition with crm_cus_info
SELECT 
	TRIM(REPLACE(cid, '-', '')) as cid
	, cntry
FROM bronze.erp_loc_a101
where TRIM(REPLACE(cid, '-', '')) NOT IN (select distinct cst_key from silver.crm_cust_info)
; 


-- -- Check country
select distinct cntry from bronze.erp_loc_a101
order by cntry;

-- Need to standardize country names.. but just using TRIM is not working .. there is some extra char in the column values - we will check for \r (carriage return)
select 
	distinct cntry as old_cntry
    , CASE WHEN SUBSTRING(cntry, 1, 2) = 'DE' and CHAR_LENGTH(cntry) = 3 THEN 'matched'
           WHEN SUBSTRING(cntry, 1, 2) = '' and CHAR_LENGTH(cntry) <= 3 THEN 'n/a'
		ELSE 'Ignore'
      END as test_cntry
from bronze.erp_loc_a101
;   -- Not working for n/a

select 
	distinct cntry as old_cntry
	, CASE WHEN UPPER(TRIM(cntry)) LIKE 'US%' THEN 'United States'
			WHEN SUBSTRING(cntry, 1, 2) = 'DE' and CHAR_LENGTH(cntry) = 3 THEN 'Germany'
			WHEN LENGTH(TRIM(BOTH '' FROM cntry)) = 0 THEN 'n/a'
 		ELSE TRIM(cntry)
    END as cntry
from bronze.erp_loc_a101
;    -- Not working for n/a


-- Let's expose characters in the column.. 
select 
	distinct REPLACE(REPLACE(cntry, '\r', '*'), ' ', '?' ) as cntry
from bronze.erp_loc_a101
;

-- So, finally working logic is ..
select 
	distinct cntry as old_cntry
    , REPLACE(REPLACE(cntry, '\r', '*'), ' ', '?' ) as exposed_cntry 	-- column has carriage return and spaces!
    , CASE 
			WHEN TRIM(REPLACE(UPPER(cntry), '\r', '')) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(REPLACE(UPPER(cntry), '\r', '')) = 'DE' THEN 'Germany'
            WHEN TRIM(REPLACE(UPPER(cntry), '\r', '')) = '' THEN 'n/a'
		ELSE TRIM(REPLACE(cntry, '\r', ''))
      END as new_cntry
from bronze.erp_loc_a101
order by cntry
;




/** 
	Script to check Bronze layer table erp_px_cat_g1v2
    Check duplicates, handle missing values, check join condition
**/

Use bronze;

select count(*) from bronze.erp_px_cat_g1v2;  -- 37 rows

select
	id
	, cat
    , subcat
    , maintenance
from bronze.erp_px_cat_g1v2
;


-- Check for primary key: id

select * from bronze.erp_px_cat_g1v2 where id is null or id = '';
-- no nulls

select
	id
	, count(*)
from bronze.erp_px_cat_g1v2
group by id
having count(*) > 1
;  -- no duplicates


-- Check the join coniditon with crm_prd_info .. need to join with silver table crm_prd_info <cat_id>
select * from silver.crm_prd_info;

select * from bronze.erp_px_cat_g1v2
where id not in (select distinct cat_id from silver.crm_prd_info)
;  -- no such rows! join coniditon is good!


-- Check for extra spaces in the remaining columns ...
select *
from bronze.erp_px_cat_g1v2
where TRIM(cat) != cat OR TRIM(subcat) != subcat OR TRIM(maintenance) != maintenance
;  -- no rows. all good!


-- Checking Data Standardisation in the 3 descriptive columns ...
select distinct cat
from bronze.erp_px_cat_g1v2;

select distinct subcat
from bronze.erp_px_cat_g1v2;

select 
	distinct maintenance
from bronze.erp_px_cat_g1v2
;


-- select 
-- 	REPLACE(REPLACE(maintenance, '\r', '*'), ' ', '?') as old_maintenance
-- from bronze.erp_px_cat_g1v2
-- where TRIM(maintenance) != maintenance
-- ; 










