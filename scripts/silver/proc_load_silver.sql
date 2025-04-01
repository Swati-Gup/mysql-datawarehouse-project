/**
	==== ==== Stored Procedure : Load Silver Layer (Bronze -> Silver) ==== ====
    Script Purpose:
      This stored procedure performs the ETL (Extract, Transform, Load) process to 
      populate the 'silver' schema tables from the 'bronze' schema.
	  
    Actions Performed:
  		- Truncates Silver tables.
  		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
    Parameters:
      None. 
  	  This stored procedure does not accept any parameters or return any values.

    Usage Example:
      CALL silver.proc_load_silver;
    
  *** WARNING ***
	  This script can truncate Silver layer tables. Be careful when using this and keep backups.
    
**/

-- CALL silver.proc_load_silver;


DROP PROCEDURE IF EXISTS silver.proc_load_silver;

DELIMITER //
CREATE PROCEDURE silver.proc_load_silver()
BEGIN
	DECLARE start_time, end_time, batch_start_time, batch_end_time DATETIME;
	SET @batch_start_time = now();
        
	/** ==== Loading Table: silver.crm_cust_info ==== **/
	SET @start_time = now();
	SELECT CONCAT('**** Truncating table: silver.crm_cust_info at ', @start_time);
	TRUNCATE TABLE silver.crm_cust_info;
    
	INSERT INTO silver.crm_cust_info(
			cst_id
			, cst_key
			, cst_firstname
			, cst_lastname
			, cst_marital_status
			, cst_gndr
			, cst_create_date
	)
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
	SELECT
			cst_id
			, TRIM(cst_key) AS cst_key
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
			cst_id <> 0 AND cst_id IS NOT NULL;
	SET @end_time = now();
	SELECT CONCAT('**** Load duration for crm_cust_info: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time));


	/** ==== Loading Table: silver.crm_prd_info ==== **/
    SET @start_time = now();
    SELECT CONCAT('**** Truncating table: silver.crm_prd_info at ', @start_time);
	TRUNCATE TABLE silver.crm_prd_info;
    
	INSERT INTO silver.crm_prd_info(
		prd_id
		, cat_id
		, prd_key
		, prd_nm
		, prd_cost
		, prd_line 
		, prd_start_dt
		, prd_end_dt 
	)
	SELECT
		prd_id
		, REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id -- Derive a new column: Extract 1st 5 characters as CAT_ID
		, SUBSTRING(prd_key, 7) AS prd_key 		-- Extract all characters from the 7th position till the end as PRD_KEY
		, prd_nm
		, IFNULL(prd_cost, 0) AS prd_cst 		-- Replace NULL with 0: Handled missing values
		 , CASE UPPER(TRIM(prd_line)) 			-- Data Standardisation: Map product line to descriptive values
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line
		, CAST(prd_start_dt AS DATE) AS prd_start_dt	-- Removed the time as there was no data
		, CAST(SUBDATE(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY) AS DATE) AS prd_end_dt 
			-- Data Enrichment: calculated the End date as 1 day before the next start date
	FROM bronze.crm_prd_info;
	SET @end_time = now();
    SELECT CONCAT('**** Load duration for crm_prd_info: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time));
	
    
    /** ==== Loading Table: silver.crm_sales_details ==== **/
	SET @start_time = now();
    SELECT CONCAT('**** Truncating table: silver.crm_sales_details at ', @start_time);
	TRUNCATE TABLE silver.crm_sales_details;
    
	INSERT INTO silver.crm_sales_details(
		sls_ord_num
		, sls_prd_key
		, sls_cust_id
		, sls_order_dt
		, sls_ship_dt
		, sls_due_dt
		, sls_sales
		, sls_quantity
		, sls_price
	)
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
		, CASE WHEN sls_sales < 0 AND (ABS(sls_sales) = sls_quantity * sls_price) THEN ABS(sls_sales)
				WHEN (ABS(sls_sales) != sls_quantity * sls_price) AND (sls_price is not null AND sls_price != 0) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
		END AS sls_sales
		, sls_quantity
		, CASE WHEN sls_price IS NULL OR sls_price = 0 THEN ROUND(sls_sales / NULLIF(sls_quantity, 0), 0) -- change to null if qty is 0 to avoid divide by zero problem.
				WHEN sls_price < 0 THEN ABS(sls_price)
				ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details;
	SET @end_time = now();
    SELECT CONCAT('**** Load duration for crm_sales_details: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time));


	/** ==== Loading Table: silver.erp_cust_az12 ==== **/
	SET @start_time = now();
    SELECT CONCAT('**** Truncating table: silver.erp_cust_az12 at ', @start_time);
	TRUNCATE TABLE silver.erp_cust_az12;
    
	INSERT INTO silver.erp_cust_az12(
		cid
		, bdate
		, gen
	)
	SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END AS cid
		, CASE WHEN bdate > now() THEN NULL
				ELSE bdate
		END AS bdate
		, CASE 
				WHEN UPPER(TRIM(gen)) LIKE '%F%' THEN 'Female'
				WHEN UPPER(TRIM(gen)) LIKE '%M%' THEN 'Male'
				ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12;
	SET @end_time = now();
    SELECT CONCAT('**** Load duration for erp_cust_az12: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time));

	
	/** ==== Loading Table: silver.erp_loc_a101 ==== **/
	SET @start_time = now();
    SELECT CONCAT('**** Truncating table: silver.erp_loc_a101 at ', @start_time);
	TRUNCATE TABLE silver.erp_loc_a101;
    
	INSERT INTO silver.erp_loc_a101(
		cid
		, cntry
	)
	SELECT
		TRIM(REPLACE(cid, '-', '')) as cid
		, CASE 
				WHEN TRIM(REPLACE(UPPER(cntry), '\r', '')) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(REPLACE(UPPER(cntry), '\r', '')) = 'DE' THEN 'Germany'
				WHEN TRIM(REPLACE(UPPER(cntry), '\r', '')) = '' THEN 'n/a'
				ELSE TRIM(REPLACE(cntry, '\r', ''))
		  END as cntry
	FROM bronze.erp_loc_a101;
	SET @end_time = now();
    SELECT CONCAT('**** Load duration for erp_loc_a101: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time));
    
    
    
    /** ==== Loading Table: silver.erp_px_cat_g1v2 ==== **/
	SET @start_time = now();
    SELECT CONCAT('**** Truncating table: silver.erp_px_cat_g1v2 at ', @start_time);
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
    
	INSERT INTO silver.erp_px_cat_g1v2(
		id
		, cat
		, subcat
		, maintenance
	)
	SELECT
		id
		, cat
		, subcat
		, maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time = now();
    SELECT CONCAT('**** Load duration for erp_px_cat_g1v2: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time));

	SET @batch_end_time = now();
	SELECT CONCAT('**** Total Silver Layer Load duration: ', TIMESTAMPDIFF(SECOND, @batch_start_time, @batch_end_time));
        
END //
