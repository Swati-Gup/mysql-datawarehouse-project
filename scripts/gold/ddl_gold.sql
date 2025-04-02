/**
	==== BUILDING the GOLD Layer : Create Gold Views ====
    
    Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

	Usage:
    - These views can be queried directly for analytics and reporting.
**/

select * from silver.crm_cust_info;
select * from silver.erp_cust_az12;
select * from silver.erp_loc_a101;
SELECT COUNT(*) FROM silver.crm_cust_info;  -- 18,484


## 1 - Building the Dimension CUSTOMERS View -- collecting all the customer information from 3 tables - crm_cust_info, erp_cust_az12, erp_loc_a101
WITH cte_customer_data AS(
	SELECT
		cci.cst_id
		, cci.cst_key
		, cci.cst_firstname
		, cci.cst_lastname
		, cci.cst_marital_status
		, cci.cst_gndr
		, cci.cst_create_date
		, eca.bdate
		, eca.gen
		, eloc.cntry
	FROM 
		silver.crm_cust_info cci
			LEFT JOIN silver.erp_cust_az12 eca ON cci.cst_key = eca.cid
			LEFT JOIN silver.erp_loc_a101 eloc ON cci.cst_key = eloc.cid
)
-- SELECT * FROM cte_customer_data;
-- SELECT COUNT(*) FROM cte_customer_data;  -- 18,484: same as in original master table silver.crm_cust_info, so no duplicate rows expected after the join.

-- Check for duplicate rows after the joins
SELECT cst_id, count(*)
FROM cte_customer_data
GROUP BY cst_id
HAVING count(*) > 1;  -- no rows!


-- Sort out data integration issues in the above join like in columns: cst_gndr & gen ...
SELECT
	cci.cst_firstname
    , cci.cst_lastname
	, cci.cst_gndr
	, eca.gen
    , CASE WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr -- CRM is the master for gender info
			ELSE COALESCE(eca.gen, 'n/a')				-- ELSE case is when CRM is n/a: then if ERP is not n/a then pick the value from ERP, else n/a
	END AS gender
FROM 
	silver.crm_cust_info cci
		LEFT JOIN silver.erp_cust_az12 eca ON cci.cst_key = eca.cid
		LEFT JOIN silver.erp_loc_a101 eloc ON cci.cst_key = eloc.cid
-- where cci.cst_gndr != eca.gen OR (eca.gen = 'n/a' and cci.cst_gndr = 'n/a') -- temporary to check the problem cases
;

## -- Finally: the Dimension CUSTOMERS - we build the view
-- Rename the columns to user friendly names.
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cci.cst_id) AS customer_key  -- Surrogate key (system generated primary key) - 1 for each cci.cst_id
	, cci.cst_id AS customer_id
	, cci.cst_key AS customer_number
	, cci.cst_firstname AS first_name
	, cci.cst_lastname AS last_name
	, eloc.cntry AS country
	, cci.cst_marital_status AS marital_status
	, CASE WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr -- CRM is the master for gender info
			ELSE COALESCE(eca.gen, 'n/a')				-- ELSE case is when CRM is n/a: then if ERP is not n/a then pick the value from ERP, else n/a
	END AS gender
	, eca.bdate AS birthdate
	, cci.cst_create_date AS customer_create_date
FROM 
	silver.crm_cust_info cci
		LEFT JOIN silver.erp_cust_az12 eca ON cci.cst_key = eca.cid
		LEFT JOIN silver.erp_loc_a101 eloc ON cci.cst_key = eloc.cid
;


## Quality check for gold.dim_customers
DESCRIBE gold.dim_customers;
SELECT * FROM gold.dim_customers;

SELECT DISTINCT gender FROM gold.dim_customers;  -- Male, Female, n/a: all ok!
SELECT DISTINCT marital_status FROM gold.dim_customers; 




## 2 - Building the Dimension PRODUCTS -- collecting all the product information from 2 tables - crm_prd_info, erp_px_cat_g1v2
-- Note that tablel contains all historical as well as current data of a product. For the purpose of the gold layer & dependng on 
-- business requirements, we will choose to keep only the latest record for each product in this view (where prd_end_dt is null).
-- Rename the columns to user friendly names.
SELECT COUNT(*) FROM silver.crm_prd_info; -- 397 rows
SELECT * FROM silver.crm_prd_info;
SELECT * FROM silver.erp_px_cat_g1v2;

WITH cte_dim_products AS(
SELECT
	cpi.prd_id AS product_id
	, cpi.prd_key AS product_number
    , cpi.prd_nm AS product_name
    , cpi.cat_id AS category_id
	, epcat.cat AS category
    , epcat.subcat AS subcategory
    , epcat.maintenance
    , cpi.prd_cost AS product_cost
    , cpi.prd_line AS product_line
    , cpi.prd_start_dt AS product_start_date
FROM silver.crm_prd_info cpi
		LEFT JOIN silver.erp_px_cat_g1v2 epcat ON cpi.cat_id = epcat.id
WHERE cpi.prd_end_dt IS NULL	-- Pick the latest record for each product/product key
)
SELECT * FROM cte_dim_products;


-- Check the join has not introduced duplicate rows: use prd_key
-- SELECT COUNT(*) FROM cte_dim_products; -- 295 rows
-- SELECT prd_key, count(*)
-- FROM cte_dim_products
-- GROUP BY prd_key
-- HAVING count(*) > 1  -- no rows returend hence no 2 rows have the same prd_key!
-- ;

SELECT COUNT(*) FROM silver.crm_prd_info where prd_end_dt IS NULL; -- 295 rows


## -- Finally: the Dimension PRODUCTS - we build the view

CREATE VIEW gold.dim_products AS(
	SELECT
		ROW_NUMBER() OVER(ORDER BY cpi.prd_start_dt, cpi.prd_key) AS product_key  -- Surrogate key (system generated primary key) - basically 1 for each cpi.prd_key
		, cpi.prd_id AS product_id
		, cpi.prd_key AS product_number
		, cpi.prd_nm AS product_name
		, cpi.cat_id AS category_id
		, epcat.cat AS category
		, epcat.subcat AS subcategory
		, epcat.maintenance
		, cpi.prd_cost AS product_cost
		, cpi.prd_line AS product_line
		, cpi.prd_start_dt AS product_start_date
	FROM silver.crm_prd_info cpi
			LEFT JOIN silver.erp_px_cat_g1v2 epcat ON cpi.cat_id = epcat.id
	WHERE cpi.prd_end_dt IS NULL
);

## Quality check for gold.dim_products
DESCRIBE gold.dim_products;
SELECT * FROM gold.dim_products;



## 3 - Building the FACT table SALES -- Join with the dimension tables in the Gold layer built above.
/** silver.crm_sales_details <sls_prd_key> -- column to join with gold.dim_products <product_number>
	silver.crm_sales_details <sls_cust_id>	-- column to join with gold.dim_customers <customer_id>
	Then we need to include the sys generated surrogate keys from the 2 dimension tables 
    in the final fact table.. instead of the silver layer source system keys.
    Rename the columns to user friendly names.
**/
SELECT * FROM silver.crm_sales_details;
SELECT * FROM gold.dim_products;
SELECT * FROM gold.dim_customers;

CREATE VIEW gold.fact_sales AS(
	SELECT 
		csales.sls_ord_num AS order_number
		, pr.product_key		-- sys generated surrogate key of gold.dim_products
		, cust.customer_key		--  sys generated surrogate key of gold.dim_customers
		, csales.sls_order_dt AS order_date
		, csales.sls_ship_dt AS shipping_date
		, csales.sls_due_dt AS due_date
		, csales.sls_sales AS sales_amount
		, csales.sls_quantity AS order_quantity
		, csales.sls_price AS price
	FROM silver.crm_sales_details csales
			LEFT JOIN gold.dim_products pr ON csales.sls_prd_key = pr.product_number
			LEFT JOIN gold.dim_customers cust ON csales.sls_cust_id = cust.customer_id
);

## Quality check for gold.fact_sales
DESCRIBE gold.fact_sales;
SELECT * FROM gold.fact_sales;

-- Check the referential integrity of the data in the fact table: join the fact table with the 2 dimension tables
SELECT 
	* 
FROM gold.fact_sales f 
		LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
        LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL
;	-- no rows returned.


