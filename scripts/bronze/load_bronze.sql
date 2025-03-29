/*
===============================================================================
 Load Bronze Layer (Data Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `LOAD DATA LOCAL INFILE` command to load data from csv Files to bronze tables.



To load the data, I resolved the following errors: 
>> Error Code: 3948. Loading local data is disabled; this must be enabled on both the client and server sides.
>> Error Code: 2068. LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.

To resolve this, I had to do 3 steps:

1) Enable local_infile on the Server
	This can be set dynamically in the script by executing the following statement:
    SET GLOBAL local_infile = 1;
    
2) Enable local_infile on the Client
	For this, in the MySQL Workbench, 'Database' dropdown menu, click 'Manage Connections'.
    Edit the connection: on the Connection tab, go to the 'Advanced' sub-tab, and in the 'Others:' box,
    add the line 'OPT_LOCAL_INFILE=1'
    
3) Restart MySql Workbench !!

NOTE: MySQL does not allow LOAD DATA to run within a stored procedure?? So, in real world projects, this script will need to run say, daily, so we need an external workflow mechanism to
execute this script automatically at a predefined time. In MS SQL Server, we can use bulk insert within a stored procedure.

===============================================================================
*/

SET GLOBAL local_infile = 1;

-- First, truncate the table and then Load cust_info.csv
TRUNCATE TABLE bronze.crm_cust_info;

LOAD DATA LOCAL INFILE '/Users/vcswg/Documents/Swati/Data Projects/mysql-data-warehouse/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

SELECT count(*) FROM bronze.crm_cust_info;  -- 18494
SELECT * FROM bronze.crm_cust_info;


-- Loading prd_info.csv
TRUNCATE TABLE bronze.crm_prd_info;

LOAD DATA LOCAL INFILE '/Users/vcswg/Documents/Swati/Data Projects/mysql-data-warehouse/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

SELECT count(*) FROM bronze.crm_prd_info;  -- 397
SELECT * FROM bronze.crm_prd_info;


-- Loading sales_details.csv
TRUNCATE TABLE bronze.crm_sales_details;

LOAD DATA LOCAL INFILE '/Users/vcswg/Documents/Swati/Data Projects/mysql-data-warehouse/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

SELECT count(*) FROM bronze.crm_sales_details;  -- 60,398
SELECT * FROM bronze.crm_sales_details;


-- Loading CUST_AZ12.csv
TRUNCATE TABLE bronze.erp_cust_az12;

LOAD DATA LOCAL INFILE '/Users/vcswg/Documents/Swati/Data Projects/mysql-data-warehouse/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

SELECT count(*) FROM bronze.erp_cust_az12;  -- 18,484
SELECT * FROM bronze.erp_cust_az12;


-- Loading LOC_A101.csv
TRUNCATE TABLE bronze.erp_loc_a101;

LOAD DATA LOCAL INFILE '/Users/vcswg/Documents/Swati/Data Projects/mysql-data-warehouse/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

SELECT count(*) FROM bronze.erp_loc_a101;  -- 18,484
SELECT * FROM bronze.erp_loc_a101;


-- Loading PX_CAT_G1V2.csv
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE '/Users/vcswg/Documents/Swati/Data Projects/mysql-data-warehouse/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

SELECT count(*) FROM bronze.erp_px_cat_g1v2;  -- 37
SELECT * FROM bronze.erp_px_cat_g1v2;

