/*
=============================================================
Create the DataWarehouse - Add 3 schemas in MySQL
=============================================================
Script Purpose:
    This script creates 3 new databases/schemas named 'bronze', 'silver' & 'gold'. 
    If a database exists, it is dropped and recreated.
    Note that in MySQL, physically, a schema is synonymous with a database.
    
WARNING:
    Running this script will drop the entire database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

DROP DATABASE IF EXISTS bronze;
CREATE DATABASE bronze;

DROP DATABASE IF EXISTS silver;
CREATE DATABASE silver;

DROP DATABASE IF EXISTS gold;
CREATE DATABASE gold;
