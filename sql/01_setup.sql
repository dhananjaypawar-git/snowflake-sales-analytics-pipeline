-- # Choose correct role to setup account # --

// First use a powerful role (like ACCOUNTADMIN) only for setup.

use role accountadmin ;

// Create database for the project 

create or replace database sales_db;

use database sales_db;  -- Database = main container for your complete project.

// Create schemas ( Raw, Staging, Analytics)

create or replace schema raw; -- RAW → exactly same data as source (no changes)
create or replace schema staging; -- STAGING → cleaned/standardized data
create or replace schema analytics; -- ANALYTICS → final fact/dim tables for reporting.


-- # Create Warehouses (Compute) # --

// Now create separate warehouses for different workloads.

create or replace warehouse ingest_wh
with warehouse_size = 'xsmall'
auto_suspend = 60 -- 1 min
auto_resume = true
initially_suspended = true;

create or replace warehouse transform_wh
with warehouse_size = 'small'
auto_suspend = 120 -- 2 min
auto_resume = true
initially_suspended = true;

create or replace warehouse reporting_wh
with warehouse_size = 'xsmall'
auto_suspend = 300 -- 5 min
auto_resume = true
initially_suspended =true;

--(Why separate warehouses ->Ingestion jobs should not slow down reporting, Reporting queries should not consume ETL compute, You can control cost easily (auto-suspend)).

// Validate that everything is okay or not 

show schemas in database sales_db;
show warehouses;

-- how you setup environment -> “I separated the project into RAW, STAGING, ANALYTICS schemas and created dedicated warehouses for INGEST, TRANSFORM and REPORTING to isolate workloads and optimize cost.”
