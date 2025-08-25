/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy,
	and standardization across the 'silver' schema. It includes checks for:
	- Null or duplicate primary keys.
	- Unwanted spaces in string fields.
	- Data standardization and consistency.
	- Invalid date ranges and orders.
	- Data consistency between related fieds.

Usage Notes:
	- Run these checks after data loading silver layer.
	- Investigate and resolve and discrepencies found during the checks.
*/

---------------------------------------------------------------------------
-------------------------------------------
--Cust_Info Table Quality Check

--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
select cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 OR cst_id is Null

--Check for unwanted spaces
--Expectation: No Results
select cst_firstname 
from silver.crm_cust_info 
where cst_firstname != Trim(cst_firstname)

--Data Standardization & Consistency
select distinct cst_gndr
from silver.crm_cust_info

select * from silver.crm_cust_info

---------------------------------------------------------------------------
-------------------------------------------
--Prd_info Table Quality Check

-- Quality Checks
-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result
select prd_id, 
count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

--Check for unwanted spaces
--Expectations: No Results
Select prd_nm 
from silver.crm_prd_info
where prd_nm != Trim(prd_nm)

--Check for Nulls or Negative Numbers
--Expectations: No Results
select prd_cost 
from silver.crm_prd_info
where prd_cost < 0 OR prd_cost is Null

--Data Standardization & Consistency
select distinct prd_line 
from silver.crm_prd_info

--Check for Invalid Date Orders
select * 
from silver.crm_prd_info
where prd_end_dt < prd_start_dt

select * 
from silver.crm_prd_info

---------------------------------------------------------------------------
-------------------------------------------
--sales_details Table Quality Check

--Check for invalid dates 
select 
nullif(sls_due_dt,0) sls_due_dt
from silver.crm_sales_details 
where sls_due_dt <= 0
OR len(sls_due_dt) !=8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

--Check for invalid Date Orders
select *
from silver.crm_sales_details 
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check for Data Consistency: Between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative
select distinct
sls_sales,
sls_quantity,
sls_price 
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales is null OR sls_quantity is null OR sls_price is null
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
order by sls_sales, sls_quantity, sls_price

select * from silver.crm_sales_details

---------------------------------------------------------------------------
-------------------------------------------
--cust_az12 Table Quality Check

--Identify Out-of-Range Dates
select distinct 
bdate 
from silver.erp_cust_az12 
where bdate <'1924-01-01' OR bdate > GETDATE()

--Data Standardization & Consistency
select distinct 
gen 
from silver.erp_cust_az12

select * from silver.erp_cust_az12

---------------------------------------------------------------------------
-------------------------------------------
--loc_a101 Table Quality Check

-- Data Standardization & Consistency
select distinct cntry
from silver.erp_loc_a101
order by cntry

select* from silver.erp_loc_a101


---------------------------------------------------------------------------
-------------------------------------------
--px_cat_g1v2 Table Quality Check

--Check for unwanted spaces
select * from 
silver.erp_px_cat_g1v2
where cat != Trim(cat) OR subcat != Trim(subcat) OR maintenance != Trim(maintenance)

--Data Standardization & Consistency
select distinct 
maintenance 
from silver.erp_px_cat_g1v2
