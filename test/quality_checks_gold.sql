/*
===============================================================================
Quality Check
===============================================================================
Script Purpose:
	This script performs quality checks to validate the integrity, consistency,
	and accuracy of the Gold Layer. These checks ensures:
		- Uniqueness of surrogate keys in dimension tables.
		- Referential integrity between fact and dimension tables.
		- Validation of relationship in the data model for analytical purposes.

	Usage Notes:
		- Run these checks after data loading silver Layer.
		- Investigate and resolve any discrepancies found during the checks
===============================================================================
*/

-- ==========================================================
-- Check 'gold.dim_customers'
-- ==========================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
select customer_key, 
count(*) AS duplicate_count
from gold.dim_customers
group by Customer_Key
having count(*) > 1;

-- ==========================================================
-- Check 'gold.dim_product'
-- ==========================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
select product_key, 
count(*) AS duplicate_count
from gold.dim_product
group by Product_Key
having count(*) > 1;

-- ==========================================================
-- Check 'gold.fact_sales'
-- ==========================================================
-- Check the data model connectivity between fact and dimensions
select * 
from gold.fact_sales f
left join gold.dim_customers c
ON c.Customer_Key = f.Customer_Key
left join gold.dim_product p 
ON p.product_key = f.Product_Key
where p.Product_Key is null OR c.Customer_Key is null
