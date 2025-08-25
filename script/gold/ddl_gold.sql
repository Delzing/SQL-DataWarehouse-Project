/*
============================================================================
DDL Script: Create Gold Views
============================================================================
Script Purpose:
	This script create views for the Gold layer in the data warehouse.
	This Gold layer represents the final dimension and fact tables (star schema)

	Each View performs transformations and combines data from the silver layer
	to produce a clean, enriched, and business-ready dataset. 

Usage:
	- These views can be queried directly for analytics and reporting
==============================================================================
*/


-- ===========================================================================
-- Creating Dimension: gold.dim_customers
-- ===========================================================================
if OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
go
create view gold.dim_customers as
select 
	ROW_NUMBER() over (order by cst_id) Customer_Key,
	ci.cst_id as Customer_ID,
	ci.cst_key as Customer_Number,
	ci.cst_firstname as First_Name,
	ci.cst_lastname as Last_Name,
	la.cntry As Country,
	ci.cst_material_status as Marital_Status,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr
		 else coalesce(ca.gen, 'n/a')
	End As Gender,
	ca.bdate As Birtdate,
	ci.cst_create_date As Create_Date	
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
ON ci.cst_key = la.cid


-- ===========================================================================
-- Creating Dimension: gold.dim_product
-- ===========================================================================
if OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
	DROP VIEW gold.dim_product;
go
create view gold.dim_product AS
select
	ROW_NUMBER() over(order by pn.prd_start_dt, pn.prd_key ) As Product_Key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name, 
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
where prd_end_dt is null


-- ===========================================================================
-- Creating Dimension: gold.fact_sales
-- ===========================================================================
if OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
go
create view gold.fact_sales as 
select 
	sd.sls_ord_num as order_number,
	pr.product_Key,
	cu.customer_Key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price
	from silver.crm_sales_details sd
	left join gold.dim_product pr
	ON sd.sls_prd_key = pr.product_number
	left join gold.dim_customers cu
	ON sd.sls_cust_id = cu.Customer_ID
