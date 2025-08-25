/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze - > Silver)
===============================================================================
Script Purpose: 
	This stored procedure performs the ETL (Extract, Transform, Load) process to
	populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed: 
	-Truncate Silver Tables.
	-Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
	None.
	This stored Procedure does not accept any parameters or return any values.

Usage Examples:
	Exec silver.load_silver 
===============================================================================
*/

create or alter procedure silver.load_silver AS 
Begin 
	print '>> Truncating Table: silver.crm_cust_info';
	Truncate Table silver.crm_cust_info;
	print '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info( 
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date
	)

	select 
	cst_id,
	cst_key,
	trim(cst_firstname) AS cst_firstname, 
	trim(cst_lastname) AS cst_lastname, 
	case when upper(Trim(cst_material_status)) = 'S' then 'Single' 
		 when upper(Trim(cst_material_status)) = 'M' then 'Married'
		 else 'n/a'
	end cst_material_status,
	case when upper(Trim(cst_gndr)) = 'F' then 'Female' 
		 when upper(Trim(cst_gndr)) = 'M' then 'Male'
		 else 'n/a'
	end cst_gndr,
	cst_create_date
	from (
	select *, 
	row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null
	)t 
	where flag_last = 1 -- 



	-------------------------------------------------------------
	-------------------------------------------------------------

	--PRD
	print '>> Truncating Table: silver.crm_prd_info';
	Truncate Table silver.crm_prd_info;
	print '>> Inserting Data Into: silver.crm_prd_info';
	insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)

	select
	prd_id,
	replace(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, 
	SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key, 
	prd_nm,
	isnull(prd_cost, 0) AS prd_cost, 
	case upper(trim(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		Else 'n/a'
	end AS prd_line,
	cast(prd_start_dt as date) AS prd_start_dt, 
	cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 AS Date) AS prd_end_dt
	from bronze.crm_prd_info
	



	---------------------------------------------------------------------------------------
	---------------------------------------------------------------------------------------
	-- Sales_details
	print '>> Truncating Table: silver.crm_sales_details';
	Truncate Table silver.crm_sales_details;
	print '>> Inserting Data Into: silver.crm_sales_details';
	insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	Select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 OR len(sls_order_dt) != 8 then NULL
			else CAST(CAST(sls_order_dt AS varchar) AS date) 
			end as sls_order_dt,
		case when sls_ship_dt = 0 OR len(sls_ship_dt) != 8 then NULL
			else CAST(CAST(sls_ship_dt AS varchar) AS date) 
			end as sls_ship_dt,
		case when sls_due_dt = 0 OR len(sls_due_dt) != 8 then NULL
			else CAST(CAST(sls_due_dt AS varchar) AS date)
			end as sls_due_dt,
		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price)
		else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity, 0)
		else sls_price
		end as sls_price
		from bronze.crm_sales_details

	--------------------------------------------------------------------------------------
	--------------------------------------------------------------------------------------
	--erp_cust_az12
	print '>> Truncating Table: silver.erp_cust_az12';
	Truncate Table silver.erp_cust_az12;
	print '>> Inserting Data Into: silver.erp_cust_az12';
	insert into silver.erp_cust_az12
	(cid,
	 bdate,
	 gen
	)
	select 
	case when cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid))
		else cid
	end AS cid,
	case when bdate > GETDATE() then null
		else bdate
	end as bdate,
	case when upper(trim(gen)) IN ('F', 'Female') Then 'Female'
		 when upper(trim(gen)) IN ('M', 'Male') Then 'Male'
		 else 'n/a'
	end as gen
	from bronze.erp_cust_az12


	--------------------------------------------------------------------------------------
	--------------------------------------------------------------------------------------
	--erp_loc_a101
	print '>> Truncating Table: silver.erp_loc_a101';
	Truncate Table silver.erp_loc_a101;
	print '>> Inserting Data Into: silver.erp_loc_a101';
	insert into silver.erp_loc_a101(cid, cntry)
	select 
	Replace (cid, '-', '') cid,
	case when trim(cntry) = 'DE' Then 'Germany'
		 when trim(cntry) IN ('US', 'USA') Then 'United States'
		 when trim(cntry) = '' OR cntry IS NULL Then 'n/a'
		 else trim(cntry) 
	end as cntry
	from bronze.erp_loc_a101


	--------------------------------------------------------------------
	--------------------------------------------------------------------
	-- erp_px_cat_g1v2
	print '>> Truncating Table: silver.erp_px_cat_g1v2';
	Truncate Table silver.erp_px_cat_g1v2;
	print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
	select 
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2
end
