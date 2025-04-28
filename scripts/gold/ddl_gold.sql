/*

DDL Script: Create Gold Views

Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


-- Create Dimension: gold.dim_customers


if OBJECT_ID('Gold.Dim_Customers', 'v') is not null
	drop view Gold.Dim_Customers;
	
go

create view Gold.Dim_Customers as
select 
    row_number() over(order by cst_id) as Customer_Key,
	ci.cst_id             as         Customer_id,
	ci.cst_key            as         Customer_numbers,
	ci.cst_firstname      as         Fitst_name,
	ci.cst_lastname       as         Last_name,
		case when ci.cst_gndr != 'n/a'
		     then coalesce(ca.gen, 'n/a')
			 else ca.gen
	    end                as         Gender,
    la.cntry               as         Country,
	ci.cst_marital_status  as         marital_status,
	ca.bdate               as         Birthdate,
	ci.cst_create_date     as         Create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca
on     ci.cst_key = ca.cid
left join silver.erp_loc_a101 la  
on     ci.cst_key = la.cid

select * from gold.Dim_Customers


-- Create Dimension: gold.dim_products

if OBJECT_ID ('Gold.dim_Product', 'v') is not null
	drop view Gold.dim_Product;
go

create view Gold.dim_Product as
SELECT
  ROW_NUMBER() over(order by pn.prd_key) as Product_key,
    pn.prd_id   as Product_id,      
    pn.prd_key  as Product_number,      
    pn.prd_nm   as Product_name,      
    pn.cat_id   as Category_id,        
    pn.prd_cost as Product_cost,     
    pn.prd_line as Product_line,
	pc.cat      as Category,
	pc.subcat   as Subcategory,
	pc.maintenance as Maintenece,
    pn.prd_start_dt as Start_date,
	pn.prd_end_dt    as End_date
FROM silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null

select * from gold.dim_Product

-- Create Fact Table: gold.fact_sales

  if OBJECT_ID('gold.fact_sales', 'v') is not null
	drop view gold.fact_sales
go

create view gold.fact_sales as
select 
pr.product_key AS product_key,
cu.Customer_Key AS customer_key,
sd.sls_ord_num AS order_number,
sd.sls_new_ord_dt AS order_date,
sd.sls_new_ship_dt AS shipping_date,
sd.sls_new_due_dt AS due_date,
sd.sls_new_price AS price,
sd.sls_new_sales AS sales_amount,
sd.sls_quantity AS quantity
from silver.crm_sales_details sd
left join gold.dim_Product pr
on sd.sls_prd_key = pr.Product_number
left join gold.Dim_Customers cu
on sd.sls_cust_id = cu.Customer_id

select * from gold.fact_sales


