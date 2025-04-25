truncate table silver.crm_cust_info
insert into silver.crm_cust_info(
		cst_id,
        cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

select 
cst_id,
cst_key,
trim (cst_firstname) as cst_firstname,
trim (cst_lastname) as cst_lastname,

	case when upper(trim (cst_gender)) = 'F' then 'FEMALE'
		 when upper(trim (cst_gender)) = 'M' then 'MALE'
		 else 'N/A'
    end as cst_gender,

	case when upper(trim(cst_marital_staus)) = 'S' then 'SINGLE'
	     when upper(trim(cst_marital_staus)) = 'M' then 'MARRIED'
		 else 'N/A'
     end as cst_marital_staus,

cst_create_date

from (
	select *,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
)t where flag_last = 1 

delete from silver.crm_cust_info where cst_id is null

select * from silver.crm_cust_info

