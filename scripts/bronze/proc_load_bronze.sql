/*
Stored Procedure: Load Bronze Layer (Source = Bronze)

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
*/
exec bronze.load_bronze
=
= 
=
create or alter procedure bronze.load_bronze as
begin
    declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
     begin try 
	   set @batch_start_time = getdate();
		   print '==============================================';
		   print 'loading the bronze layer';
		   print '=============================================';

		   print 'loading CRM tables'
		   print '-------------------'

		  set @start_time = GETDATE();  
			print '>> Truncating table<<'
			Truncate Table bronze.crm_cust_info;
			print '>> inserting data <<'
			bulk insert bronze.crm_cust_info
			from 'C:\Users\RAMIJ\Downloads\cust_info.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock 
			);

          set @end_time = GETDATE();
		  print '>> load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'     
		  
		  print '=============================================='
			--select * from bronze.crm_cust_info 
			--select count (*) from bronze.crm_cust_info   
			;

          set @start_time = GETDATE();
			print '>> Truncating table<<'
			truncate table bronze.crm_prd_info;
			print '>> inserting data <<'
			bulk insert bronze.crm_prd_info
			from 'C:\Users\RAMIJ\Downloads\prd_info.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
         set @end_time = GETDATE();
		 print '>> load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
			--select * from bronze.crm_prd_info 
			--select count (*) from bronze.crm_prd_info   
			;
			print '=============================================='

		set @start_time = GETDATE();	
			print '>> Truncating table<<'
			truncate table bronze.crm_sales_details;
			print '>> inserting data <<'
			bulk insert bronze.crm_sales_details
			from 'C:\Users\RAMIJ\Downloads\sales_details.csv'
			with(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
         set @end_time = GETDATE();
		 print '>> load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'    
			--select * from bronze.crm_sales_details
			--select count (*) from bronze.crm_sales_details 
			;

			print '=============================================='


         set @start_time = GETDATE();
			print '>> Truncating table<<'
			truncate table bronze.erp_loc_a101;
			print '>> inserting data <<'
			bulk insert bronze.erp_loc_a101
			from 'C:\Users\RAMIJ\Downloads\LOC_A101.csv'
			with(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
         set @end_time = GETDATE();
		 print '>> load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
			--select * from bronze.erp_loc_a101
			--select count (*) from bronze.erp_loc_a101 
			;

			print '=============================================='



         set @start_time = GETDATE();
			print '>> Truncating table<<'
			truncate table bronze.erp_cust_az12;
			print '>> inserting data <<'
			bulk insert bronze.erp_cust_az12
			from 'C:\Users\RAMIJ\Downloads\CUST_AZ12.csv'
			with(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
          set @end_time = GETDATE();
		  print '>> load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
			--select * from bronze.erp_cust_az12
			--select count (*) from bronze.erp_cust_az12
			;

			print '=============================================='


         set @start_time = GETDATE();
			print '>> Truncating table<<'
			truncate table bronze.erp_px_cat_g1v2;
			print '>> inserting data <<'
			bulk insert bronze.erp_px_cat_g1v2
			from 'C:\Users\RAMIJ\Downloads\PX_CAT_G1V2.csv'
			with(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
         set @end_time = GETDATE();
		 print '>> load duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
			--select * from bronze.erp_px_cat_g1v2
			--select count (*) from bronze.erp_px_cat_g1v2 
			;

			print '=============================================='
       set @batch_end_time = getdate();
	        print '=============================================='
			print 'loading bronze layer'
			print '>> toatl load duration:' + cast(datediff(second, @batch_start_time,@batch_end_time) as nvarchar) + ' seconds';
	 end try
		 begin catch
			 print '============================='
			 print 'Error occoured'
			 print 'Error Message' + Error_message();
			 print 'Error Message' + cast(Error_number() as nvarchar);
			 print 'Error message' + cast(Error_state() as nvarchar);
			 print '============================='
		 end catch
end
