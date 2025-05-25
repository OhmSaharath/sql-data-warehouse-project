/*
  This's script for load data from datawarehouse. Process this script TRUNCATE TABLE for clear data in table and re-insert from CSV file.
  ** If, you see data path folder.  I'm use SQL server from docker. **
  Learning from this script :
    - TRUNCATE : Delete data in table not delete table.
    - BULK INSERT : Insert data big file size.
    - CREATE OR ALTER PROCEDURE : It's looklike create function in another language.
    - BEGIN TRY/END TRY, BEGIN CATCH/END CATCH : for validate this script if error this script will display a error message. (Track ETL duration
Helps to identify bottlenecks, optimize performance, monitor trends, detect issues)
    - DECLARE : it's for declare variable.
    - DATETIME FUNCTION : This function can calculate time for load the data. (each data and batch time)
*/
EXEC bronze.load_bronze -- execute function

CREATE OR ALTER PROCEDURE bronze.load_bronze AS -- Create Procedure for call this code again in load_bronze
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '====================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '====================================================';

		PRINT '----------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; -- Delete data in table but not delete structure in table
		PRINT '>> Inserting Data Into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info -- Insert data from csv file (my case make in docker)
		FROM '/data_warehouse/data/datasets/source_crm/cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM '/data_warehouse/data/datasets/source_crm/prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM '/data_warehouse/data/datasets/source_crm/sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------------';

		PRINT '----------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into : erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM '/data_warehouse/data/datasets/source_erp/CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM '/data_warehouse/data/datasets/source_erp/LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatin Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM '/data_warehouse/data/datasets/source_erp/PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '===================================================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '		-Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===================================================='
	END TRY
	BEGIN CATCH
		PRINT '===================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' +ERROR_MESSAGE();
		PRINT 'Error Message' +CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' +CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================='
	END CATCH
END
