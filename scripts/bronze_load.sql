/*
====================================================================================
Procedure Name: bronze.load_bronze

Description:
This stored procedure loads all raw source files into the Bronze layer of the
Retail Inventory Data Warehouse.

Key Characteristics of Bronze Layer:
- Stores raw, untransformed data exactly as received from source files.
- No business rules, cleansing, or standardization applied at this stage.
- Designed for high-speed ingestion and traceability.
- Acts as the foundation for downstream Silver layer transformations.

Execution Flow:
1. Truncate target Bronze table.
2. Bulk load corresponding CSV file.
3. Log load duration.
4. Repeat for all source entities.

====================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    BEGIN TRY
        PRINT '============================================';
        PRINT 'Loading Bronze Layer'
        PRINT '============================================';

        SET @start_time = GETDATE();
        PRINT'>>Truncating Table: bronze.crm_cust_registry';
        TRUNCATE TABLE bronze.crm_cust_registry;

        PRINT'>>Inserting into Table: bronze.crm_cust_registry';
        BULK INSERT bronze.crm_cust_registry
        FROM 'C:\Users\kadiy\Downloads\retail_inventory\dataset\customer_registry.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------';
       
       --====================================================================================================
        SET @start_time = GETDATE();
        PRINT'>>Truncating Table: bronze.erp_invt_mov';
        TRUNCATE TABLE bronze.erp_invt_mov;

        PRINT'>>Inserting into Table: bronze.erp_invt_mov';
        BULK INSERT bronze.erp_invt_mov
        FROM 'C:\Users\kadiy\Downloads\retail_inventory\dataset\inventory_movements_2023_2025.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------';

        --====================================================================================================
        SET @start_time = GETDATE();
        PRINT'>>Truncating Table: bronze.erp_product_catalog';
        TRUNCATE TABLE bronze.erp_product_catalog;

        PRINT'>>Inserting into Table: bronze.erp_product_catalog';
        BULK INSERT bronze.erp_product_catalog
        FROM 'C:\Users\kadiy\Downloads\retail_inventory\dataset\product_catalog.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------';

        --====================================================================================================
        SET @start_time = GETDATE();
        PRINT'>>Truncating Table: bronze.erp_purchase_ord_header';
        TRUNCATE TABLE bronze.erp_purchase_ord_header;

        PRINT'>>Inserting into Table: bronze.erp_purchase_ord_header';
        BULK INSERT bronze.erp_purchase_ord_header
        FROM 'C:\Users\kadiy\Downloads\retail_inventory\dataset\purchase_order_header.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------';

        --====================================================================================================
        SET @start_time = GETDATE();
        PRINT'>>Truncating Table: bronze.erp_purchase_ord_line';
        TRUNCATE TABLE bronze.erp_purchase_ord_line;

        PRINT'>>Inserting into Table: bronze.erp_purchase_ord_line';
        BULK INSERT bronze.erp_purchase_ord_line
        FROM 'C:\Users\kadiy\Downloads\retail_inventory\dataset\purchase_order_lines.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------';

        --====================================================================================================
        SET @start_time = GETDATE();
        PRINT'>>Truncating Table: bronze.erp_sales_transactions';
        TRUNCATE TABLE bronze.erp_sales_transactions;

        PRINT'>>Inserting into Table: bronze.erp_sales_transactions';
        BULK INSERT bronze.erp_sales_transactions
        FROM 'C:\Users\kadiy\Downloads\retail_inventory\dataset\sales_transactions_2023_2025.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------';

        --====================================================================================================
        SET @start_time = GETDATE();
        PRINT'>>Truncating Table: bronze.erp_store_master';
        TRUNCATE TABLE bronze.erp_store_master;

        PRINT'>>Inserting into Table: bronze.erp_store_master';
        BULK INSERT bronze.erp_store_master
        FROM 'C:\Users\kadiy\Downloads\retail_inventory\dataset\store_master.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------';

        --====================================================================================================
        SET @start_time = GETDATE();
        PRINT'>>Truncating Table: bronze.erp_supplier_directory';
        TRUNCATE TABLE bronze.erp_supplier_directory;

        PRINT'>>Inserting into Table: bronze.erp_supplier_directory';
        BULK INSERT bronze.erp_supplier_directory
        FROM 'C:\Users\kadiy\Downloads\retail_inventory\dataset\supplier_directory.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-------------------------';

    END TRY 

    BEGIN CATCH
    PRINT '-------------------------';
    PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    PRINT 'Message: ' + ERROR_MESSAGE();
    PRINT 'Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
    PRINT 'State  : ' + CAST(ERROR_STATE() AS NVARCHAR(10));
    PRINT 'Line   : ' + CAST(ERROR_LINE() AS NVARCHAR(10));
    PRINT 'Proc   : ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    PRINT '-------------------------';
    END CATCH
END

EXEC bronze.load_bronze;
