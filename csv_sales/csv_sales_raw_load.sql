/*
MASTER SCRIPT: Bronze Layer Setup & Load
Target: bronze.electronics_sales_raw
Source: C:\DW_Project\electronics_sales_raw_countrycode.csv
*/

-- Creating the bronze schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END
GO

-- Recreating table to ensure it matches the CSV structure exactly
IF OBJECT_ID('bronze.electronics_sales_raw', 'U') IS NOT NULL
    DROP TABLE bronze.electronics_sales_raw;
GO

CREATE TABLE bronze.electronics_sales_raw (
    OrderLineID  NVARCHAR(255),
    OrderID      NVARCHAR(255),
    CustomerID   NVARCHAR(255),
    ProductID    NVARCHAR(255),
    ProductName  NVARCHAR(255),
    Brand        NVARCHAR(255),
    Quantity     NVARCHAR(255),
    UnitPrice    NVARCHAR(255),
    LineAmount   NVARCHAR(255),
    OrderDate    NVARCHAR(255),
    DeliveryDate NVARCHAR(255),
    CustomerName NVARCHAR(255),
    Gender       NVARCHAR(255),
    DateOfBirth  NVARCHAR(255),
    Age          NVARCHAR(255),
    CountryCode  NVARCHAR(255),
    Email        NVARCHAR(255),
    Phone        NVARCHAR(255)
);
GO

-- 3. Creating the Stored Procedure
CREATE OR ALTER PROCEDURE bronze.load_sales_data AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME = GETDATE();

    BEGIN TRY
        PRINT '>> Truncating table: bronze.electronics_sales_raw';
        TRUNCATE TABLE bronze.electronics_sales_raw;

        PRINT '>> Executing Bulk Insert...';
        BULK INSERT bronze.electronics_sales_raw
        FROM 'C:\DW_Project\electronics_sales_raw_countrycode.csv'
        WITH (
            FIRSTROW = 2,        
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK 
        );

        PRINT '>> Success! Load finished in ' + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds.';
    END TRY
    BEGIN CATCH
        PRINT '!! ERROR ENCOUNTERED !!';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    END CATCH
END;
GO

-- Executing the procedure
EXEC bronze.load_sales_data;
GO

-- Verification Queries
SELECT COUNT(*) AS [Total Rows in Table] FROM bronze.electronics_sales_raw;
SELECT TOP 10 * FROM bronze.electronics_sales_raw;