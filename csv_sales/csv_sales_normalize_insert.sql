-- Creating the stored procedure for the silver
CREATE OR ALTER PROCEDURE silver.load_silver_layer
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @start_time DATETIME = GETDATE();

    BEGIN TRY
        PRINT '>> Starting Silver Layer Full Reload';
        BEGIN TRANSACTION;

        -- Product Dimension --
        PRINT '>> Loading dim_products';

        INSERT INTO silver.dim_products (product_id, product_name, brand)
        SELECT DISTINCT
            TRY_CAST(REPLACE(TRIM(ProductID), '"', '') AS INT),
            REPLACE(TRIM(ProductName), '"', ''),
            REPLACE(TRIM(Brand), '"', '')
        FROM bronze.electronics_sales_raw
        WHERE TRY_CAST(REPLACE(TRIM(ProductID), '"', '') AS INT) IS NOT NULL;

        -- Customer Dimension --
        PRINT '>> Loading dim_customers';

        INSERT INTO silver.dim_customers
        (customer_id, customer_name, gender, birthday)
        SELECT DISTINCT
            TRY_CAST(REPLACE(TRIM(CustomerID), '"', '') AS INT),
            REPLACE(TRIM(CustomerName), '"', ''),
            REPLACE(TRIM(Gender), '"', ''),
            TRY_CAST(REPLACE(TRIM(DateOfBirth), '"', '') AS DATE)
        FROM bronze.electronics_sales_raw
        WHERE TRY_CAST(REPLACE(TRIM(CustomerID), '"', '') AS INT) IS NOT NULL;

        -- Customer History --
        PRINT '>> Loading dim_customers_history';

        INSERT INTO silver.dim_customers_history
        (customer_id, email, phone, country_name, region, valid_from, is_current)
        SELECT DISTINCT
            TRY_CAST(REPLACE(TRIM(CustomerID), '"', '') AS INT),
            LOWER(REPLACE(TRIM(Email), '"', '')),
            REPLACE(TRIM(Phone), '"', ''),
            REPLACE(TRIM(cc.country), '"', ''),
            REPLACE(TRIM(cc.region), '"', ''),
            GETDATE(),
            1
        FROM bronze.electronics_sales_raw src
        LEFT JOIN dbo.CountryCodes cc
            ON REPLACE(TRIM(src.CountryCode), '"', '') = cc.cca2
        WHERE TRY_CAST(REPLACE(TRIM(CustomerID), '"', '') AS INT) IS NOT NULL;

        -- Sales Table (Fact) --
        PRINT '>> Loading fact_sales';

        INSERT INTO silver.fact_sales
        (order_line_id, order_id, customer_key, product_key, quantity, unit_price, line_amount, order_date, delivery_date)
        SELECT
            TRY_CAST(REPLACE(TRIM(OrderLineID), '"', '') AS INT),
            TRY_CAST(REPLACE(TRIM(OrderID), '"', '') AS INT),
            dc.customer_key,
            dp.product_key,
            TRY_CAST(REPLACE(TRIM(Quantity), '"', '') AS INT),
            TRY_CAST(REPLACE(TRIM(UnitPrice), '"', '') AS DECIMAL(18,2)),
            TRY_CAST(REPLACE(TRIM(LineAmount), '"', '') AS DECIMAL(18,2)),
            TRY_CAST(REPLACE(TRIM(OrderDate), '"', '') AS DATE),
            TRY_CAST(REPLACE(TRIM(DeliveryDate), '"', '') AS DATE)
        FROM bronze.electronics_sales_raw src
        INNER JOIN silver.dim_customers dc
            ON TRY_CAST(REPLACE(TRIM(src.CustomerID), '"', '') AS INT) = dc.customer_id
        INNER JOIN silver.dim_products dp
            ON TRY_CAST(REPLACE(TRIM(src.ProductID), '"', '') AS INT) = dp.product_id;

        COMMIT TRANSACTION;

        PRINT '>> Full Silver Layer Reload Completed in '
            + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS NVARCHAR)
            + ' seconds';

    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO

-- Execute
EXEC silver.load_silver_layer;

-- Verify
SELECT DISTINCT brand FROM silver.dim_products;
SELECT TOP 10 * FROM silver.dim_customers;
SELECT TOP 10 * FROM silver.dim_customers_history;
SELECT TOP 10 * FROM silver.fact_sales;
