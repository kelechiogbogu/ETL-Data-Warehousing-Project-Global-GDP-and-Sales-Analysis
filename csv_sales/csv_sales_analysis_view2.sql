/*
DATA WAREHOUSE GOLD LAYER (ANALYTICS)
Description: 
    This script creates the 'Gold' layer of the Medallion Architecture.
    It transforms technical 'Silver' tables into business-ready Semantic Views.
Logic:
    - Products: Categorized by Brand (Premium vs. Standard).
    - Customers: Demographics segmented by Age Group.
    - Sales: Logistics tracking (Order to Delivery lead times).
    - History: Filtered for 'Current' state, removing technical metadata.
*/

USE [DWETL];
GO

-- 1. Ensure the Gold Schema exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Gold')
BEGIN
    EXEC('CREATE SCHEMA Gold');
END
GO

-- Gold Product View (Brand-Based Tiering)
-- Logic: Apple and Samsung are flagged as Premium; all other brands are Standard.
PRINT '>> Creating Gold.vw_Products';
GO
CREATE OR ALTER VIEW Gold.vw_Products AS
SELECT 
    product_key,
    product_id,
    product_name,
    brand,
    CASE 
        WHEN brand IN ('Apple', 'Samsung') THEN 'Premium' 
        ELSE 'Standard' 
    END AS brand_tier
FROM silver.dim_products;
GO

-- Gold Customer View (Demographic Segmentation)
-- Logic: Calculates age based on Current Date and segments into life stages.
PRINT '>> Creating Gold.vw_Customers';
GO
CREATE OR ALTER VIEW Gold.vw_Customers AS
SELECT 
    customer_key,
    customer_id,
    customer_name,
    gender,
    birthday,
    DATEDIFF(YEAR, birthday, GETDATE()) AS age,
    CASE 
        WHEN DATEDIFF(YEAR, birthday, GETDATE()) < 18  THEN 'Child'
        WHEN DATEDIFF(YEAR, birthday, GETDATE()) BETWEEN 18 AND 24 THEN 'Young Adult'
        WHEN DATEDIFF(YEAR, birthday, GETDATE()) BETWEEN 25 AND 34 THEN 'Adult'
        WHEN DATEDIFF(YEAR, birthday, GETDATE()) BETWEEN 35 AND 49 THEN 'Mid Age'
        WHEN DATEDIFF(YEAR, birthday, GETDATE()) BETWEEN 50 AND 64 THEN 'Senior'
        ELSE 'Elder'
    END AS age_group
FROM silver.dim_customers;
GO

-- Gold Customer Contact View (SCD Type 2 Filtration)
-- Logic: Removes historical technical columns to present only active contact records.
PRINT '>> Creating Gold.vw_Customer_Contact_Current';
GO
CREATE OR ALTER VIEW Gold.vw_Customer_Contact_Current AS
SELECT 
    customer_id,
    email,
    phone,
    country_name,
    region
FROM silver.dim_customers_history
WHERE is_current = 1;
GO

-- Gold Sales View (Logistics & Performance)
-- Logic: Calculates delivery lead time and categorizes fulfillment speed.
PRINT '>> Creating Gold.vw_Sales_Performance';
GO
CREATE OR ALTER VIEW Gold.vw_Sales_Performance AS
SELECT 
    order_id,
    customer_key,
    product_key,
    quantity,
    unit_price,
    line_amount,
    order_date,
    delivery_date,
    DATEDIFF(DAY, order_date, delivery_date) AS days_to_deliver,
    CASE 
        WHEN DATEDIFF(DAY, order_date, delivery_date) <= 3 THEN 'Fast'
        WHEN DATEDIFF(DAY, order_date, delivery_date) BETWEEN 4 AND 7 THEN 'Standard'
        ELSE 'Delayed'
    END AS delivery_status
FROM silver.fact_sales;
GO

PRINT '>> Gold Layer Semantic Views successfully deployed.';

