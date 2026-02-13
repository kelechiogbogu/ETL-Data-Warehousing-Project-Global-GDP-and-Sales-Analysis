/*
===============================================================================
PORTFOLIO PROJECT: MASTER ANALYTICS VIEW (GOLD LAYER)
Description: 
    This script creates the final 'Master' view by joining the Gold Layer 
    Dimensions and Facts. This is the primary table used for BI Reporting.
===============================================================================
*/

USE [DWETL];
GO

CREATE OR ALTER VIEW Gold.vw_Master_Sales_Report AS
SELECT 
    -- Sales Data
    s.order_id,
    s.order_date,
    s.delivery_date,
    s.days_to_deliver,
    s.delivery_status,
    s.quantity,
    s.unit_price,
    s.line_amount,

    -- Customer Info (from Gold View)
    c.customer_name,
    c.gender,
    c.age_group,

    -- Customer Contact/Location (from Current History View)
    ch.country_name,
    ch.region,
    ch.email,

    -- Product Info (from Gold View)
    p.product_name,
    p.brand,
    p.brand_tier

FROM Gold.vw_Sales_Performance s
LEFT JOIN Gold.vw_Customers c 
    ON s.customer_key = c.customer_key
LEFT JOIN Gold.vw_Customer_Contact_Current ch 
    ON c.customer_id = ch.customer_id
LEFT JOIN Gold.vw_Products p 
    ON s.product_key = p.product_key;
GO

-- TEST QUERY
SELECT * FROM Gold.vw_Master_Sales_Report;