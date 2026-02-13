-- Creating the silver schema --
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO


-- Dimensional Modeling (Optimized Data Types) --
-- Creating my Customer Table --
IF OBJECT_ID('silver.dim_customers', 'U') IS NOT NULL DROP TABLE silver.dim_customers;
GO
CREATE TABLE silver.dim_customers (
    customer_key  INT IDENTITY(1,1) PRIMARY KEY, 
    customer_id   INT,                           
    customer_name NVARCHAR(100), 
    gender        NVARCHAR(50),  
    birthday      DATE
);


-- Creating my Customer History Table --
IF OBJECT_ID('silver.dim_customers_history', 'U') IS NOT NULL DROP TABLE silver.dim_customers_history;
GO
CREATE TABLE silver.dim_customers_history (
    customer_history_key INT IDENTITY(1,1) PRIMARY KEY, 
    customer_id          INT,                           
    email                NVARCHAR(100),
    phone                NVARCHAR(50), 
    country_name         NVARCHAR(100), 
    region               NVARCHAR(50), 
    valid_from           DATETIME DEFAULT GETDATE(),
    valid_to             DATETIME NULL,
    is_current           BIT DEFAULT 1 
);


-- Product Table (Dimension) --
IF OBJECT_ID('silver.dim_products', 'U') IS NOT NULL DROP TABLE silver.dim_products;
GO
CREATE TABLE silver.dim_products (
    product_key   INT IDENTITY(1,1) PRIMARY KEY, 
    product_id    INT,                           
    product_name  NVARCHAR(100), -- Reduced from 255
    brand         NVARCHAR(50)   -- Reduced from 100
);


-- Sales Table (Fact) --
IF OBJECT_ID('silver.fact_sales', 'U') IS NOT NULL DROP TABLE silver.fact_sales;
GO
CREATE TABLE silver.fact_sales (
    sales_key      INT IDENTITY(1,1) PRIMARY KEY, 
    order_line_id  INT,
    order_id       INT,
    customer_key   INT, 
    product_key    INT, 
    quantity       INT,
    unit_price     DECIMAL(18,2),
    line_amount    DECIMAL(18,2),
    order_date     DATE,
	delivery_date  DATE,
    
    CONSTRAINT FK_fact_sales_customers FOREIGN KEY (customer_key) REFERENCES silver.dim_customers(customer_key),
    CONSTRAINT FK_fact_sales_products  FOREIGN KEY (product_key)  REFERENCES silver.dim_products(product_key)
);