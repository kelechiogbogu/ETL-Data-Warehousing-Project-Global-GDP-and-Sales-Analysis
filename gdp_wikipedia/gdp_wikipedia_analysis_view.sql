USE [DWETL];
GO

-- Creating the Gold/analytics Schema for the GDP data
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA Gold');
END
GO

-- Creating the Production View in the Gold Layer
CREATE OR ALTER VIEW gold.vw_Global_GDP_Current AS
SELECT 
    Country, 
    Source AS DataSource, 
    Data_Year AS ReportYear, 
    GDP_Value AS NominalGDP
FROM dbo.CountryGDP
WHERE IsCurrent = 1;


-- Testing
SELECT * FROM gold.vw_Global_GDP_Current