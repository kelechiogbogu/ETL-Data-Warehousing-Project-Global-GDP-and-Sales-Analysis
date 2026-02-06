-- Create a staging Table for GDP data(The Transient Layer / ETL Staging) --
CREATE TABLE dbo.StagingCountryGDP (
    Country    NVARCHAR(255),
    Source     NVARCHAR(50),
    Data_Year  INT,
    GDP_Value  FLOAT
);

-- Creating my main table (The History/Main Layer)--
CREATE TABLE dbo.CountryGDP (
    RecordID    INT IDENTITY(1,1) PRIMARY KEY, 
    Country     NVARCHAR(255) NOT NULL,
    Source      NVARCHAR(50)  NOT NULL, 
    GDP_Value   FLOAT,
    Data_Year   INT,                    
    
    -- SCD Type 2 columns
    ValidFrom   DATETIME2 NOT NULL DEFAULT GETDATE(),
    ValidTo     DATETIME2 NULL,         
    IsCurrent   BIT NOT NULL DEFAULT 1
);

-- Index for the SCD2 "Look-back"
CREATE NONCLUSTERED INDEX IX_CountryGDP_SCD2 
ON dbo.CountryGDP (Country, Source, IsCurrent);


-- To test the incremental load, I intentionally updated the IMF's 2024 GDP for the United States
UPDATE dbo.CountryGDP 
SET GDP_Value = 999999 
WHERE Country = 'United States' AND Source = 'IMF' AND IsCurrent = 1;

-- Checking if it works--
SELECT * FROM dbo.CountryGDP 
WHERE Country = 'United States' AND Source = 'IMF';

--WORKS
