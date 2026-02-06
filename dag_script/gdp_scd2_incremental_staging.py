#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow.decorators import dag, task
from datetime import datetime
from io import StringIO
import pandas as pd
import requests
import numpy as np
import pyodbc
import re
import logging

# --- DATABASE CONFIGURATION ---
DB_CONFIG = {
    "DRIVER": "{ODBC Driver 18 for SQL Server}",
    "SERVER": "*******************", 
    "DATABASE": "DWETL",
    "UID": "airflow_user",
    "PWD": "***********" 
}

@dag(
    dag_id='gdp_scd2_incremental_staging',
    schedule='@monthly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['GDP', 'SCD2', 'Login_Fix']
)
def gdp_incremental_pipeline():

    @task()
    def extract_and_transform_to_long():
        url = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)'
        headers = {"User-Agent": "MyDWProject/1.0 (DWskills)"}
        response = requests.get(url, headers=headers)
        tables = pd.read_html(StringIO(response.text))
        df = tables[2] 

        rename_map = {}
        for col in df.columns:
            if 'Country' in col: rename_map[col] = 'Country'
            else:
                year_match = re.search(r'(\d{4})', col)
                if year_match:
                    year = year_match.group(1)
                    if 'IMF' in col: rename_map[col] = f'GDP_IMF_{year}'
                    elif 'World Bank' in col: rename_map[col] = f'GDP_WB_{year}'
                    elif 'United Nations' in col: rename_map[col] = f'GDP_UN_{year}'
        
        df = df.rename(columns=rename_map)
        df['Country'] = df['Country'].astype(str).str.replace(r'\[.*?\]', '', regex=True).str.strip()
        gdp_cols = [col for col in df.columns if col.startswith('GDP')]
        
        for col in gdp_cols:
            df[col] = (df[col].astype(str).str.replace(r'\[.*?\]', '', regex=True)
                       .str.replace(r'\(.*?\)', '', regex=True).str.replace(',', '').str.strip())
            df[col] = df[col].replace([r'^—.*', r'^N/A$', r'^-$', r'^\s*$'], np.nan, regex=True)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df_long = df.melt(id_vars=['Country'], value_vars=gdp_cols, var_name='Source_Year', value_name='GDP_Value')
        df_long['Source'] = df_long['Source_Year'].str.split('_').str[1]
        df_long['Data_Year'] = df_long['Source_Year'].str.split('_').str[2].astype(int)
        
        return df_long[['Country', 'Source', 'Data_Year', 'GDP_Value']].dropna(subset=['GDP_Value']).replace({np.nan: None}).values.tolist()

    @task()
    def load_incremental_scd2(data_list):
        logging.info(f"Connecting to SQL Server at {DB_CONFIG['SERVER']}...")
        
        # Clean connection string
        conn_str = (
            f"DRIVER={DB_CONFIG['DRIVER']};"
            f"SERVER={DB_CONFIG['SERVER']};"
            f"DATABASE={DB_CONFIG['DATABASE']};"
            f"UID={DB_CONFIG['UID']};"
            f"PWD={DB_CONFIG['PWD']};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )
        
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Truncate Staging
            cursor.execute("TRUNCATE TABLE dbo.StagingCountryGDP")
            
            # Bulk Insert to Staging
            cursor.fast_executemany = True
            cursor.executemany("INSERT INTO dbo.StagingCountryGDP (Country, Source, Data_Year, GDP_Value) VALUES (?, ?, ?, ?)", data_list)
            
            #  SCD Type 2 Update
            cursor.execute("""
                UPDATE tgt SET ValidTo = GETDATE(), IsCurrent = 0
                FROM dbo.CountryGDP tgt JOIN dbo.StagingCountryGDP src 
                ON tgt.Country = src.Country AND tgt.Source = src.Source AND tgt.Data_Year = src.Data_Year
                WHERE tgt.IsCurrent = 1 AND tgt.GDP_Value <> src.GDP_Value;
            """)
            
            # SCD Type 2 Insert
            cursor.execute("""
                INSERT INTO dbo.CountryGDP (Country, Source, Data_Year, GDP_Value, ValidFrom, IsCurrent)
                SELECT s.Country, s.Source, s.Data_Year, s.GDP_Value, GETDATE(), 1
                FROM dbo.StagingCountryGDP s LEFT JOIN dbo.CountryGDP t 
                ON s.Country = t.Country AND s.Source = t.Source AND s.Data_Year = t.Data_Year AND t.IsCurrent = 1
                WHERE t.Country IS NULL;
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            logging.info("SUCCESS! Data has been synchronized.")
        except Exception as e:
            logging.error(f"Database Error: {str(e)}")
            raise

    gdp_data = extract_and_transform_to_long()
    load_incremental_scd2(gdp_data)

gdp_dag = gdp_incremental_pipeline()


# In[ ]:




