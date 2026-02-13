## Project Title
### ETL-Data-Warehousing-Project-Global-GDP-and-Sales-Analysis

## Project Overview/Description
The goal of this project is to integrate data from multiple sources (wikipedia, json and csv) into a database, cleaned and stored in an analysis-ready form.

## Hypothetical Business Task
A global electronic store is interested in understanding how their sales are performing globally with regard to the economic strength of each country. This would help management make better decisions in terms of market expansion, resource allocation, and customer satisfaction. 

## Data Sources:
- The GDP data comes from the Wikipedia page.
- Further information about countries is stored in a JSON file.
- Sales and order information are stored in a CSV file (this is a mock-up file generated using the Faker library in Python).

## Tools & Technologies:
The tools used for this project are:
- Python (Pandas, requests, BeautifulSoup, NumPy, pyodbc, SQLAlchemy, Faker)
- SQL(MSSQL)
- Docker
- Apache Airflow

## Project Summary: Multi-Source ETL & Hybrid Relational Modelling
### Automated Ingestion & Historisation (The Wikipedia Pipeline)
For the GDP data, I used Python to scrape Wikipedia, carrying out initial cleaning before transferring the results to SQL Server. To make this production-ready, I Dockerised this specific workflow and utilised Apache Airflow for orchestration. I implemented SCD Type 2 (Slowly Changing Dimension) logic here, ensuring the database maintains a full historical trail of GDP changes over time rather than just a current snapshot.

### The Hybrid Star Schema & The JSON Bridge
I transformed a 15,000-row Sales CSV from a messy flat file into a structured Hybrid Star Schema. I deliberately separated the customer data into two layers to handle different velocities of change:

- Static Dimension: Stores immutable data like Date of Birth and Gender.

- Historical Dimension (SCD2): Tracks dynamic attributes such as Email, Region, and Country.

A significant engineering challenge was that the Sales CSV only contained ISO country codes (e.g., 'NGA'), while the Wikipedia GDP data used full country names. To bridge this gap, I used a JSON dataset as a transient lookup layer during the ETL process. By joining this JSON data while building the Customer History table, I persisted the full country names directly into the records. This then glued the sales ecosystem to the GDP data, using the history table as the functional bridge.
![image description](https://github.com/user/repo/assets/12345/abc-123)

### The Gold Layer: Engineering Business Value
The "Gold" layer is where I applied custom business logic to turn raw data into meaningful insights. I developed a series of SQL Views that hide the underlying engineering complexity (like the SCD2 flags) and calculate key metrics on the fly:

- Logistics Analysis: I engineered a calculation to determine the Days to Deliver for every order, allowing for a clear view of shipping efficiency.

- Strategic Brand Tiering: I implemented logic to categorise products into Brand Tiers, such as 'Premium' (e.g., Apple, Samsung) or 'Standard', enabling more sophisticated sales reporting.

- Demographic Segmentation: I used the static customer data to generate Age Groups, making the dataset instantly ready for targeted marketing analysis.

### Integrated Data Strategy & Master View
The project manages three distinct data behaviours:

- Dynamic Data: The Wikipedia scrape, which requires automated scheduling and history tracking (Airflow/SCD2).

- Static Reference Data: The JSON country data, which served as a lookup during transformation and isn't stored in the final SQL model.

- Transactional Data: The Sales CSV, processed via Bulk Insert for efficiency.

To wrap everything up, I developed a Master Sales View. This consolidates the Fact table with both branches of the Customer dimension into one source of truth. Because the full country names were baked into the Customer History during the ETL phase, the model is incredibly flexible; an analyst can cross-reference sales performance against national wealth by performing a join between this Master View and the CountryGDP table.
