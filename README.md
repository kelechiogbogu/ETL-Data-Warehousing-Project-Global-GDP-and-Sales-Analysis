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

## ETL Process
For the GDP data, I used Python to scrape Wikipedia, carrying out initial cleaning before transferring it to SQL Server via pyodbc. To make this production-ready, I Dockerised this specific workflow and used Apache Airflow for orchestration. I implemented SCD Type 2 (Slowly Changing Dimension) logic here, ensuring the database maintains a full historical trail of GDP changes over time rather than just a current snapshot.

### Data Normalisation & Enrichment (Sales & JSON)
I transformed a Sales CSV (about 15,000 rows) from a messy flat file into a structured Star Schema to eliminate redundancy. During the normalisation process, I integrated a JSON dataset to pull standardised, full country names into the Customer History table. This ensured the dimensions were enriched with accurate geographic data right from the start.

### Integrated Data Strategy
The project manages three distinct data behaviours:

- Dynamic Data: The Wikipedia scrape, which requires automated scheduling and history tracking (Airflow/SCD2).

- Static Reference Data: The JSON country data, loaded as a stable reference as it rarely changes.

- Sample Transactional Data: The Sales CSV, processed via Bulk Insert to demonstrate efficient relational modelling.

### The Semantic Layer & Master View
I created a Gold Layer using SQL Views to hide the underlying engineering complexity (like the SCD2 flags) and add business logic on the fly, such as customer Age Groups and Brand Tiers (e.g., categorising Apple and Samsung as 'Premium').

To wrap up the project, I developed a Master Sales View that consolidates all sales, product, and customer dimensions into a single source of truth. This view is designed for maximum flexibility; it’s completely ready for analysis as-is, and if an analyst needs to cross-reference sales performance against national wealth, they only need to perform one simple join between this Master View and the GDP table.



