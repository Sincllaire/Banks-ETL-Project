# Banks Market Capitalization ETL Project

**About**

This project is an end-to-end ETL (Extract, Transform, Load) pipeline designed to scrape, process, and store market capitalization data of the world's largest banks. It automates data extraction from Wikipedia, transforms values into multiple currencies, and loads the results into both a CSV file and an SQLite database for further analysis. This project demonstrates key data engineering concepts, including web scraping, data transformation, and database management.

**Project Overview**

This project is an ETL (Extract, Transform, Load) pipeline that extracts data on the largest banks by market capitalization from a Wikipedia page, transforms the data into multiple currencies using exchange rates, and loads the results into both a CSV file and an SQLite database. The final dataset can then be queried to analyze market trends.

**Technologies Used**

Python for scripting and automation

BeautifulSoup for web scraping

Pandas and NumPy for data manipulation and transformation

Requests for fetching web page content

SQLite3 for database management

Datetime for timestamp logging

**Project Structure**

├── banks_project.py  # Main script executing the ETL pipeline
├── code_log.txt      # Log file tracking the execution process
├── Largest_banks_data.csv  # Output CSV containing transformed data
├── Banks.db          # SQLite database storing the processed data

**ETL Process**

1. Extraction

The script scrapes a Wikipedia page (archived version) containing a list of the largest banks by market capitalization. It extracts the bank names and their market capitalization in USD.

2. Transformation

Converts the market capitalization from USD to GBP, EUR, and INR using exchange rates from an external CSV file.

Handles data cleaning and formatting.

3. Loading

Saves the transformed data into a CSV file.

Loads the data into an SQLite database table.

**Key Functions**

extract(url, table_attribs): Scrapes the bank names and market capitalization from the webpage.

transform(df, csv_path): Converts market capitalization values into different currencies.

load_to_csv(df, output_path): Saves the transformed data to a CSV file.

load_to_db(df, sql_connection, table_name): Inserts data into an SQLite database.

run_queries(query_statement, sql_connection): Executes SQL queries for analysis.

log_progress(message): Logs process execution steps for tracking.

**Sample Queries**

The script runs SQL queries to:

1. Print the entire table of transformed data.

2. Calculate the average market capitalization in GBP.

3. List the names of the top 5 banks.

**How to Run**

*Prerequisites*

Ensure you have the required Python libraries installed:

pip install numpy pandas requests beautifulsoup4 sqlite3

**Execution Steps**

Download the exchange_rate.csv using:

wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

Run the script:

python banks_project.py

The script will:

- Log execution steps in code_log.txt.

- Save transformed data in Largest_banks_data.csv.

- Store data in Banks.db.

- Execute predefined queries and print results.

**Future Enhancements**

Error Handling: Improve robustness by handling missing or malformed data.

Automation: Schedule periodic updates to fetch and refresh data.

Visualization: Implement dashboards for market analysis using Matplotlib or Tableau.

Workflow Orchestration: Implement Apache Airflow to schedule and automate the ETL pipeline, ensuring efficient data processing and better monitoring of data workflows.

Author

Sinclaire Hoyt 
Aspiring Data Engineer | Python | SQL | Data Analytics



