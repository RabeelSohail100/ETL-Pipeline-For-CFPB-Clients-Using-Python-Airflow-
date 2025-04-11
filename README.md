# ETL-Pipeline-For-CFPB-Clients-Using-Python-Airflow-
# ETL Pipeline – API to Google Sheets

This project demonstrates a basic ETL pipeline using Python. It extracts data from a public API, transforms it using pandas, and loads the cleaned data into a Google Sheet. The process is also automated using **Apache Airflow**.

## Steps

**1. Extract**  
- `extract.py`: Fetches raw data from an API.

**2. Transform**  
- `transform.py`: Cleans and formats the data using pandas.

**3. Load**  
- `load.py`: Loads the final data into a Google Sheet using the Google Sheets API.

**4. Orchestration (Airflow)**  
- `dag_etl.py`: Airflow DAG to automate the ETL process. The pipeline is scheduled and managed using Apache Airflow.

## Files Included
- `extract.py`, `transform.py`, `load.py`: Core ETL scripts
- `dag_etl.py`: Airflow DAG to automate and schedule the ETL workflow
- `credentials.json`: Google API credentials (not included here for security)
- `requirements.txt`: Required Python packages
