
## Project Overview

Findata Inc. is developing a comprehensive master financial statement database to enable better and faster fundamental analysis of US public companies. This project covers everything from data scraping and schema design to building robust ETL pipelines and data validation workflows.

Streamlit url = 
Api url = https://fastapi-service-476858206005.us-central1.run.app/docs



## Objectives

- Scrape and retrieve dataset links from the SEC Markets Data page.
- Review and analyze the formatting of SEC’s Financial Statement Data.
- Evaluate and implement three data storage approaches:
  - **Raw Staging:** Store data in its original format.
  - **JSON Transformation:** Denormalize data into JSON format for improved access speed.
  - **Denormalized Fact Tables:** Transform data into three key fact tables (Balance Sheet, Income Statement, and Cash Flow) with associated company and period identifiers.
 
## Technologies Used

- **Snowflake:** Data warehousing and storage.
- **Python:** Scripting for data scraping, validation, and pipeline tasks.
- **Airflow:** Orchestrating ETL processes and pipeline workflows.
- **S3:** Intermediate data staging.
- **Streamlit:** Frontend application for data visualization.
- **FastAPI:** Backend API connectivity to Snowflake.
- **SQLAlchemy & Snowflake SQL:** Querying and interacting with Snowflake data.


## Getting Started
1. Clone the repository.
2. Set up your environment by installing required Python packages:
pip install -r requirements.txt
3. Configure your environment variables or configuration files for Snowflake credentials, S3 access, and Airflow settings.
4. Run the Airflow image  : docker compose up -d
5. Run uvicorn server : uvicorn main:app --reload
6. Run streamlit ui : streamlit run streamlit-app.py
