import streamlit as st
import requests
import pandas as pd

# FastAPI Backend URL
API_BASE_URL = "https://fastapi-service-476858206005.us-central1.run.app"

# Function to fetch data from FastAPI
def call_api(query, year, qtr, schema):
    api_url = f"{API_BASE_URL}/user_query/{query}/{year}/{qtr}/{schema}"
    
    with st.spinner("Fetching data... "):
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            result = response.json()

            if isinstance(result, dict) and "data" in result:
                df = pd.DataFrame(result["data"])
                return df  # Return DataFrame

            elif isinstance(result, str) and "Run Airflow pipeline" in result:
                return "not_found"  # Return a string if data isn't available
            
            else:
                st.error("Unexpected API response format.")
                return None

        except requests.exceptions.RequestException as e:
            st.error(f" API request failed: {e}")
        except Exception as e:
            st.error(f" An unexpected error occurred: {e}")

    return None

# --------- Streamlit UI ---------

# Set Page Configuration
st.set_page_config(
    page_title="Snowflake Query Dashboard",
    page_icon="📊",
    layout="wide"
)

# App Title
st.title("📊 Snowflake Query Dashboard")

# Sidebar for Instructions
with st.sidebar:
    st.header("ℹ️ Instructions")
    st.write("""
    - **Enter a valid SQL query** to fetch data.
    - **Specify the year, quarter, and schema** to filter data.
    - If data is unavailable, **trigger Airflow DAG** to process it.
    """)
    st.markdown("---")
    st.subheader("🔗 Backend API")
    st.code(API_BASE_URL, language="bash")

# Main Layout
with st.container():
    st.subheader("🔍 Query Snowflake Database")

    with st.form(key="query_form"):
        # Dropdown for Data Type
        option = st.selectbox(
            "Choose Data Type",
            ("Raw Data", "JSON Data", "De-Normalized Data"),
            index=0
        )

        # Year & Quarter Inputs
        col1, col2, col3 = st.columns([1, 1, 1])

        with col1:
            year = st.number_input(
                " Year",
                min_value=1900, max_value=2100, step=1,
                value=2025, help="Enter a 4-digit year"
            )

        with col2:
            qtr = st.number_input(
                " Quarter",
                min_value=1, max_value=4, step=1,
                value=1, help="Enter quarter (1-4)"
            )

        with col3:
            schema = st.selectbox(
                " Schema",
                ("raw", "json", "nm"),  # Updated dropdown menu
                index=0
            )

        # Query Input
        query = st.text_area(
            " SQL Query",
            placeholder="Enter SQL query",
            height=100
        )

        submit = st.form_submit_button(" Fetch Data")

# -------- Handle Form Submission --------
if submit:
    st.info(" Fetching data from Snowflake...")
    result = call_api(query, year, qtr, schema)

    if result is None:
        st.error("❌ An error occurred while fetching data.")

    elif isinstance(result, pd.DataFrame):  # If valid data returned
        if not result.empty:
            st.success("✅ Data Retrieved Successfully!")

            # Apply custom styling for better visibility
            styled_df = result.style.set_properties(**{
                'background-color': '#f8f9fa',  # Light gray background
                'color': 'black',  # Text color
                'border': '1px solid #ddd'  # Light border
            }).set_table_styles([
                {'selector': 'thead th', 'props': [('background-color', '#343a40'), ('color', 'white')]}  # Dark header
            ])

            st.dataframe(styled_df)

        else:
            st.warning(f"⚠️ No data found for **Year: {year}, Quarter: {qtr}, Schema: {schema}**.")

    elif result == "not_found":
        st.warning(f"⚠️ Data for **Year: {year}, Quarter: {qtr}, Schema: {schema}** not found.")
