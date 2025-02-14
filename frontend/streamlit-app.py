import streamlit as st
import requests
import pandas as pd
import time

# FastAPI Backend URL
API_BASE_URL = "https://fastapi-service-476858206005.us-central1.run.app"

# Function to fetch data from FastAPI with single attempt
def call_api_once(query, year, qtr, schema):
    api_url = f"{API_BASE_URL}/user_query/{query}/{year}/{qtr}/{schema}"
    loading_placeholder = st.empty()
    access_placeholder = st.empty()
    
    try:
        # Make single API call
        loading_placeholder.info("Fetching data from Snowflake... Please wait while we process your request.")
        response = requests.get(api_url)
        response.raise_for_status()
        result = response.json()   
        # Show completion message
        if isinstance(result, dict) and "data" in result:
            df = pd.DataFrame(result["data"])
            loading_placeholder.empty()
            return df
        
        if isinstance(result, dict) and "status" in result:
            if result["status"]=="error":
                raise Exception(result["message"])
        
    except Exception as e:
        loading_placeholder.error(f"❌ Error fetching data: {str(e)}")

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
    - After clicking 'Fetch Data', please wait while we process your request.
    - Once fetching is complete, you'll see your data displayed below.
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
                ("raw", "json", "dw"),
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
    # Make single API call and wait
    result = call_api_once(query, year, qtr, schema)
    
    if isinstance(result, pd.DataFrame):
        if not result.empty:
            st.success("📊 Here's your requested data:")

            # Apply custom styling for better visibility
            styled_df = result.style.set_properties(**{
                'background-color': '#f8f9fa',
                'color': 'black',
                'border': '1px solid #ddd'
            }).set_table_styles([
                {'selector': 'thead th', 'props': [('background-color', '#343a40'), ('color', 'white')]}
            ])

            st.dataframe(styled_df)
        else:
            st.warning(f"⚠️ No data found for **Year: {year}, Quarter: {qtr}, Schema: {schema}**.")