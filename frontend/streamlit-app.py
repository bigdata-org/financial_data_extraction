import streamlit as st

# Sample data (replace with actual table and column data from your database)
schemas = {
    'RAW': {
        'table1': ['column1', 'column2', 'column3'],
        'table2': ['column1', 'column2'],
    },
    'JSON': {
        'table3': ['column1', 'column2', 'column3', 'column4'],
        'table4': ['column1'],
    },
    'DW': {
        'table5': ['column1', 'column2', 'column3', 'column4'],
        'table6': ['column1'],
    }
}

# Main UI
st.title('Database Schema Explorer')

# Schema selection
schema_name = st.selectbox('Select Schema', ['RAW', 'JSON', 'DW'])

# Table selection
table_name = st.selectbox('Select Table', list(schemas[schema_name].keys()))
