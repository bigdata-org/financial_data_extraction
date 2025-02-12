import streamlit as st





with st.form(key='my_form'):
    # Create a select box
    option = st.selectbox(
        "Choose an option",
        ("Raw Data", "JSON Data", "De-Normalize Data"),
        placeholder="Select an option..."
    )

    # Create two columns for the first two text inputs
    col1, col2 = st.columns(2)

    # Put text inputs in the columns
    with col1:
        text1 = st.number_input("Year", min_value=1900, 
                               max_value=2100,
                               step=1,
                               value=2025,  # default value
                               help="Please enter a 4-digit year")
        
    with col2:
        text2 = st.number_input("Quater", placeholder="Enter Quater")

    # Third text input below, using full width
    text3 = st.text_input( "Query", placeholder="Enter Query")

    submit = st.form_submit_button("Get Data")

    
if submit:
    st.write("calling api")