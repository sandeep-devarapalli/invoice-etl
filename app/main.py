
from daft_helpers import run_etl_pipeline
import streamlit as st
from etl_pipeline import transform_data, load_data_to_duckdb

# Title of the app
st.title("Invoice Processing App")

# API Key Input
api_key = st.sidebar.text_input("Enter Gemini Flash 2.0 API Key", type="password")
if api_key:
    st.sidebar.success("API Key Saved!")
    import os
    os.environ["GEMINI_API_KEY"] = api_key

# Define Required Columns
st.subheader("Define Required Columns")
required_columns = st.text_area(
    "Enter required columns (comma-separated)",
    value="Invoice ID, Vendor Information, Total Amount, Tax Details, Line Items"
).split(",")

# Clean up column names
required_columns = [col.strip() for col in required_columns if col.strip()]

# PDF Upload
uploaded_files = st.file_uploader("Upload Invoice PDFs", type="pdf", accept_multiple_files=True)

print(uploaded_files);

# Run ETL Process Button
if st.button("Run ETL Process"):
    if not uploaded_files:
        st.error("Please upload PDF files first.")
    elif not required_columns:
        st.error("Please define at least one required column.")
    else:
        try:
            # Process all PDFs in parallel using Daft
            extracted_data_list = run_etl_pipeline(uploaded_files, required_columns)
            
            # Transform and load each result
            for idx, extracted_data in enumerate(extracted_data_list):
                transformed_data = transform_data(extracted_data, required_columns)
                load_data_to_duckdb(transformed_data)
                st.success(f"Successfully processed file {idx + 1}")
                
        except Exception as e:
            st.error(f"Processing failed: {str(e)}")