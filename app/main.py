import streamlit as st
import os
import json
import logging
from helpers.daft_helper import PDFProcessor
from helpers.db_helper import DatabaseHelper
from helpers.etl_pipeline import transform_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def display_db_contents():
    df = db_helper.get_db_contents()
    logger.info(f"Final DB contents: {df}")
    if df.empty:
        st.info("No invoices processed yet.")
        return
    
    # Display the table without the JSON Extract column
    # df_display = df.drop(columns=['JSON Extract'])
    df_display = df
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True
    )
    
    # Add download button
    csv = df_display.to_csv(index=False).encode('utf-8')
    st.download_button(
        "Download as CSV",
        csv,
        "invoice_data.csv",
        "text/csv",
        key='download-csv'
    )

# Initialize helpers
pdf_processor = PDFProcessor()
db_helper = DatabaseHelper()

# Title of the app
st.title("Invoice Processing App")

# API Key Input
api_key = st.sidebar.text_input("Enter Gemini Flash 2.0 API Key", type="password")
if api_key:
    st.sidebar.success("API Key Saved!")
    os.environ["GEMINI_API_KEY"] = api_key

# Define Required Columns
st.subheader("Define Required Columns")
required_columns = st.text_area(
    "Enter required columns (comma-separated)",
    value="Invoice ID, Vendor Information, Total Amount, Tax Details, Line Items"
).split(",")
required_columns = [col.strip() for col in required_columns if col.strip()]

# PDF Upload
uploaded_files = st.file_uploader("Upload Invoice PDFs", type="pdf", accept_multiple_files=True)
if uploaded_files:
    logger.info(f"Files uploaded: {[f.name for f in uploaded_files]}")
    st.info(f"Successfully uploaded {len(uploaded_files)} files")

# Run Process Button
if st.button("Process Invoices"):
    if not uploaded_files:
        st.error("Please upload PDF files first.")
    elif not required_columns:
        st.error("Please define at least one required column.")
    else:
        try:
            # Check DB status
            db_status = db_helper.check_db_status()
            logger.info(f"Database status: {db_status}")
            st.info(f"Database status: {db_status}")

            for pdf_file in uploaded_files:
                with st.spinner(f'Processing {pdf_file.name}...'):
                    logger.info(f"Starting processing for {pdf_file.name}")
                    
                    # Extract data
                    extracted_data = pdf_processor.extract_data_from_pdf(
                        pdf_file, required_columns
                    )
                    logger.info(f"Extracted data: {json.dumps(extracted_data, indent=2)}")
                    st.json(extracted_data)
                    
                    # Transform data
                    transformed_data = transform_data(extracted_data, required_columns)
                    logger.info(f"Transformed data: {transformed_data}")
                    
                    # Load to database
                    success = db_helper.load_data(transformed_data)
                    if success:
                        logger.info(f"Successfully loaded data for {pdf_file.name}")
                        st.success(f"Successfully processed {pdf_file.name}")
                    
            # Show final DB state
            # Add this after your file upload section
            st.subheader("Processed Invoices")

            # Add refresh button
            if st.button("Refresh Data"):
                display_db_contents()
            else:
                display_db_contents()
                    
        except Exception as e:
            logger.error(f"Processing failed: {str(e)}", exc_info=True)
            st.error(f"Processing failed: {str(e)}")


