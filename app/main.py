import streamlit as st
import os
import json
import logging
from helpers.daft_helper import PDFProcessor
from helpers.db_helper import DatabaseHelper

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def display_db_contents():
    df = db_helper.get_db_contents()
    if df.empty:
        st.info("No invoices processed yet.")
        return
    
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True
    )
    
    csv = df.to_csv(index=False).encode('utf-8')
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
    pdf_processor.initialize_model(api_key)

# Multiple PDF Upload
uploaded_files = st.file_uploader("Upload Invoice PDFs", type="pdf", accept_multiple_files=True)
if uploaded_files:
    st.info(f"Files uploaded: {len(uploaded_files)} PDFs")

# Process Button
if st.button("Process Invoices"):
    if not uploaded_files:
        st.error("Please upload PDF files first.")
    elif not api_key:
        st.error("Please enter your API key first.")
    else:
        try:
            with st.spinner("Initializing processing..."):
                pdf_processor = PDFProcessor()
                pdf_processor.initialize_model(api_key)
                pdf_processor.setup_pipeline(uploaded_files)

            with st.spinner("Processing PDFs..."):
                pdf_processor.process_pdfs()
                processed_data = pdf_processor.get_processed_data()

            with st.spinner("Saving to database..."):
                db_helper.load_data_batch(processed_data)
                st.success(f"Processed {len(uploaded_files)} invoices!")

        except Exception as e:
            st.error(f"Processing failed: {str(e)}")

# Display Database Contents
st.subheader("Processed Invoices")
if st.button("Refresh Data"):
    display_db_contents()
else:
    display_db_contents()