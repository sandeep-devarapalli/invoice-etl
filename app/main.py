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
        st.info("No pdfs processed yet.")
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
        "pdf_extract_data.csv",
        "text/csv",
        key='download-csv'
    )

# Initialize helpers
pdf_processor = PDFProcessor()
db_helper = DatabaseHelper()

# Title of the app
st.title("PDF Document Processing App")

# API Key Input
api_key = st.sidebar.text_input("Enter Gemini Flash 2.0 API Key", type="password")
if api_key:
    st.sidebar.success("API Key Saved!")
    pdf_processor.initialize_model(api_key)

# Add custom prompt input
custom_prompt = st.sidebar.text_area(
    "Customize extraction prompt (optional)",
    """Extract all relevant information from this invoice document and return it in JSON format. 
Include any important fields such as invoice number, date, vendor details, line items, 
amounts, taxes, and any other relevant information found in the document.
Rules: 
1. If the document is not an invoice, return an error message in the response""",
    help="Customize how the AI should extract information from your PDFs"
)

# Multiple PDF Upload
uploaded_files = st.file_uploader("Upload PDFs", type="pdf", accept_multiple_files=True)
if uploaded_files:
    st.info(f"Files uploaded: {len(uploaded_files)} PDFs")

# Process Button
if st.button("Process PDF Documents"):
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
                pdf_processor.process_pdfs(custom_prompt)
                processed_data = pdf_processor.get_processed_data()

            with st.spinner("Saving to database..."):
                db_helper.load_data_batch(processed_data)
                st.success(f"Processed {len(uploaded_files)} documents!")

        except Exception as e:
            st.error(f"Processing failed: {str(e)}")

# Display Database Contents
st.subheader("Processed PDF Documents")
if st.button("Refresh Data"):
    display_db_contents()
else:
    display_db_contents()