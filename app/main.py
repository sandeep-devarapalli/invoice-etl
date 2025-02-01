import streamlit as st
import os
import json
import logging
from helpers.daft_helper import PDFProcessor
from helpers.db_helper import DatabaseHelper

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_single_file(pdf_file, pdf_processor, db_helper):
    """Process a single PDF file and return the results"""
    try:
        logger.info(f"Processing file: {pdf_file.name}")
        # Extract data
        extracted_data = pdf_processor.extract_data_from_pdf(pdf_file)
        
        # Check if there was an error in extraction
        if "error" in extracted_data or extracted_data.get("status") == "Failed":
            status = "Failed"
            success = False
        else:
            status = "Completed"
            success = True
            
        json_extract = json.dumps(extracted_data)
        
        # Save to database
        db_success = db_helper.load_data(
            pdf_file.name,
            status,
            json_extract
        )
        
        if not db_success:
            success = False
            status = "Failed"
        
        return {
            'filename': pdf_file.name,
            'status': status,
            'success': success,
            'data': extracted_data
        }
        
    except Exception as e:
        logger.error(f"Error processing {pdf_file.name}: {str(e)}", exc_info=True)
        return {
            'filename': pdf_file.name,
            'status': 'Failed',
            'success': False,
            'data': {'error': str(e)}
        }

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

# Initialize session state for uploaded files if it doesn't exist
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = []


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
    # Update session state with new files
    st.session_state.uploaded_files = uploaded_files
    st.info(f"Files uploaded: {len(uploaded_files)} PDFs")

# Process Button
if st.button("Process Invoices"):
    if not st.session_state.uploaded_files:
        st.error("Please upload PDF files first.")
    elif not api_key:
        st.error("Please enter your API key first.")
    else:
        # Create a progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Process each file
        results = []
        files_to_process = st.session_state.uploaded_files.copy()
        
        for idx, pdf_file in enumerate(files_to_process):
            # Update progress
            progress = (idx + 1) / len(files_to_process)
            progress_bar.progress(progress)
            status_text.text(f"Processing {pdf_file.name}... ({idx + 1}/{len(files_to_process)})")
            
            # Process the file
            result = process_single_file(pdf_file, pdf_processor, db_helper)
            results.append(result)
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        
        # Clear the uploaded files
        st.session_state.uploaded_files = []
        
        # Display results
        st.success(f"Processed {len(files_to_process)} files")
        
        # Create a summary
        success_count = sum(1 for r in results if r['success'])
        failed_count = len(results) - success_count
        
        st.write(f"Successfully processed: {success_count}")
        st.write(f"Failed: {failed_count}")
        
        # Display detailed results in an expander
        with st.expander("View Processing Details"):
            for result in results:
                status_color = "🟢" if result['status'] == "Completed" else "🔴"
                st.markdown(f"{status_color} **{result['filename']}** - {result['status']}")
                if result['status'] == "Failed":
                    st.error(f"Error: {result['data'].get('error', 'Unknown error')}")
                else:
                    st.json(result['data'])

# Display Database Contents
st.subheader("Processed Invoices")
if st.button("Refresh Data"):
    display_db_contents()
else:
    display_db_contents()