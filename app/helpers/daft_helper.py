import base64
from utils import clean_and_parse_json  # Ensure this utility function is implemented
import google.generativeai as genai
import json
import logging
import daft
from datetime import datetime
from typing import List
import pyarrow as pa
import pandas as pd

logger = logging.getLogger(__name__)

class PDFProcessor:
    def __init__(self, api_key=None):
        logger.info("Initializing PDFProcessor")
        self.model = None
        self.api_key = api_key
        self.pdf_pipeline = None
        
    def initialize_model(self, api_key):
        """Initialize or reinitialize the model with a new API key"""
        try:
            self.api_key = api_key
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
            logger.info("Model initialized with API key")
        except Exception as e:
            logger.error(f"Failed to initialize model: {str(e)}")
            raise e
    
    def setup_pipeline(self, pdf_files: List):
        """Setup Daft pipeline for PDF processing"""
        pdf_data = [{
            'filename': pdf.name,
            'file_content_base64': base64.standard_b64encode(pdf.read()).decode("utf-8"),  # Encode file content as Base64
            'ingestion_timestamp': datetime.now(),
            'processing_status': 'pending',
            'completed_timestamp': None,
            'metadata': None
        } for pdf in pdf_files]
        self.pdf_pipeline = daft.from_pydict({
            'filename': [d['filename'] for d in pdf_data],
            'file_content_base64': [d['file_content_base64'] for d in pdf_data],
            'ingestion_timestamp': [d['ingestion_timestamp'] for d in pdf_data],
            'processing_status': [d['processing_status'] for d in pdf_data],
            'completed_timestamp': [d['completed_timestamp'] for d in pdf_data],
            'metadata': [d['metadata'] for d in pdf_data]
        })
        logger.info("Pipeline setup complete.")
        logger.info(self.pdf_pipeline)
    
    def process_pdfs(self):
        """Process PDFs using Daft's parallel processing"""
        logger.info("Entered process_pdfs")
        logger.info(self.pdf_pipeline)

        if not self.model:
            raise ValueError("Model not initialized. Please provide API key first.")
        
        @daft.udf(return_dtype=daft.DataType.python())  # Specify JSON type for metadata
        def process_pdf(file_content_base64):
            try:
                logger.info("Entered process_pdf function")
                # Send Base64-encoded PDF content to Gemini for processing
                prompt = (
                    "Extract all relevant information from this invoice document and return it in JSON format. "
                    "Include any important fields such as invoice number, date, vendor details, line items, "
                    "amounts, taxes, and any other relevant information found in the document."
                )
                
                response = self.model.generate_content(
                    [{'mime_type': 'application/pdf', 'data': file_content_base64}, prompt]
                )
                
                metadata = clean_and_parse_json(response.text)  # Parse Gemini's response
                logger.info(metadata)
                return metadata
            except Exception as e:
                logger.error(f"Error processing PDF: {str(e)}", exc_info=True)
                return {'error': str(e)}
        
        # Process using Daft's expression API
        try:
            logger.info("Next apply lambda function on below df")
            logger.info(self.pdf_pipeline)

            # Define the full pipeline first
            result = (
                self.pdf_pipeline
                .with_column(
                    "metadata", 
                    daft.col('file_content_base64').apply(process_pdf, return_dtype=daft.DataType.python())
                )
                .with_column(
                    "processing_status",
                    daft.col("metadata").is_null().if_else("failed", "completed")
                )
                .with_column(
                    "completed_timestamp",
                    daft.lit(datetime.now())
                )
            )
            
            self.pdf_pipeline = result.collect()

            logger.info("Lambda function on below df result")
            logger.info(self.pdf_pipeline)

        except Exception as e:
            logger.error(f"Failed to apply UDF: {str(e)}", exc_info=True)
            raise e
    
    def get_processed_data(self):
        """Get processed data as PyArrow table for DuckDB insertion"""
        logger.info("Entered Get Processed Data")
        logger.info(self.pdf_pipeline)
        
        # Select only required columns and rename them
        processed_data = (
            self.pdf_pipeline
            .select(
                daft.col('filename').alias('invoice_file_name'),
                'ingestion_timestamp',
                'completed_timestamp',
                daft.col('processing_status').alias('status'),
                daft.col('metadata').map(json.dumps).alias('json_extract')
            )
            .to_arrow()
        )
        
        return processed_data