import base64
import logging
from utils import clean_and_parse_json
from datetime import datetime
from typing import List, Optional
import google.generativeai as genai

import daft
daft.context.set_runner_native()

logger = logging.getLogger(__name__)

class PDFProcessor:
    def __init__(self):
        self.model: Optional[genai.GenerativeModel] = None
        self.pdf_pipeline: Optional[daft.DataFrame] = None
        self._model_api_key: Optional[str] = None

    def initialize_model(self, api_key: str):
        """Initialize model separately to avoid serialization issues"""
        try:
            self._model_api_key = api_key
            genai.configure(api_key=self._model_api_key)
            self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
            logger.info("Model initialized")
        except Exception as e:
            logger.error(f"Model init failed: {str(e)}")
            raise

    def setup_pipeline(self, pdf_files: List):
        """Create pipeline without storing PDF content in class state"""
        pdf_data = [{
            'filename': pdf.name,
            'file_content_base64': base64.b64encode(pdf.read()).decode("utf-8"),
            'ingestion_timestamp': datetime.now(),
        } for pdf in pdf_files]
        
        self.pdf_pipeline = daft.from_pydict({
            'filename': [d['filename'] for d in pdf_data],
            'file_content_base64': [d['file_content_base64'] for d in pdf_data],
            'ingestion_timestamp': [d['ingestion_timestamp'] for d in pdf_data],
        })
        logger.info("Pipeline initialized with %d files", len(pdf_files))

    def process_pdfs(self):
        """Process PDFs using Daft's parallel processing"""
        if not self.model:
            raise ValueError("Model not initialized. Please provide API key first.")

        def create_processing_udf(model: genai.GenerativeModel):
            def process_pdf(file_content_base64: str) -> dict:
                try:
                    response = model.generate_content([
                        {'mime_type': 'application/pdf', 'data': file_content_base64},
                        "Extract all relevant information from this invoice document and return it in JSON format. "
                        "Include any important fields such as invoice number, date, vendor details, line items, "
                        "amounts, taxes, and any other relevant information found in the document."
                    ])
                    # Parse and clean the Gemini response
                    response_dict = clean_and_parse_json(response.text)
                    logger.info(f"Processed metadata: {response_dict}")
                    return response_dict
                
                except Exception as e:
                    logger.error(f"Error processing PDF: {str(e)}", exc_info=True)
                    return {"error": str(e)}
            return process_pdf

        try:
            # Create UDF with only needed dependencies
            process_udf = create_processing_udf(self.model)

            # Build pipeline with explicit execution
            self.pdf_pipeline = (
                self.pdf_pipeline
                .with_column(
                    "metadata", 
                    daft.col("file_content_base64").apply(
                        process_udf, 
                        return_dtype=daft.DataType.python()
                    )
                )
                .with_column(
                    "processing_status",
                    daft.col("metadata").is_null().if_else("failed", "completed")
                )
                .with_column(
                    "completed_timestamp",
                    daft.lit(datetime.now())
                )
                
                .collect()
            )

            logger.info(f"Processing completed for {len(self.pdf_pipeline)} files")

        except Exception as e:
            logger.error(f"Processing failed: {str(e)}", exc_info=True)
            raise

    def get_processed_data(self):
        """Get processed data as PyArrow table for DuckDB insertion"""
        processed_df = (
            self.pdf_pipeline
            .with_column(
                "metadata",
                self.pdf_pipeline["metadata"].cast(daft.DataType.string())
            )
            .select(
                daft.col('filename').alias('invoice_file_name'),
                'ingestion_timestamp',
                'completed_timestamp',
                daft.col('processing_status').alias('status'),
                daft.col('metadata').alias('json_extract')
            )
            .to_arrow()
        )
        
        return processed_df