import base64
from utils import clean_and_parse_json
from google.generativeai import GenerativeModel
import json
import logging

logger = logging.getLogger(__name__)

class PDFProcessor:
    def __init__(self):
        logger.info("Initializing PDFProcessor")
        self.model = GenerativeModel("gemini-2.0-flash-exp")

    def extract_data_from_pdf(self, pdf_file, required_columns):
        """Extract data from a single PDF"""
        try:
            logger.info(f"Starting PDF processing: {pdf_file.name}")
            
            # Create prompt
            prompt = (
                f"Extract the following fields from the invoice document in JSON format: "
                f"{', '.join(required_columns)}. Ensure the output is structured as a JSON object."
            )
            logger.info(f"Using prompt: {prompt}")

            # Read and encode PDF
            pdf_content = pdf_file.read()
            logger.info(f"Read PDF file, size: {len(pdf_content)} bytes")
            doc_data = base64.standard_b64encode(pdf_content).decode("utf-8")
            logger.info("PDF encoded successfully")
            
            # Generate content
            logger.info("Calling Gemini API")
            response = self.model.generate_content(
                [{'mime_type': 'application/pdf', 'data': doc_data}, prompt]
            )
            logger.info(f"Received response from Gemini: {response.text}")
            
            # Parse and validate response
            extracted_data = clean_and_parse_json(response.text)
            
            # Log success
            logger.info(f"Successfully parsed JSON: {json.dumps(extracted_data, indent=2)}")
            logger.debug(f"Extracted data: {json.dumps(extracted_data, indent=2)}")

            return extracted_data
            
        except Exception as e:
            logger.error(f"Error processing {pdf_file.name}: {str(e)}", exc_info=True)
            raise e