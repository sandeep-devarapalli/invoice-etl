import base64
from utils import clean_and_parse_json
import google.generativeai as genai
import json
import logging

logger = logging.getLogger(__name__)

class PDFProcessor:
    def __init__(self, api_key=None):
        logger.info("Initializing PDFProcessor")
        self.model = None
        self.api_key = api_key
        
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

    def extract_data_from_pdf(self, pdf_file):
        """Extract data from a single PDF"""
        if not self.model:
            raise ValueError("Model not initialized. Please provide API key first.")
            
        try:
            logger.info(f"Starting PDF processing: {pdf_file.name}")
            
            prompt = (
                "Extract all relevant information from this invoice document and return it in JSON format. "
                "Include any important fields such as invoice number, date, vendor details, line items, "
                "amounts, taxes, and any other relevant information found in the document."
            )
            
            pdf_content = pdf_file.read()
            doc_data = base64.standard_b64encode(pdf_content).decode("utf-8")
            
            response = self.model.generate_content(
                [{'mime_type': 'application/pdf', 'data': doc_data}, prompt]
            )
            
            extracted_data = clean_and_parse_json(response.text)
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error processing {pdf_file.name}: {str(e)}", exc_info=True)
            return {
                "error": str(e),
                "status": "Failed"
            }