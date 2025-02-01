import logging
from utils import clean_numeric_value
import json

logger = logging.getLogger(__name__)

def transform_data(data, required_columns):
    """
    Transforms extracted JSON data into a consistent schema for DuckDB.
    """
    # Check if processing failed
    if isinstance(data, dict) and data.get("status") == "FAILED":
        return [
            data.get("raw_text", "")[:50] + "...",  # First 50 chars of raw text as ID
            "FAILED",  # status
            "Processing Failed",  # vendor_info
            0.0,  # total_amount
            json.dumps({"error": data.get("error")}),  # tax_details
            "[]",  # line_items
            json.dumps(data)  # full JSON extract with error details
        ]

    # Transform the raw extracted data into the format needed for DB
    try:
        transformed = [
            data.get('Invoice ID', 'N/A'),
            'Processed',  # status
            data.get('Vendor Information', 'N/A'),
            clean_numeric_value(data.get('Total Amount', 0)),
            json.dumps(data.get('Tax Details', [])),
            json.dumps(data.get('Line Items', [])),
            json.dumps(data)  # full JSON extract
        ]
        return transformed
    except Exception as e:
        logger.error(f"Transform failed: {str(e)}")
        return [
            str(hash(str(data)))[:10],  # Generate an ID from the data
            "FAILED",
            "Transform Failed",
            0.0,
            json.dumps({"error": str(e)}),
            "[]",
            json.dumps({"raw_data": str(data), "error": str(e)})
        ]