import base64
import json
from google.generativeai import GenerativeModel
import duckdb

def transform_data(data, required_columns):
    """
    Transforms extracted JSON data into a consistent schema for DuckDB.
    :param data: Extracted JSON data
    :param required_columns: List of required columns
    :return: Dictionary with standardized schema
    """
    transformed_data = {}
    for col in required_columns:
        transformed_data[col] = data.get(col, "N/A")  # Default value if field is missing
    
    # Add Full JSON Extract
    transformed_data["json_extract"] = str(data)
    return transformed_data


def load_data_to_duckdb(data):
    """
    Loads transformed data into DuckDB.
    :param data: Transformed data as a dictionary
    """
    try:
        conn = duckdb.connect("data/invoices.db")
        
        # Ensure the table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                invoice_id STRING,
                status STRING,
                vendor_info STRING,
                total_amount FLOAT,
                tax_details STRING,
                line_items STRING,
                json_extract STRING
            )
        """)
        
        # Insert data into the table
        conn.execute("""
            INSERT INTO invoices (invoice_id, status, vendor_info, total_amount, tax_details, line_items, json_extract)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("Invoice ID", "N/A"),
            data.get("Status", "Pending"),
            data.get("Vendor Information", "N/A"),
            float(data.get("Total Amount", 0)),
            data.get("Tax Details", "N/A"),
            data.get("Line Items", "[]"),
            data.get("json_extract", "{}")
        ))
        
        conn.close()
    
    except Exception as e:
        raise Exception(f"Database connection failed: {e}")
    finally:
        if 'conn' in locals():
            conn.close()