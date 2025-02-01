import base64
from google.generativeai import GenerativeModel
import json
import daft
from daft.datatype import DataType

def extract_data_from_pdf(pdf_file, prompt):
    """
    Extracts structured data from a PDF file using Gemini Flash.
    :param pdf_file: Uploaded PDF file object
    :param prompt: Dynamic prompt for Gemini Flash
    :return: Extracted JSON data as a dictionary
    """
    # Initialize the Gemini Flash model
    model = GenerativeModel("gemini-2.0-flash-exp")
    
    # Read and encode the PDF file
    doc_data = base64.standard_b64encode(pdf_file.read()).decode("utf-8")
    
    # Generate content using Gemini Flash
    response = model.generate_content(
        [{'mime_type': 'application/pdf', 'data': doc_data}, prompt]
    )
    
    # Parse the response text as JSON
    try:
        extracted_data = json.loads(response.text)  # Safe JSON parsing
        return extracted_data
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON response: {e}")


def run_etl_pipeline(pdf_files, required_columns):
    """
    Runs the ETL pipeline using Daft.
    :param pdf_files: List of uploaded PDF files
    :param required_columns: List of required columns
    :return: List of transformed data rows
    """
    # Construct the prompt dynamically
    prompt = (
        f"Extract the following fields from the invoice document in JSON format: "
        f"{', '.join(required_columns)}. Ensure the output is structured as a JSON object."
    )
    
    print(prompt)

    # Create Daft DataFrame for parallel processing
    pdf_data = [{"pdf_file": pdf, "required_columns": required_columns} for pdf in pdf_files]
    print(pdf_data)

    df = daft.from_pylist(pdf_data)

    print(df)


    
    # Process PDFs in parallel with specified return type
    df = df.with_column("extracted_data", 
        df["pdf_file"].apply(
            lambda x: extract_data_from_pdf(x, prompt),
            return_dtype=DataType.python()  # Specify return type
        ))
    
    return df.to_pandas()["extracted_data"].tolist()