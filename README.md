# Invoice Processing App

This project processes invoice PDFs using Gemini Flash APIs, Daft, and DuckDB. 
It provides a Streamlit interface for uploading PDFs, extracting data, and managing the extracted information.

## Setup Instructions

# Build the image
docker build -t invoice-processor .

# Run the container
docker run -p 8501:8501 invoice-processor

For more details, refer to the documentation.

1. Setup schema (Only once), New Schema
    - Take in multiple sample invoices
    - User will also enter the columns required
    - Table name for DuckDB
    - Sent to a schema builder - gemini api which will give - 
        - Structured Output schema - gemini structured output
        - Duckdb Table schema
    - Create Duckdb Table (Show schema on screen)
    - Save structured output schema, temp file, DuckDB - Structured O/P

2. Invoice Processor
    - Uploads all the invoices
    - Process data from pdf_processor
    - Push data to the duckdb table selected above
    - Display all the data from DuckDB in an streamlit editor screen

## Docker commands to note - 

# List all running containers
docker ps

# Stop all running containers
docker stop $(docker ps -q)

# Remove all stopped containers
docker rm $(docker ps -aq)

# Then try running your container again
<!-- docker run -p 8501:8501 -v $(pwd)/data:/app/data invoice-processor (Volume mount to local folder if required) -->

docker run -p 8501:8501 invoice-processor


## DuckDB Schema
    - id (system_id) - auto increment number
    - timestamp (system_timestamp) - current timestamp
    - file_name - filename of the uploaded file being processed
    - status (Failed, Completed) - if record is being processed successfully, i.e. json extract is valid can can be inserted into the column. If there is an error, mark it as failed and still insert the other metadata.
    - json_extract - gemini output in json format