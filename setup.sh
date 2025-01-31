#!/bin/bash

# Create app directory and files
mkdir -p app
touch app/__init__.py
touch app/main.py
touch app/etl_pipeline.py
touch app/utils.py
touch app/secrets.toml

# Create daft_service directory and files
mkdir -p daft_service
touch daft_service/__init__.py
touch daft_service/daft_worker.py
touch daft_service/requirements.txt

# Create duckdb_service directory and files
mkdir -p duckdb_service
touch duckdb_service/Dockerfile
touch duckdb_service/init.sql
touch duckdb_service/requirements.txt

# Create data directory and subdirectories
mkdir -p data/logs

# Create top-level files
touch docker-compose.yml
touch Dockerfile
touch README.md
touch requirements.txt
touch .env

# Add placeholder content to some files
cat <<EOL > README.md
# Invoice Processing App

This project processes invoice PDFs using Gemini Flash APIs, Daft, and DuckDB. 
It provides a Streamlit interface for uploading PDFs, extracting data, and managing the extracted information.

## Setup Instructions

1. Run \`./setup.sh\` to create the folder structure.
2. Install dependencies: \`pip install -r requirements.txt\`.
3. Start the application: \`docker-compose up --build\`.

For more details, refer to the documentation.
EOL

cat <<EOL > requirements.txt
streamlit==1.22.0
duckdb==0.8.1
daft==0.1.0
requests==2.31.0
pdfplumber==0.9.0
google-generativeai==0.1.0
EOL

cat <<EOL > .env
GEMINI_API_KEY=your_gemini_api_key_here
DUCKDB_PATH=/data/invoices.db
EOL

cat <<EOL > docker-compose.yml
version: '3.8'
services:
  streamlit:
    build: .
    ports:
      - "8501:8501"
    volumes:
      - ./app:/app
    environment:
      - GEMINI_API_KEY=\${GEMINI_API_KEY}
    depends_on:
      - duckdb
      - daft

  duckdb:
    build: ./duckdb_service
    volumes:
      - ./data:/data

  daft:
    build: ./daft_service
    volumes:
      - ./app:/app
EOL

cat <<EOL > duckdb_service/Dockerfile
FROM python:3.9-slim
RUN pip install duckdb
COPY init.sql /init.sql
CMD ["duckdb", "/data/invoices.db", ".read /init.sql"]
EOL

cat <<EOL > duckdb_service/init.sql
CREATE TABLE IF NOT EXISTS invoices (
    invoice_id STRING,
    status STRING,
    vendor_info STRING,
    total_amount FLOAT,
    tax_details STRING,
    line_items STRING,
    json_extract STRING
);
EOL

echo "Project folder structure created successfully!"