# Use a lightweight Python image
FROM python:3.12-slim

# Install DuckDB
RUN pip install duckdb

# Copy the initialization script
COPY init.sql /init.sql

# Set up the database file
VOLUME /data
CMD ["duckdb", "/data/invoices.db", ".read /init.sql"]