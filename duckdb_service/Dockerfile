FROM python:3.9-slim
RUN pip install duckdb
COPY init.sql /init.sql
CMD ["duckdb", "/data/invoices.db", ".read /init.sql"]
