FROM python:3.12-slim

WORKDIR /duckdb_service

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "duckdb_worker.py"]