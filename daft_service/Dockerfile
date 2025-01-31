FROM python:3.12-slim

WORKDIR /app
COPY daft_service/requirements.txt .
RUN pip install -r requirements.txt

COPY daft_service/ .

CMD ["python", "daft_worker.py"]