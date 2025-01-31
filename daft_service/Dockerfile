FROM python:3.12-slim

WORKDIR /daft_service

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "daft_worker.py"]