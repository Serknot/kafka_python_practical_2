FROM python:3.11-slim

WORKDIR /app

COPY consumer_batch/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY consumer_batch/consumer_batch.py ./consumer_batch.py

CMD ["python", "consumer_batch.py"]
