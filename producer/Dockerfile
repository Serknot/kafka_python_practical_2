FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY producer/producer.py ./producer.py

CMD ["python", "producer.py"]
