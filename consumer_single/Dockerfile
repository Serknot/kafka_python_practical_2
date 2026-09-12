FROM python:3.11-slim

WORKDIR /app

COPY consumer_single/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY consumer_single/consumer_single.py ./consumer_single.py

CMD ["python", "consumer_single.py"]
