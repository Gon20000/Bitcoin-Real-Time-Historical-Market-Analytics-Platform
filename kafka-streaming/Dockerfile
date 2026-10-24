FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for confluent-kafka / librdkafka if needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

COPY producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

COPY producer/ /app/producer/

CMD ["python", "-u", "/app/producer/main.py"]
