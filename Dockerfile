FROM python:3.9-slim

WORKDIR /app

# Install system dependencies including librdkafka for confluent-kafka
RUN apt-get update && \
    apt-get install -y openjdk-11-jre curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
# Copy requirements first to leverage Docker cache

RUN pip install cassandra-driver pyspark kafka-python

# Copy source code
COPY . .

CMD ["python", "streaming.py"]
