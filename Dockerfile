FROM python:3.9-slim

WORKDIR /app

# Install system dependencies including librdkafka for confluent-kafka
RUN apt-get update && \
    apt-get install -y openjdk-11-jre curl netcat-openbsd && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies with retry logic
RUN pip install --no-cache-dir -r requirements.txt && \
    # Add additional required dependencies
    pip install --no-cache-dir confluent-kafka pyspark==3.4.1 cassandra-driver

# Copy source code
COPY . .

# Add healthcheck script
COPY healthcheck.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/healthcheck.sh

# Wait for Kafka and Cassandra to be available before starting
COPY wait-for-it.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/wait-for-it.sh

CMD ["/bin/bash", "-c", "/usr/local/bin/wait-for-it.sh cassandra:9042 -t 120 && /usr/local/bin/wait-for-it.sh kafka:9092 -t 120 && python streaming.py"]