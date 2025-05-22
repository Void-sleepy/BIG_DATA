FROM python:3.9-slim-bullseye
FROM bitnami/spark:3.3.0

WORKDIR /app

# Install system dependencies including librdkafka for confluent-kafka
USER root
RUN apt-get update && \
    apt-get install -y wget apt-transport-https gnupg curl netcat-openbsd python3 python3-pip && \
    wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add - && \
    echo "deb https://packages.adoptium.net/artifactory/deb $(grep VERSION_CODENAME /etc/os-release | cut -d= -f2) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y temurin-11-jre && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


RUN pip install cassandra-driver kafka-python pyspark==3.3.0 numpy pandas scikit-learn


# Add Spark Kafka connector and Cassandra connector
RUN mkdir -p /opt/bitnami/spark/jars
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/spark-cassandra-connector_2.12-3.3.0.jar

# Copy requirements first to leverage Docker cache
COPY requirements.txt .



# Install Python dependencies with retry logic
RUN pip3 install --no-cache-dir -r requirements.txt && \
    # Add additional required dependencies
    pip3 install --no-cache-dir confluent-kafka pyspark==3.3.0 cassandra-driver

# Copy source code
COPY . .

# Add wait-for-it script
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

WORKDIR /app

# Command to run
CMD ["/bin/bash", "-c", "/usr/local/bin/wait-for-it.sh cassandra:9042 -t 120 && /usr/local/bin/wait-for-it.sh kafka:9092 -t 120 && python3 streaming.py"]
