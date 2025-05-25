# Use bitnami/spark as base image for compatibility
FROM bitnami/spark:3.3.0

# Switch to root user to install packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    netcat \
    curl \
    wget \
    build-essential \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Python packages
RUN pip3 install --no-cache-dir \
    kafka-python \
    confluent-kafka \
    requests \
    pandas \
    beautifulsoup4 \
    pyspark==3.3.0 \
    cassandra-driver \
    python-dotenv \
    playwright \
    lxml \
    selenium \
    aiohttp \
    python-dateutil

# Install Playwright browsers
RUN playwright install chromium
RUN playwright install-deps

# Create necessary directories
RUN mkdir -p /opt/bitnami/spark/jars
RUN mkdir -p /app/data
RUN mkdir -p /tmp/checkpoint

# Download required JAR files
RUN wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar

RUN wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar

RUN wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/spark-cassandra-connector_2.12-3.3.0.jar

RUN wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.14.1/java-driver-core-4.14.1.jar
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/spark-cassandra-connector_2.12-3.3.0.jar

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app/

# Set environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PYTHONPATH=/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.5-src.zip
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Set permissions
RUN chown -R 1001:1001 /app
RUN chown -R 1001:1001 /tmp/checkpoint
RUN chmod +x /app/*.py

# Switch back to spark user
USER 1001

# Default command
CMD ["python", "/app/streaming.py"]
