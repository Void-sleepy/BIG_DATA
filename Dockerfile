FROM bitnami/spark:3.3.0

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip python3-dev netcat curl wget build-essential openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Python dependencies
RUN pip3 install --no-cache-dir \
    kafka-python==2.0.2 \
    confluent-kafka==2.3.0 \
    requests==2.31.0 \
    pandas==1.5.3 \
    beautifulsoup4==4.12.2 \
    pyspark==3.3.0 \
    cassandra-driver==3.29.1 \
    python-dotenv==1.0.0 \
    playwright==1.40.0 \
    lxml==4.9.3 \
    selenium==4.15.2 \
    aiohttp==3.9.1 \
    python-dateutil==2.8.2

# Install Playwright browser
RUN playwright install chromium && playwright install-deps

# Create required directories
RUN mkdir -p /opt/bitnami/spark/jars /app/data /tmp/checkpoint

# Download Spark-Kafka connector and Cassandra connector JARs
RUN wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar \
    && wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar \
    && wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/spark-cassandra-connector_2.12-3.3.0.jar \
    && wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    && wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.14.1/java-driver-core-4.14.1.jar

WORKDIR /app

COPY . /app/

# Set proper permissions
RUN chown -R 1001:1001 /app /tmp/checkpoint

# Switch back to non-root user
USER 1001
