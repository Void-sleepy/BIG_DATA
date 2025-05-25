#!/bin/bash

echo "Starting Big Data Deal Aggregator Services..."

# Clean up any existing containers
echo "Cleaning up existing containers..."
docker-compose down --volumes --remove-orphans

# Build and start services
echo "Building and starting services..."
docker-compose up --build

echo "Services started successfully!"
echo "Access Spark UI at: http://localhost:8080"
echo "Kafka is available on: localhost:9092"
echo "Cassandra is available on: localhost:9042"
