#!/usr/bin/env python3
import json
import logging
from confluent_kafka import Consumer, Producer, KafkaError
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_kafka_producer():
    """Test sending a message to Kafka"""
    conf = {'bootstrap.servers': 'kafka:9092'}
    producer = Producer(conf)
    
    test_message = {
        "deal_id": "test-123",
        "user_id": "test_user",
        "item": "Test Air Fryer",
        "retailer": "Test Store",
        "price": 99.99,
        "discount": "20%",
        "timestamp": "2025-05-24T12:41:18.000Z",
        "url": "http://test.com"
    }
    
    try:
        producer.produce('deals', value=json.dumps(test_message))
        producer.flush()
        logger.info("Test message sent successfully")
    except Exception as e:
        logger.error(f"Failed to send test message: {e}")

def test_kafka_consumer():
    """Test consuming messages from Kafka"""
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'debug-consumer',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(['deals'])
    
    logger.info("Starting to consume messages...")
    
    try:
        while True:
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                logger.info("No message received")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("End of partition")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
            else:
                logger.info(f"Received message: {msg.value().decode('utf-8')}")
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'produce':
        test_kafka_producer()
    else:
        test_kafka_consumer()
