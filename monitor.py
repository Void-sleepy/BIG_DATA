#!/usr/bin/env python3
"""
Big Data Pipeline Monitor
Monitors the health and performance of all services
"""
import time
import requests
import subprocess
import json
from confluent_kafka import Consumer, Producer
from cassandra.cluster import Cluster

def check_service_health():
    services = {
        'Spark Master': 'http://localhost:8080',
        'Spark Worker': 'http://localhost:8081',
    }
    
    print("=== SERVICE HEALTH CHECK ===")
    for service, url in services.items():
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"✅ {service}: OK")
            else:
                print(f"❌ {service}: ERROR ({response.status_code})")
        except Exception as e:
            print(f"❌ {service}: ERROR ({e})")

def check_kafka():
    print("\n=== KAFKA HEALTH CHECK ===")
    try:
        # Check if Kafka is responding
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'health-check',
            'auto.offset.reset': 'latest'
        })
        
        # List topics
        metadata = consumer.list_topics(timeout=10)
        topics = list(metadata.topics.keys())
        print(f"✅ Kafka: OK (Topics: {topics})")
        consumer.close()
    except Exception as e:
        print(f"❌ Kafka: ERROR ({e})")

def check_cassandra():
    print("\n=== CASSANDRA HEALTH CHECK ===")
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        
        # Check keyspace
        result = session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='deals_keyspace'")
        if result.one():
            print("✅ Cassandra: OK (deals_keyspace exists)")
        else:
            print("⚠️  Cassandra: OK but deals_keyspace not found")
        
        cluster.shutdown()
    except Exception as e:
        print(f"❌ Cassandra: ERROR ({e})")

def check_docker_containers():
    print("\n=== DOCKER CONTAINERS ===")
    try:
        result = subprocess.run(['docker', 'ps', '--format', 'table {{.Names}}\t{{.Status}}'], 
                              capture_output=True, text=True)
        print(result.stdout)
    except Exception as e:
        print(f"❌ Docker: ERROR ({e})")

if __name__ == '__main__':
    while True:
        print(f"\n{'='*50}")
        print(f"HEALTH CHECK - {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*50}")
        
        check_docker_containers()
        check_service_health()
        check_kafka()
        check_cassandra()
        
        print(f"\nNext check in 60 seconds...")
        time.sleep(60)
