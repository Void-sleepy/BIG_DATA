import json
import uuid
import time
import random
import logging
from confluent_kafka import kafkaProducer



# احتياطي




# Updated Kafka configuration: using localhost for Kafka bootstrap server
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'deals'

# Setting up the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('scraper_producer.py')

# Simulate scraping from multiple sites - remove the fixed limit of 5 deals per site

def fetch_deals_from_site(retailer):
    # In production, this function would scrape the website.
    # Here we just simulate generating several deals
    num_deals = random.randint(6, 10)  # generate between 6 to 10 deals instead of fixed 5
    deals = []
    for i in range(num_deals):
        deal = {
            'retailer': retailer,
            'item': f'{retailer} product {i}',
            'price': random.uniform(10, 500),
            'url': f'https://{retailer.lower()}.com/product/{i}',
            'deal_id': str(uuid.uuid4()),
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S')
        }
        deals.append(deal)
    return deals


def deliver_report(err, msg):
    if err is not None:
        logger.error('Message failed delivery: ' + str(err))
    else:
        logger.info('Message delivered to ' + str(msg.topic()) + ' [' + str(msg.partition()) + ']')


def main():
    # Initialize Kafka Producer with updated bootstrap server
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = kafkaProducer(producer_conf)

    # Define the list of retailers to scrape
    retailers = ['BTECH', 'NOON', 'JUMIA']
    all_deals = []

    # Fetch deals from each site
    for retailer in retailers:
        deals = fetch_deals_from_site(retailer)
        all_deals.extend(deals)
        logger.info('Deals fetched: ' + json.dumps(deals))
        # For streaming, immediately produce the deals to Kafka
        for deal in deals:
            producer.produce(KAFKA_TOPIC, json.dumps(deal), callback=deliver_report)
            producer.poll(0)

    producer.flush()
    logger.info('Finished executing scraper_producer.py')

if __name__ == '__main__':
    main()

print('Modified scraper_producer.py executed successfully.')