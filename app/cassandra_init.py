import time  
import logging  
import sys
import traceback
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
  
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')  
  
CASSANDRA_HOSTS = ['cassandra']  # Make sure this matches your docker-compose service name  
MAX_RETRIES = 20  
RETRY_INTERVAL = 5  
  
def create_keyspace_and_tables():  
    auth_provider = None  # Configure if you have credentials  
    retries = 0  
    while retries < MAX_RETRIES:  
        try:  
            cluster = Cluster(CASSANDRA_HOSTS, auth_provider=auth_provider)  
            session = cluster.connect()  
            logging.info("Connected to Cassandra")  
              
            # Create keyspace if it doesn't exist  
            session.execute("""  
            CREATE KEYSPACE IF NOT EXISTS deals_keyspace   
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  
            """)  
            session.set_keyspace("deals_keyspace")  
              
            # Create tables  
            session.execute("""  
            CREATE TABLE IF NOT EXISTS deals (  
                deal_id uuid PRIMARY KEY,  
                user_id text,  
                item text,  
                retailer text,  
                price float,  
                discount float,  
                url text,  
                processed_timestamp timestamp  
            )  
            """)  
            session.execute("""  
            CREATE TABLE IF NOT EXISTS recommended_deals (  
                recommendation_id uuid PRIMARY KEY,  
                user_id text,  
                deal_id uuid,  
                recommendation_score float,  
                recommendation_timestamp timestamp  
            )  
            """)  
            logging.info("Keyspace and tables created successfully")  
            return True  
        except Exception as e:  
            logging.error("Error creating keyspace/tables:\n" + traceback.format_exc())
            retries += 1  
            time.sleep(RETRY_INTERVAL)  
    logging.error("Exceeded max retries. Exiting.")  
    return False  
  
if __name__ == '__main__':  
    if not create_keyspace_and_tables():  
        sys.exit(1)  
    logging.info("Cassandra initialization complete")  