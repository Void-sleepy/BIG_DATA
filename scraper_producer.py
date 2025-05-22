import json
import requests
from confluent_kafka import Producer
import logging
from bs4 import BeautifulSoup
import random
import time
import re
from datetime import datetime
import uuid
import os
import math


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Local Kafka config
conf = {
    'bootstrap.servers': 'kafka:9092',
}
try:
    producer = Producer(conf)
    logger.info("Connected to local Kafka at localhost:9092")
except Exception as e:
    logger.error(f"Kafka connection failed: {e}. Ensure Kafka is running (docker-compose up).")
    exit(1)

# Rotate User-Agents to mimic different browsers
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0',
]

def delivery_report(err, msg):
    """Confirm message delivery to Kafka."""
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [partition {msg.partition()}]")

def scrape_btech(query):
    """Scrape B.TECH for specific products with direct links, focusing on discounted items."""
    url = f"https://btech.com/en/catalogsearch/result/?q={query.replace(' ', '+')}"
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://btech.com/en/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        products = soup.select('div.product-item-info')
        deals = []
        
        logger.info(f"B.TECH raw product elements found: {len(products)}")
        
        for product in products:
            title_elem = product.select_one('a.product-item-link')
            if not title_elem:
                continue
                
            title = title_elem.text.strip()
            
            # Get the exact product link - fix URLs to ensure they're specific product pages
            product_url = title_elem.get('href')
            if not product_url:
                continue
            
            # Ensure URL is absolute and goes to a specific product page
            if not product_url.startswith('http'):
                product_url = 'https://btech.com' + product_url
                
            # Extract product ID to ensure we get a specific product
            product_id_match = re.search(r'/([^/]+)\.html', product_url)
            if product_id_match:
                product_id = product_id_match.group(1)
                # Ensure we have a properly formed URL that points to a specific product
                if not re.search(r'/[a-z0-9-]+/[a-z0-9-]+\.html$', product_url):
                    continue
            else:
                continue  # Skip if we can't identify a specific product
                
            # Get current price
            current_price_elem = product.select_one('span.price')
            current_price = current_price_elem.text.replace('EGP', '').strip() if current_price_elem else 'N/A'
            
            # Check for discount
            old_price_elem = product.select_one('span.old-price span.price')
            discount = 'N/A'
            
            if old_price_elem:
                old_price = old_price_elem.text.replace('EGP', '').strip()
                try:
                    old_price_val = float(old_price.replace(',', ''))
                    current_price_val = float(current_price.replace(',', ''))
                    if old_price_val > current_price_val:
                        discount = f"{int((old_price_val - current_price_val) / old_price_val * 100)}%"
                except (ValueError, TypeError):
                    pass
            
            deals.append({
                'retailer': 'B.TECH',
                'item': title,
                'price': current_price,
                'url': product_url,  # Direct product URL
                'discount': discount,
            })
            
            logger.debug(f"Added B.TECH product: {title} - URL: {product_url}")
        
        logger.info(f"Found {len(deals)} B.TECH deals for '{query}'")
        return deals[:10]  # Return up to 10 deals
        
    except Exception as e:
        logger.error(f"B.TECH scraping failed: {e}")
        return []

def scrape_noon(query):
    """Scrape Noon Egypt for specific products using robust session with retries."""
    # Create a session to maintain cookies
    session = requests.Session()
    max_retries = 3
    
    # First visit the homepage to get cookies
    for attempt in range(max_retries):
        try:
            # Use a different user agent for each attempt
            headers_home = {
                'User-Agent': random.choice(user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            session.get("https://www.noon.com/egypt-en/", headers=headers_home, timeout=10)
            break
        except Exception as e:
            logger.warning(f"Noon homepage visit failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                logger.error("Noon initialization failed after all retries")
                return []
            time.sleep(random.uniform(1, 3))  # Random backoff
    
    # Now search for products
    url = f"https://www.noon.com/egypt-en/search/?q={query.replace(' ', '%20')}"
    
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.noon.com/egypt-en/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
    }
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            logger.info(f"Noon search response status: {response.status_code}")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for product grid containers first
            product_grid = soup.select('div[class*="productGrid"]')
            products = []
            
            if product_grid:
                # Find all product tiles within the grid
                products = soup.select('div[data-qa="product-tile"]')
                logger.info(f"Found {len(products)} products in product grid")
            
            # Fallback to searching for links containing product info
            if not products:
                products = soup.select('a[href*="/egypt-en/product/"]')
                logger.info(f"Fallback found {len(products)} product links")
            
            # Another fallback
            if not products:
                products = soup.select('a[href*="/egypt-en/"]')
                logger.info(f"Second fallback found {len(products)} possible product links")
            
            deals = []
            seen_urls = set()  # Track URLs to avoid duplicates
            
            for product in products:
                # For div containers, look for the link inside
                if product.name == 'div':
                    link = product.select_one('a[href*="/egypt-en/"]')
                    if not link:
                        continue
                else:
                    link = product
                
                # Get product URL and ensure it points to a specific product
                product_url = link.get('href')
                if not product_url:
                    continue
                    
                # Filter for product URLs and ensure they're specific
                if '/egypt-en/product/' not in product_url and not re.search(r'/egypt-en/[^/]+/[^/]+/p-\d+', product_url):
                    continue
                    
                # Ensure URL is absolute
                if not product_url.startswith('http'):
                    product_url = 'https://www.noon.com' + product_url
                    
                # Skip duplicates
                if product_url in seen_urls:
                    continue
                    
                seen_urls.add(product_url)
                
                # Try to extract title
                title_elem = link.select_one('[data-qa="product-name"]') or link.select_one('div[title]') or link.find('h2') or link.find('h3')
                title = title_elem.text.strip() if title_elem else "Noon Product"
                
                # Try to extract price
                price_elem = link.select_one('[data-qa="price"]') or link.select_one('span.amount')
                price = price_elem.text.replace('EGP', '').strip() if price_elem else 'N/A'
                
                # Try to extract discount
                discount_elem = link.select_one('[data-qa="discount"]') or link.select_one('div.discount') or link.select_one('span.discount')
                discount = discount_elem.text.strip() if discount_elem else 'N/A'
                
                deals.append({
                    'retailer': 'Noon',
                    'item': title,
                    'price': price,
                    'url': product_url,  # Direct product URL
                    'discount': discount,
                })
                
                # Limit to first 10
                if len(deals) >= 10:
                    break
            
            logger.info(f"Found {len(deals)} Noon deals for '{query}'")
            return deals[:10]
            
        except Exception as e:
            logger.error(f"Noon scraping failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return []
            time.sleep(random.uniform(2, 5))  # Exponential backoff

def scrape_jumia(query):
    """Scrape Jumia Egypt for specific products with direct links, focusing on discounted items."""
    url = f"https://www.jumia.com.eg/catalog/?q={query.replace(' ', '+')}"
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.jumia.com.eg/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        products = soup.select('article.prd')
        deals = []
        
        for product in products:
            # Extract title
            title_elem = product.select_one('h3.name')
            if not title_elem:
                continue
                
            title = title_elem.text.strip()
            
            # Get direct product link
            link_elem = product.select_one('a.core')
            if not link_elem or 'href' not in link_elem.attrs:
                continue
                
            product_url = link_elem['href']
            # Ensure URL is absolute
            if not product_url.startswith('http'):
                product_url = 'https://www.jumia.com.eg' + product_url
            
            # Extract current price
            price_elem = product.select_one('div.prc')
            current_price = price_elem.text.replace('EGP', '').strip() if price_elem else 'N/A'
            
            # Extract discount percentage
            discount_elem = product.select_one('div.bdg._dsct')
            discount = discount_elem.text.strip() if discount_elem else 'N/A'
            
            # Include all products from Jumia
            deals.append({
                'retailer': 'Jumia',
                'item': title,
                'price': current_price,
                'url': product_url,  # Direct product URL
                'discount': discount,
            })
        
        logger.info(f"Found {len(deals)} Jumia deals for '{query}'")
        return deals[:10]  # Return up to 10 deals
        
    except Exception as e:
        logger.error(f"Jumia scraping failed: {e}")
        return []

def scrape_carrefour_egypt(query):
    """Scrape Carrefour Egypt for specific products."""
    url = f"https://www.carrefouregypt.com/mafegy/en/v4/search?keyword={query.replace(' ', '%20')}"
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.carrefouregypt.com/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        products = soup.select('li.product')
        if not products:
            products = soup.select('div.product-item')
        
        deals = []
        
        for product in products:
            # Extract title
            title_elem = product.select_one('h3.product-name') or product.select_one('div.name')
            if not title_elem:
                continue
                
            title = title_elem.text.strip()
            
            # Get product link
            link_elem = product.select_one('a.product-url') or product.find('a')
            if not link_elem or 'href' not in link_elem.attrs:
                continue
                
            product_url = link_elem['href']
            # Ensure URL is absolute
            if not product_url.startswith('http'):
                product_url = 'https://www.carrefouregypt.com' + product_url
            
            # Extract current price
            price_elem = product.select_one('span.current-price') or product.select_one('div.price')
            current_price = price_elem.text.replace('EGP', '').strip() if price_elem else 'N/A'
            
            # Extract discount percentage
            discount_elem = product.select_one('span.discount') or product.select_one('div.discount')
            discount = discount_elem.text.strip() if discount_elem else 'N/A'
            
            # Alternative discount calculation from old price
            if discount == 'N/A':
                old_price_elem = product.select_one('span.old-price')
                if old_price_elem and current_price != 'N/A':
                    old_price = old_price_elem.text.replace('EGP', '').strip()
                    try:
                        old_price_val = float(old_price.replace(',', ''))
                        current_price_val = float(current_price.replace(',', ''))
                        if old_price_val > current_price_val:
                            discount = f"{int((old_price_val - current_price_val) / old_price_val * 100)}%"
                    except (ValueError, TypeError):
                        pass
            
            deals.append({
                'retailer': 'Carrefour Egypt',
                'item': title,
                'price': current_price,
                'url': product_url,
                'discount': discount,
            })
        
        logger.info(f"Found {len(deals)} Carrefour Egypt deals for '{query}'")
        return deals[:10]  # Return up to 10 deals
        
    except Exception as e:
        logger.error(f"Carrefour Egypt scraping failed: {e}")
        return []

def scrape_2b_egypt(query):
    """Scrape 2B Egypt for specific products."""
    url = f"https://2b.com.eg/en/catalogsearch/result/?q={query.replace(' ', '+')}"
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://2b.com.eg/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        products = soup.select('li.item.product.product-item')
        deals = []
        
        for product in products:
            # Extract title
            title_elem = product.select_one('a.product-item-link')
            if not title_elem:
                continue
                
            title = title_elem.text.strip()
            
            # Get product link
            product_url = title_elem['href'] if 'href' in title_elem.attrs else None
            if not product_url:
                continue
            
            # Extract current price
            price_elem = product.select_one('span.price')
            current_price = price_elem.text.replace('EGP', '').strip() if price_elem else 'N/A'
            
            # Check for discount
            old_price_elem = product.select_one('span.old-price span.price')
            discount = 'N/A'
            
            if old_price_elem:
                old_price = old_price_elem.text.replace('EGP', '').strip()
                try:
                    old_price_val = float(old_price.replace(',', ''))
                    current_price_val = float(current_price.replace(',', ''))
                    if old_price_val > current_price_val:
                        discount = f"{int((old_price_val - current_price_val) / old_price_val * 100)}%"
                except (ValueError, TypeError):
                    pass
            
            deals.append({
                'retailer': '2B Egypt',
                'item': title,
                'price': current_price,
                'url': product_url,
                'discount': discount,
            })
        
        logger.info(f"Found {len(deals)} 2B Egypt deals for '{query}'")
        return deals[:10]  # Return up to 10 deals
        
    except Exception as e:
        logger.error(f"2B Egypt scraping failed: {e}")
        return []

def fetch_deals(query, user_id='mock_user'):
    """Fetch deals from multiple Egyptian retailers and send to Kafka with improved error handling."""
    retailers = {
        'B.TECH': scrape_btech,
        'Noon': scrape_noon,
        'Jumia': scrape_jumia,
        'Carrefour Egypt': scrape_carrefour_egypt,
        '2B Egypt': scrape_2b_egypt
    }
    all_deals = []
    
    for retailer, scrape_func in retailers.items():
        logger.info(f"Scraping {retailer} for {query} (user: {user_id})")
        
        # Add retry logic for each retailer
        max_retries = 2
        for attempt in range(max_retries):
            try:
                deals = scrape_func(query)
                if deals:  # Only consider successful if we got some deals
                    all_deals.extend(deals)
                    break
                else:
                    logger.warning(f"No deals found from {retailer} (attempt {attempt+1}/{max_retries})")
                    if attempt < max_retries - 1:  # Don't sleep after last attempt
                        time.sleep(random.uniform(1, 3) * (attempt + 1))  # Exponential backoff
            except Exception as e:
                logger.error(f"Error scraping {retailer} (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3) * (attempt + 1))
        
        # Add a delay between retailers to avoid rate-limiting
        time.sleep(random.uniform(2, 5))
    
    # Add validation for URLs before sending to Kafka
    valid_deals = []
    for deal in all_deals:
        # Validate URL format
        url = deal.get('url', '')
        if url and url.startswith('http') and ('.' in url):
            valid_deals.append(deal)
        else:
            logger.warning(f"Skipping deal with invalid URL: {deal}")
    
    if not valid_deals:
        logger.warning(f"No valid deals found for query: {query}. Using default mock data.")
        retailers_list = ['B.TECH', 'Noon', 'Jumia', 'Carrefour Egypt', '2B Egypt']
        for retailer in retailers_list:
            # Create realistic mock URLs based on retailer
            if retailer == 'B.TECH':
                mock_url = f'https://btech.com/en/{query.replace(" ", "-")}.html'
            elif retailer == 'Noon':
                mock_url = f'https://www.noon.com/egypt-en/search/?q={query.replace(" ", "%20")}'
            elif retailer == 'Jumia':
                mock_url = f'https://www.jumia.com.eg/{query.replace(" ", "-")}'
            elif retailer == 'Carrefour Egypt':
                mock_url = f'https://www.carrefouregypt.com/mafegy/en/v4/search?keyword={query.replace(" ", "%20")}'
            else:  # 2B Egypt
                mock_url = f'https://2b.com.eg/en/catalogsearch/result/?q={query.replace(" ", "+")}'
                
            deal = {
                'user_id': user_id,
                'item': f"{query.title()} {random.choice(['Standard', 'Premium', 'Deluxe', 'Basic'])} Model",
                'retailer': retailer,
                'price': str(random.randint(800, 3000)),
                'discount': f"{random.randint(10, 40)}%",
                'timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                'url': mock_url
            }
            producer.produce('deals', value=json.dumps(deal), callback=delivery_report)
            producer.flush()
            logger.info(f"Sent mock deal to Kafka: {deal}")
        return

    # Send each valid deal to Kafka
    for deal in valid_deals:
        deal_data = {
            'deal_id': str(uuid.uuid4()),
            'user_id': deal.get('user_id', 'mock_user'),
            'item': deal['item'],
            'retailer': deal['retailer'],
            'price': float(deal['price'].replace(',', '')),
            'discount': float(deal.get('discount', 0)),
            'timestamp': datetime.utcnow().isoformat(),
            'url': deal['url']
        }

        try:
            producer.produce('deals', value=json.dumps(deal_data), callback=delivery_report)
            producer.flush()
            logger.info(f"Sent deal to Kafka: {deal_data}")
        except Exception as e:
            logger.error(f"Failed to send deal to Kafka: {e}")

# User input for testing
if __name__ == '__main__':
    try:
        # Get product search query from environment variable or use a default
        user_query = os.environ.get("PRODUCT_QUERY", "air fryer")
        user_id = os.environ.get("USER_ID", "mock_user")

        logger.info(f"Searching for deals on: {user_query} for user: {user_id}")
        fetch_deals(query=user_query, user_id=user_id)

        # For continuous operation in a container, keep the script alive
        if os.environ.get("CONTAINER_MODE", "false").lower() == "true":
            logger.info("Running in container mode. Script will sleep and periodically fetch deals.")
            while True:
                time.sleep(int(os.environ.get("FETCH_INTERVAL", "3600")))  # Default: fetch every hour
                logger.info(f"Scheduled fetch for deals on: {user_query}")
                fetch_deals(query=user_query, user_id=user_id)

    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Script error: {e}")