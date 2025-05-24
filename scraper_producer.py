import json
import requests
from confluent_kafka import Producer
import logging
from bs4 import BeautifulSoup
import random
import time
import re
from playwright.async_api import async_playwright
import asyncio
from datetime import datetime, timezone
import uuid
import os
import urllib.parse
import inspect
from typing import List, Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global products list
products = []

# Kafka config (optional - will work without Kafka)
conf = {
    'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
}

try:
    producer = Producer(conf)
    logger.info("Connected to Kafka")
    KAFKA_ENABLED = True
except Exception as e:
    logger.warning(f"Kafka not available: {e}. Will save to JSON only.")
    KAFKA_ENABLED = False

# Enhanced User-Agents
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
]


def delivery_report(err, msg):
    """Confirm message delivery to Kafka."""
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [partition {msg.partition()}]")


def safe_float(value):
    """Convert string price/discount to float, return 0.0 on failure."""
    try:
        if not value or value == 'N/A':
            return 0.0
        # Remove currency symbols, commas, and % signs
        cleaned = re.sub(r'[^\d.,]', '', str(value))
        cleaned = cleaned.replace(',', '')
        return float(cleaned) if cleaned else 0.0
    except Exception:
        return 0.0


def clean_price(price_text):
    """Clean and format price text."""
    if not price_text:
        return "N/A"
    # Remove common currency symbols and clean up
    cleaned = re.sub(r'[^\d.,\s]', '', price_text)
    return cleaned.strip() or "N/A"


def calculate_discount_percentage(old_price, current_price):
    """Calculate discount percentage between old and current price."""
    try:
        old_val = safe_float(old_price)
        current_val = safe_float(current_price)
        if old_val > current_val and old_val > 0:
            return f"{int((old_val - current_val) / old_val * 100)}%"
    except:
        pass
    return "N/A"


def save_to_json(data: List[Dict], filename: str = None):
    """Save data to JSON file formatted for Cassandra."""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"airfryer_deals_{timestamp}.json"

    # Format data for Cassandra
    cassandra_data = []
    for item in data:
        cassandra_item = {
            "id": str(uuid.uuid4()),
            "retailer": item.get("retailer", "Unknown"),
            "product_name": item.get("item", "Unknown Product"),
            "price": safe_float(item.get("price", 0)),
            "original_price": safe_float(item.get("original_price", 0)),
            "discount_percentage": safe_float(item.get("discount", 0)),
            "product_url": item.get("url", ""),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "currency": "EGP",
            "category": "air_fryer"
        }
        cassandra_data.append(cassandra_item)

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(cassandra_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(cassandra_data)} products to {filename}")
        return filename
    except Exception as e:
        logger.error(f"Failed to save JSON: {e}")
        return None


async def scrape_with_playwright(url: str, selectors: Dict[str, str], retailer: str):
    """Generic Playwright scraper for difficult sites."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled']
            )

            context = await browser.new_context(
                user_agent=random.choice(user_agents),
                viewport={'width': 1920, 'height': 1080}
            )

            page = await context.new_page()

            # Set extra headers
            await page.set_extra_http_headers({
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })

            await page.goto(url, wait_until='networkidle')
            await page.wait_for_timeout(3000)  # Wait for dynamic content

            # Try to find products using multiple selectors
            products_found = []
            for selector_name, selector in selectors.items():
                try:
                    await page.wait_for_selector(selector, timeout=10000)
                    elements = await page.query_selector_all(selector)
                    logger.info(f"{retailer}: Found {len(elements)} elements with selector '{selector_name}'")

                    if elements:
                        for element in elements[:15]:  # Limit to 15 products
                            product_data = await extract_product_data_playwright(element, retailer)
                            if product_data:
                                products_found.append(product_data)
                        break
                except Exception as e:
                    logger.debug(f"{retailer}: Selector '{selector_name}' failed: {e}")
                    continue

            await browser.close()
            return products_found

    except Exception as e:
        logger.error(f"Playwright scraping failed for {retailer}: {e}")
        return []


async def extract_product_data_playwright(element, retailer):
    """Extract product data from a Playwright element."""
    try:
        # Common selectors for different data points
        title_selectors = ['h3', '.name', '.title', '.product-name', 'a[href*="product"]', '.product-title']
        price_selectors = ['.price', '.current-price', '.sale-price', '.final-price', '.value']
        link_selectors = ['a', '[href*="product"]', '[href*="item"]']

        # Extract title
        title = None
        for selector in title_selectors:
            try:
                title_elem = await element.query_selector(selector)
                if title_elem:
                    title = await title_elem.inner_text()
                    if title and len(title.strip()) > 3:
                        title = title.strip()
                        break
            except:
                continue

        # Extract price
        price = None
        for selector in price_selectors:
            try:
                price_elem = await element.query_selector(selector)
                if price_elem:
                    price = await price_elem.inner_text()
                    if price and any(char.isdigit() for char in price):
                        price = clean_price(price)
                        break
            except:
                continue

        # Extract link
        link = None
        for selector in link_selectors:
            try:
                link_elem = await element.query_selector(selector)
                if link_elem:
                    link = await link_elem.get_attribute('href')
                    if link:
                        if not link.startswith('http'):
                            # Make absolute URL based on retailer
                            if 'noon' in retailer.lower():
                                link = 'https://www.noon.com' + link
                            elif 'jumia' in retailer.lower():
                                link = 'https://www.jumia.com.eg' + link
                            elif 'btech' in retailer.lower():
                                link = 'https://btech.com' + link
                            elif 'carrefour' in retailer.lower():
                                link = 'https://www.carrefouregypt.com' + link
                            elif '2b' in retailer.lower():
                                link = 'https://2b.com.eg' + link
                        break
            except:
                continue

        if title and price:
            return {
                'retailer': retailer,
                'item': title,
                'price': price,
                'url': link or '',
                'discount': 'N/A'
            }

    except Exception as e:
        logger.debug(f"Failed to extract product data: {e}")

    return None


def scrape_btech(query):
    """Scrape B.TECH using requests and BeautifulSoup."""
    url = f"https://btech.com/en/catalogsearch/result/?q={query.replace(' ', '+')}"
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://btech.com/en/',
        'Connection': 'keep-alive',
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        products = soup.select('div.product-item-info, .product-item, .item')
        deals = []

        logger.info(f"B.TECH: Found {len(products)} product elements")

        for product in products[:10]:
            title_elem = product.select_one('a.product-item-link, .product-name a, h3 a')
            if not title_elem:
                continue

            title = title_elem.get_text(strip=True)
            product_url = title_elem.get('href', '')

            if not product_url.startswith('http'):
                product_url = 'https://btech.com' + product_url

            price_elem = product.select_one('span.price, .final-price, .regular-price')
            current_price = clean_price(price_elem.get_text() if price_elem else 'N/A')

            old_price_elem = product.select_one('span.old-price span.price, .old-price')
            discount = 'N/A'
            if old_price_elem:
                old_price = clean_price(old_price_elem.get_text())
                discount = calculate_discount_percentage(old_price, current_price)

            deals.append({
                'retailer': 'B.TECH',
                'item': title,
                'price': current_price,
                'url': product_url,
                'discount': discount,
            })

        logger.info(f"B.TECH: Successfully scraped {len(deals)} products")
        return deals

    except Exception as e:
        logger.error(f"B.TECH scraping failed: {e}")
        return []


async def scrape_noon(query):
    """Scrape Noon using Playwright with enhanced selectors."""
    url = f"https://www.noon.com/egypt-en/search/?q={query.replace(' ', '+')}"

    selectors = {
        'products': 'div[data-qa="product-card"], .productContainer, [data-testid="product-card"]',
        'backup': '.sc-gCpHyW, .product-box'
    }

    products = await scrape_with_playwright(url, selectors, 'Noon')
    logger.info(f"Noon: Successfully scraped {len(products)} products")
    return products


def scrape_jumia(query):
    """Scrape Jumia using requests with improved selectors."""
    url = f"https://www.jumia.com.eg/catalog/?q={query.replace(' ', '+')}"
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.jumia.com.eg/',
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        products = soup.select('article.prd, .c-prd, [data-automation-id="product-card"]')
        deals = []

        logger.info(f"Jumia: Found {len(products)} product elements")

        for product in products[:10]:
            title_elem = product.select_one('h3.name, .name, .c-prd-name')
            if not title_elem:
                continue

            title = title_elem.get_text(strip=True)

            link_elem = product.select_one('a.core, a')
            product_url = ''
            if link_elem:
                product_url = link_elem.get('href', '')
                if not product_url.startswith('http'):
                    product_url = 'https://www.jumia.com.eg' + product_url

            price_elem = product.select_one('div.prc, .prc, .c-prd-price')
            current_price = clean_price(price_elem.get_text() if price_elem else 'N/A')

            discount_elem = product.select_one('div.bdg._dsct, ._dsct, .c-prd-discount')
            discount = discount_elem.get_text(strip=True) if discount_elem else 'N/A'

            deals.append({
                'retailer': 'Jumia',
                'item': title,
                'price': current_price,
                'url': product_url,
                'discount': discount,
            })

        logger.info(f"Jumia: Successfully scraped {len(deals)} products")
        return deals

    except Exception as e:
        logger.error(f"Jumia scraping failed: {e}")
        return []


async def scrape_carrefour_egypt(query):
    """Scrape Carrefour Egypt using Playwright."""
    url = f"https://www.carrefouregypt.com/mafegy/en/search?keyword={query.replace(' ', '%20')}"

    selectors = {
        'products': '.product-item, li.product, [data-testid="product"]',
        'backup': '.item, .product-card'
    }

    products = await scrape_with_playwright(url, selectors, 'Carrefour Egypt')
    logger.info(f"Carrefour Egypt: Successfully scraped {len(products)} products")
    return products


async def scrape_2b_egypt(query):
    """Scrape 2B Egypt using Playwright."""
    url = f"https://2b.com.eg/en/catalogsearch/result/?q={query.replace(' ', '+')}"

    selectors = {
        'products': 'li.item.product.product-item, .product-item',
        'backup': '.item, .product'
    }

    products = await scrape_with_playwright(url, selectors, '2B Egypt')
    logger.info(f"2B Egypt: Successfully scraped {len(products)} products")
    return products


async def fetch_all_deals(query, user_id='default_user'):
    """Fetch deals from all retailers and save to JSON."""
    logger.info(f"Starting comprehensive scrape for: {query}")

    all_deals = []

    # Define scrapers with their types
    scrapers = [
        ('B.TECH', scrape_btech, 'sync'),
        ('Noon', scrape_noon, 'async'),
        ('Jumia', scrape_jumia, 'sync'),
        ('Carrefour Egypt', scrape_carrefour_egypt, 'async'),
        ('2B Egypt', scrape_2b_egypt, 'async')
    ]

    for retailer, scraper_func, scraper_type in scrapers:
        logger.info(f"Scraping {retailer}...")
        try:
            if scraper_type == 'async':
                deals = await scraper_func(query)
            else:
                deals = scraper_func(query)

            if deals:
                all_deals.extend(deals)
                logger.info(f"{retailer}: Added {len(deals)} products")
            else:
                logger.warning(f"{retailer}: No products found")

        except Exception as e:
            logger.error(f"Error scraping {retailer}: {e}")

        # Random delay between retailers
        await asyncio.sleep(random.uniform(2, 4))

    # Filter valid deals
    valid_deals = [deal for deal in all_deals if deal.get('item') and deal.get('price') != 'N/A']

    logger.info(f"Total valid deals found: {len(valid_deals)}")

    if valid_deals:
        # Save to JSON for Cassandra
        json_filename = save_to_json(valid_deals)

        # Send to Kafka if enabled
        if KAFKA_ENABLED:
            await send_to_kafka(valid_deals, user_id)

        # Display summary
        print_deal_summary(valid_deals)

        return json_filename, valid_deals
    else:
        logger.warning("No valid deals found!")
        return None, []


async def send_to_kafka(deals, user_id):
    """Send deals to Kafka topic."""
    if not KAFKA_ENABLED:
        return

    sent_count = 0
    for deal in deals:
        deal_data = {
            'deal_id': str(uuid.uuid4()),
            'user_id': user_id,
            'item': deal['item'],
            'retailer': deal['retailer'],
            'price': safe_float(deal['price']),
            'discount': deal.get('discount', 'N/A'),
            'timestamp': datetime.utcnow().isoformat(),
            'url': deal.get('url', '')
        }

        try:
            producer.produce('deals', value=json.dumps(deal_data), callback=delivery_report)
            sent_count += 1
        except Exception as e:
            logger.error(f"Failed to send deal to Kafka: {e}")

    producer.flush()
    logger.info(f"Sent {sent_count} deals to Kafka")


def print_deal_summary(deals):
    """Print a summary of found deals."""
    print("\n" + "=" * 80)
    print("DEAL SUMMARY")
    print("=" * 80)

    retailers = {}
    for deal in deals:
        retailer = deal['retailer']
        if retailer not in retailers:
            retailers[retailer] = []
        retailers[retailer].append(deal)

    for retailer, retailer_deals in retailers.items():
        print(f"\n{retailer} ({len(retailer_deals)} deals):")
        print("-" * 40)
        for i, deal in enumerate(retailer_deals[:5], 1):  # Show first 5
            price_info = f"Price: {deal['price']}"
            if deal.get('discount') != 'N/A':
                price_info += f" (Discount: {deal['discount']})"
            print(f"{i}. {deal['item'][:60]}...")
            print(f"   {price_info}")
            print(f"   URL: {deal.get('url', 'N/A')[:80]}...")
            print()


if __name__ == '__main__':
    async def main():
        try:
            # Get configuration from environment or use defaults
            query = os.environ.get("PRODUCT_QUERY", "air fryer")
            user_id = os.environ.get("USER_ID", "default_user")

            print(f"Searching for deals on: {query}")
            print(f"User ID: {user_id}")
            print("-" * 50)

            # Fetch all deals
            json_file, deals = await fetch_all_deals(query, user_id)

            if json_file:
                print(f"\nSUCCESS: Data saved to: {json_file}")
                print(f"SUMMARY: Total deals found: {len(deals)}")
            else:
                print("\nERROR: No deals found")

            # Container mode for continuous running
            if os.environ.get("CONTAINER_MODE", "false").lower() == "true":
                interval = int(os.environ.get("FETCH_INTERVAL", "3600"))
                logger.info(f"Container mode: Will fetch deals every {interval} seconds")

                while True:
                    await asyncio.sleep(interval)
                    logger.info("Scheduled fetch starting...")
                    await fetch_all_deals(query, user_id)

        except KeyboardInterrupt:
            logger.info("Stopped by user")
        except Exception as e:
            logger.error(f"Script error: {e}")


    # Run the async main function
    asyncio.run(main())
