import asyncio
import aiohttp
import random
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any
import logging
import asyncpg
import json

# --- Database Config ---
db_params = {
    'user': 'postgres',
    'password': 'rasiqsir',
    'database': 'newVenta3',
    'host': 'localhost',
    'port': '5432'
}

# --- Marketplace Domain Map ---
marketplace_domains = {
    "Amazon Australia": "com.au",
    "Amazon UK": "co.uk",
    "Amazon France": "fr",
    "Amazon Italy": "it",
    "Amazon Spain": "es",
    "Amazon Poland": "pl",
    "Amazon Sweden": "se",
    "Amazon Netherlands": "nl",
    "Amazon Belgium": "be",
    "Amazon UAE": "ae",
    "Amazon KSA": "sa",
    "Amazon US": "com",
    "Amazon India": "in",
    "Amazon Germany": "de"
}


def get_domain_from_name(name):
    for key, domain in marketplace_domains.items():
        if key in name:
            return domain
    return "unknown"


# --- Logging setup ---
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)
logger.propagate = False


# --- Headers for async aiohttp requests (user-agent rotating) ---
def get_headers() -> Dict[str, str]:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
    }


# --- Fixed headers for synchronous negative review scraping ---
fixed_headers = {
    'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'DNT': '1',
}

# --- Fetch products from DB ---
async def fetch_products_from_db(pool) -> List[Dict[str, Any]]:
    async with pool.acquire() as connection:
        query = """
        SELECT DISTINCT ON (a.asin, a.fk_client_id, a.fk_mp_id, a.mp_sku_code, a.dt_delete)
            a.pk_az_product_id,
            a.mp_sku_code,
            a.focused_sku,
            a.fk_sku_id,
            a.fk_mp_id,
            a.fk_client_id,
            a.asin,
            c.name AS marketplace_name
        FROM amazon_product a
        JOIN com_marketplace c ON a.fk_mp_id = c.pk_mp_id
        WHERE a.dt_delete IS NULL
            AND a.focused_sku = TRUE
        LIMIT 100;
        """
        rows = await connection.fetch(query)
        product_list = []
        for row in rows:
            pk_az_product_id = row['pk_az_product_id']
            mp_sku_code = row['mp_sku_code']
            focused_sku = row['focused_sku']
            fk_sku_id = row['fk_sku_id']
            fk_mp_id = row['fk_mp_id']
            fk_client_id = row['fk_client_id']
            asin = row['asin']
            marketplace_name = row['marketplace_name']
            clean_asin = asin.strip("() ").upper()
            domain = get_domain_from_name(marketplace_name)
            product_list.append({
                "pk_az_product_id": pk_az_product_id,
                "mp_sku_code": mp_sku_code,
                "focused_sku": focused_sku,
                "fk_sku_id": fk_sku_id,
                "fk_mp_id": fk_mp_id,
                "fk_client_id": fk_client_id,
                "asin": clean_asin,
                "marketplace_name": marketplace_name,
                "domain": domain,
            })
        return product_list


# --- Async scrape product rating and review count (with retries) ---
async def scrape_product_data(session: aiohttp.ClientSession, asin: str, country_code: str, max_retries=4) -> Dict[str, Any]:
    url = f"https://www.amazon.{country_code}/dp/{asin}?th=1&psc=1"
    data = {"asin": asin, "country_code": country_code, "url": url}
    for attempt in range(1, max_retries + 1):
        logger.info(f"Scraping product data for ASIN {asin} from URL: {url}, attempt {attempt}")
        try:
            async with session.get(url, headers=get_headers(), timeout=15) as response:
                response_text = await response.text()
                if response.status != 200:
                    logger.error(f"Failed to fetch {asin}. Status: {response.status}. URL: {url}")
                    logger.debug(f"Response body for failed request ({asin}):\n{response_text[:1000]}")
                    if attempt == max_retries:
                        return {**data, "error": f"HTTP {response.status}"}
                    else:
                        await asyncio.sleep(2)
                        continue
                if "captcha" in response_text.lower() or "api-services-support@amazon.com" in response_text.lower():
                    logger.warning(f"CAPTCHA detected for ASIN {asin}. URL: {url}")
                    if attempt == max_retries:
                        return {**data, "error": "CAPTCHA or block page detected"}
                    else:
                        await asyncio.sleep(2)
                        continue
                soup = BeautifulSoup(response_text, "lxml")
                rating_element = soup.select_one("#acrPopover span.a-icon-alt")
                data["rating"] = float(rating_element.text.split()[0]) if rating_element else None
                review_count_element = soup.select_one("#acrCustomerReviewText")
                data["review_count"] = (
                    int("".join(filter(str.isdigit, review_count_element.text))) if review_count_element else None
                )
                logger.info(f"Successfully scraped product data for {asin}.")
                return data
        except asyncio.TimeoutError:
            logger.error(f"Timeout error scraping product {asin} from {url}")
            if attempt == max_retries:
                return {**data, "error": "Request timed out"}
            else:
                await asyncio.sleep(2)
                continue
        except Exception as e:
            logger.error(f"An unexpected error occurred while scraping product {asin}: {e}", exc_info=True)
            if attempt == max_retries:
                return {**data, "error": str(e)}
            else:
                await asyncio.sleep(2)
                continue


# --- Async fetch of negative reviews (previously sync, now async) ---
async def fetch_negative_reviews_async(session: aiohttp.ClientSession, country_code: str, asin: str, pages: int = 4):
    base_url = (
        f"https://www.amazon.{country_code}/dp/{asin}/ref=mp_s_a_1_1"
        "?_encoding=UTF8&content-id=amzn1.sym.a64ac9fc-2717-4051-ae96-b2a9a2a6b039"
        "&keywords=headphones&pd_rd_r=b16cf366-1660-42d7-bbd1-6d06604b557c"
        "&pd_rd_w=JlDXh&pd_rd_wg=nou18&qid=1756144054&sr=8-1&th=1#averageCustomerReviewsAnchor"
    )
    reviews = []
    seen = set()
    for page_no in range(1, pages + 1):
        params = {
            'ie': 'UTF8',
            'reviewerType': 'all_reviews',
            'filterByStar': 'critical',
            'sortBy': 'recent',
            'pageNumber': page_no,
        }
        try:
            async with session.get(base_url, params=params, headers=fixed_headers, timeout=15) as response:
                text = await response.text()
                logger.info(f"Fetched page {page_no} with status {response.status} for ASIN {asin}")
                if response.status != 200:
                    logger.warning(f"Failed to fetch page {page_no}: HTTP {response.status}")
                    continue
                soup = BeautifulSoup(text, 'lxml')
                boxes = soup.select('li[data-hook="review"]')
                if not boxes:
                    logger.info(f"No reviews found on page {page_no}, stopping.")
                    break
                for box in boxes:
                    try:
                        star_elem = box.select_one('[data-hook="review-star-rating"]') or box.select_one('[data-hook="cmps-review-star-rating"]')
                        if star_elem:
                            split_star = star_elem.get_text(strip=True).split(' ')[0]
                            try:
                                stars = float(split_star.replace(',', '.'))
                            except ValueError:
                                stars = None
                        else:
                            stars = None
                        if stars is None or stars >= 4:
                            continue
                        title_elem = box.select_one('[data-hook="review-title"]')
                        name_elem = box.select_one('.a-profile-name')
                        date_elem = box.select_one('[data-hook="review-date"]')
                        body_elem = box.select_one('[data-hook="review-body"]')
                        title = title_elem.get_text(strip=True) if title_elem else 'N/A'
                        name = name_elem.get_text(strip=True) if name_elem else 'N/A'
                        date_str = date_elem.get_text(strip=True).split(' on ')[-1] if date_elem else 'N/A'
                        try:
                            date = datetime.strptime(date_str, '%d %B %Y').strftime("%d/%m/%Y")
                        except Exception:
                            date = date_str
                        body = body_elem.get_text(strip=True) if body_elem else 'N/A'
                        review_key = f"{name}|{title}|{date}"
                        if review_key in seen:
                            continue
                        seen.add(review_key)
                        reviews.append({
                            'Name': name,
                            'Stars': stars,
                            'Title': title,
                            'Date': date,
                            'Description': body,
                        })
                    except Exception as e:
                        logger.warning(f"Parsing error: {e}")
        except Exception as e:
            logger.error(f"Error fetching negative reviews page {page_no}: {e}")
        await asyncio.sleep(2)
    return reviews


# --- Insert / Update scraped data in DB safely, with soft delete of previous existing ---
async def insert_rating_data(pool, product: Dict[str, Any]):
    async with pool.acquire() as connection:
        fk_id = product["pk_az_product_id"]
        rating = product.get("rating")
        rating_count = product.get("review_count")
        reviews = product.get("negative_reviews")

        # Soft delete existing active records for this product
        soft_delete_query = """
            UPDATE amazon_analysis_rating
            SET dt_delete = NOW()
            WHERE fk_amazon_product_id = $1 AND dt_delete IS NULL;
        """
        await connection.execute(soft_delete_query, fk_id)

        # Insert new record with current scraped data - using JSONB column
        insert_query = """
        INSERT INTO amazon_analysis_rating
            (fk_amazon_product_id, rating, rating_count, critical_reviews, dt_create, dt_update)
        VALUES ($1, $2, $3, $4, NOW(), NOW())
        ON CONFLICT (fk_amazon_product_id, dt_delete)
        DO UPDATE SET
            rating = COALESCE(EXCLUDED.rating, amazon_analysis_rating.rating),
            rating_count = COALESCE(EXCLUDED.rating_count, amazon_analysis_rating.rating_count),
            critical_reviews = COALESCE(EXCLUDED.critical_reviews, amazon_analysis_rating.critical_reviews),
            dt_update = NOW();
        """
        await connection.execute(
            insert_query,
            fk_id,
            rating if rating is not None else None,
            rating_count if rating_count is not None else None,
            json.dumps(reviews) if reviews else None,
        )
        logger.info(f"Soft deleted old and inserted/updated rating data for Product ID {fk_id} (ASIN {product['asin']})")


# --- Process a single product ---
async def process_product(session, pool, product):
    asin, domain = product["asin"], product["domain"]
    product_data = await scrape_product_data(session, asin, domain)
    if "error" in product_data:
        logger.warning(f"Skipping ASIN {asin} due to error: {product_data['error']}")
        return None
    logger.info(f"Starting async negative review scraping for ASIN {asin}")
    negative_reviews = await fetch_negative_reviews_async(session, domain, asin)
    product_data.update({
        **product,
        "negative_reviews": negative_reviews,
        "negative_review_count": len(negative_reviews),
    })
    # Insert / update DB with soft delete old records
    await insert_rating_data(pool, product_data)
    return product_data


# --- Main entry point ---
async def main():
    pool = await asyncpg.create_pool(**db_params)
    products = await fetch_products_from_db(pool)
    logger.info(f"Got {len(products)} products from database")

    semaphore = asyncio.Semaphore(10)  # concurrency limit

    async with aiohttp.ClientSession() as session:

        async def sem_task(product):
            async with semaphore:
                return await process_product(session, pool, product)

        tasks = [sem_task(product) for product in products]
        results = await asyncio.gather(*tasks)

    results = [res for res in results if res]

    for product in results:
        asin = product.get("asin")
        print(f"\nASIN: {asin}")
        print(f"Rating: {product.get('rating')}")
        print(f"Total Reviews Count: {product.get('review_count')}")
        print(f"Negative Review Count: {product.get('negative_review_count')}")
        print("Critical Reviews:")
        for review in product["negative_reviews"]:
            print(f" - {review['Description']}")
            print("-" * 40)


if __name__ == "__main__":
    asyncio.run(main())
