
import requests
from bs4 import BeautifulSoup
import logging
from decimal import Decimal
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Thread-local sessions
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        thread_local.session = session
    return thread_local.session

# Example user agents (expand this list for production use)
user_agent_list = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:126.0) Gecko/20100101 Firefox/126.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
]

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
    "Amazon Germany": "de",
}

def get_marketplace_domain(name):
    return marketplace_domains.get(name, "in")

def fetch_search_results(query: str, marketplace: str = "Amazon India"):
    domain = get_marketplace_domain(marketplace)
    BASE_URL = f"https://www.amazon.{domain}/s"
    params = {"k": query}
    # Rotate user agents per request
    headers = {
        'User-Agent': random.choice(user_agent_list),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }
    logger.info(f"Fetching Amazon results for: {query} in {marketplace}")
    session = get_session()
    try:
        resp = session.get(BASE_URL, headers=headers, params=params, timeout=20)
    except Exception as e:
        logger.error(f"Failed request for query '{query}': {str(e)}")
        return []
    if resp.status_code != 200:
        logger.error(f"Failed to fetch page. HTTP {resp.status_code}")
        return []
    html = resp.text
    soup = BeautifulSoup(html, "lxml")
    products = []
    items = soup.select('div.s-result-item[data-asin]')
    seen_asins = set()
    logger.info(f"Found {len(items)} items in search page for {query}")
    for item in items[:15]:  # Top 15 results
        asin = item.get("data-asin")
        if not asin or not asin.strip() or asin in seen_asins:
            continue
        seen_asins.add(asin)
        title_tag = item.select_one("h2 span")
        title = title_tag.get_text(strip=True) if title_tag else None
        url_tag = item.select_one("a.a-link-normal.s-line-clamp-2.s-line-clamp-3-for-col-12.s-link-style.a-text-normal")
        product_url = f"https://www.amazon.{domain}{url_tag['href']}" if url_tag else None
        img = item.select_one("img.s-image")
        image_url = img["src"] if img else None
        rating_tag = item.select_one("span.a-icon-alt")
        rating_str = rating_tag.get_text(strip=True) if rating_tag else None
        try:
            rating = Decimal(rating_str.split(' ')[0].replace(',', '.')) if rating_str else Decimal('0.0')
        except Exception:
            rating = Decimal('0.0')
        count_tag = item.select_one("span.a-size-base.s-underline-text")
        rating_count_str = count_tag.get_text(strip=True) if count_tag else None
        try:
            review_count = int(rating_count_str.replace(',', '').strip()) if rating_count_str else 0
        except Exception:
            review_count = 0
        sales_tag = item.select_one("span.a-size-base.a-color-secondary")
        sales_volume = 0
        if sales_tag and "bought" in sales_tag.text:
            sales_volume_str = sales_tag.get_text(strip=True)
            try:
                number_part = ''.join(ch for ch in sales_volume_str if ch.isdigit() or ch == ',')
                sales_volume = int(number_part.replace(',', ''))
            except Exception:
                sales_volume = 0
        offer_span = item.select_one("span.a-price span.a-price-whole")
        offer_price_str = offer_span.get_text(strip=True) if offer_span else None
        try:
            offer_price = Decimal(offer_price_str.replace(',', '').replace('.', '')) if offer_price_str else None
        except Exception:
            offer_price = None
        mrp_span = item.select_one("span.a-text-price span")
        mrp_price_str = mrp_span.get_text(strip=True) if mrp_span else None
        try:
            mrp_price = Decimal(mrp_price_str.replace(',', '').replace('.', '')) if mrp_price_str else None
        except Exception:
            mrp_price = None
        price = offer_price if offer_price is not None else mrp_price
        if price is None:
            price = Decimal('0.00')
        delivery = ""
        texts = []
        delivery_container = item.select_one("div[data-cy='delivery-block'], div.udm-delivery-block")
        if delivery_container:
            texts.extend([el.get_text(strip=True) for el in delivery_container.find_all(["div", "span"]) if el.get_text(strip=True)])
        else:
            if asin:
                delivery_blocks = soup.select("div[data-cy='delivery-block'], div.udm-delivery-block")
                for block in delivery_blocks:
                    if asin in block.parent.get_text():
                        texts.extend([el.get_text(strip=True) for el in block.find_all(["div", "span"]) if el.get_text(strip=True)])
        delivery = " | ".join(texts)
        coupon_tag = item.select_one("span[data-component-type='s-coupon-component']")
        has_coupon = False
        if coupon_tag:
            unclipped = coupon_tag.select_one("span.s-coupon-unclipped")
            clipped = coupon_tag.select_one("span.s-coupon-clipped")
            if unclipped or clipped:
                has_coupon = True
        sponsored = False
        sponsored_labels = item.select("span.a-color-secondary, span.puis-label-popover-default, span.a-badge-text")
        for span in sponsored_labels:
            text = span.get_text(strip=True).lower()
            if "sponsored" in text:
                sponsored = True
                break
        search_product_type = item.get("data-search-product-type")
        if search_product_type and "sponsored" in search_product_type.lower():
            sponsored = True
        products.append({
            "asin": asin,
            "title": title,
            "url": product_url,
            "image": image_url,
            "offer_price": price,
            "rating": rating,
            "review_count": review_count,
            "sales_volume": sales_volume,
            "delivery": delivery,
            "has_coupon": has_coupon,
            "sponsored": sponsored
        })
    return products

def insert_search_results(conn, fk_profile_id, fk_mp_id, search_term, products):
    now = datetime.now()
    values = []
    for p in products:
        values.append((
            fk_profile_id,
            fk_mp_id,
            search_term,
            p["asin"],
            p["title"],
            p["offer_price"],
            p["rating"],
            p["review_count"],
            p["delivery"],
            p["has_coupon"],
            p["sales_volume"],
            now,
            p["sponsored"],
        ))
    insert_query = """
        INSERT INTO marketing_amazon_search_page_result(
            fk_profile_id, fk_mp_id, search_term, asin, title, price,
            rating, review_count, delivery_promise, has_coupon,
            sales_volume, dt_creation, is_sponsored
        )
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_query, values, page_size=100)  # Efficient batch size
    conn.commit()



def get_marketplace_and_profiles(conn):
    marketplace_dict = {}
    with conn.cursor() as cur:
        cur.execute("SELECT pk_mp_id, name FROM com_marketplace WHERE name ILIKE 'amazon%'")
        rows = cur.fetchall()
        for r in rows:
            marketplace_dict[r[1]] = r[0]
        cur.execute("""
            SELECT profile_id, fk_mp_id FROM marketing_profile_id
            WHERE fk_mp_id = 1
            # WHERE fk_mp_id = ANY(%s)   for all the marketplace use this 
            AND profile_enabled = TRUE
        """, (list(marketplace_dict.values()),))
        profiles = cur.fetchall()
    return marketplace_dict, profiles


#for all Search terms

# def get_search_terms(conn, profile_id):
#     with conn.cursor() as cur:
#         cur.execute("""
#             SELECT ms.search_term
#             FROM marketing_search_term_data ms
#             JOIN marketing_ad_group_master mag ON ms.fk_ad_group_id = mag.ad_group_id
#             JOIN marketing_profile_id mpi ON mag.fk_profile_id = mpi.profile_id
#             WHERE mpi.profile_id = %s
#         """, (profile_id,))
#         rows = cur.fetchall()
#     return [row[0] for row in rows if row[0]]


#for top 50  Search terms  based on units 
def get_search_terms(conn, profile_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ms.search_term, SUM(ms.units)
            FROM marketing_search_term_data ms
            JOIN marketing_ad_group_master mag ON ms.fk_ad_group_id = mag.ad_group_id
            JOIN marketing_profile_id mpi ON mag.fk_profile_id = mpi.profile_id
            WHERE mpi.profile_id = %s
            GROUP BY ms.search_term
            ORDER BY SUM(ms.units) DESC
            LIMIT 50
        """, (profile_id,))
        rows = cur.fetchall()
    return [(row[0], row[1]) for row in rows if row[0]]




def process_search(conn, profile_id, fk_mp_id, marketplace_name, search_terms):
    for query in search_terms:
        if not query:
            continue
        print(f"Processing marketplace: {marketplace_name}, search term: {query}")
        products = fetch_search_results(query, marketplace=marketplace_name)
        if products:
            insert_search_results(conn, profile_id, fk_mp_id, query, products)
        # Optionally add a small sleep for rate-limiting
        time.sleep(random.uniform(1, 3))

def main():
    DB_CONFIG = {
        'user': 'postgres',
        'password': '',
        'database': '',
        'host': 'localhost',
        'port': 5432,
    }
    conn = psycopg2.connect(**DB_CONFIG)
    marketplaces, profiles = get_marketplace_and_profiles(conn)
    amazon_marketplaces = {k: v for k, v in marketplaces.items() if "amazon" in k.lower()}
    # Parallel processing for all profiles
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for profile in profiles:
            profile_id = profile[0]
            fk_mp_id = profile[1]
            if fk_mp_id not in amazon_marketplaces.values():
                continue
            marketplace_name = None
            for k, v in amazon_marketplaces.items():
                if v == fk_mp_id:
                    marketplace_name = k
                    break
            if not marketplace_name:
                continue
            search_terms = get_search_terms(conn, profile_id)
            futures.append(executor.submit(process_search, conn, profile_id, fk_mp_id, marketplace_name, search_terms))
        for f in futures:
            f.result()
    conn.close()

if __name__ == "__main__":
    main()
