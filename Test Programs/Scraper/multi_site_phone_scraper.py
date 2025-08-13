# multi_site_phone_scraper.py

import os
import re
import time
import json
import random
from urllib.parse import urljoin, urlparse
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from scraper import (
    fetch_html_selenium,
    html_to_markdown_with_readability,
    save_raw_data,
    create_dynamic_listing_model,
    create_listings_container_model,
    format_data,
    calculate_price,
    scrape_url,  # uses your JSON/XLSX saver
)
from pagination_detector import detect_pagination_elements, PaginationData
from assets import PRICING

# =========================
# CONFIG
# =========================

# Choose one of your models defined in assets.PRICING
SELECTED_MODEL = "gemini-1.5-flash"

# Fields we’ll try to extract from each product page (works with your LLM schema builder)
SMARTPHONE_FIELDS = [
    "title", "brand", "model", "variant", "color",
    "price", "price_currency", "discount_price", "availability",
    "rating", "ratings_count", "reviews_count",
    "highlights", "description", "bullet_points",
    "images", "seller", "warranty",
    "chipset", "cpu", "gpu",
    "ram", "storage", "expandable_storage",
    "display_size", "display_type", "display_resolution", "refresh_rate",
    "rear_camera", "rear_camera_2", "rear_camera_3", "front_camera",
    "battery_capacity", "charging", "wireless_charging",
    "os", "os_version",
    "network", "sim", "5g", "wifi", "bluetooth", "nfc", "gps",
    "usb", "audio_jack",
    "dimensions", "weight",
    "launch_date",
    "in_the_box",
    "offer_info",
    "return_policy", "reviews", "feedbacks", "rating"
    "url"
]

# Seed category pages (override if you want to scope narrower)
SEEDS = {
    "amazon": [
        # Amazon India – Mobiles category
        "https://www.amazon.in/s?i=electronics&rh=n%3A1389401031",
    ],
    "flipkart": [
        # Flipkart – Mobiles category
        "https://www.flipkart.com/mobiles/pr?sid=tyy,4io",
    ],
    "gsmarena": [
        # GSMArena – All makers (we’ll later dive brand/catalog pages via pagination and product link pickup)
        "https://www.gsmarena.com/",
        "https://www.gsmarena.com/makers.php3",
    ],
}

# polite delays
DELAY_PAGE = (1.0, 2.0)
DELAY_PRODUCT = (1.2, 2.4)

# =========================
# HELPERS
# =========================

def ts_folder(prefix: str) -> str:
    stamp = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
    folder = os.path.join(r"E:\Cloud-AI-Native-Smartphone-Intelligence-Software\Test Programs\Scraper", f"{prefix}_{stamp}")
    os.makedirs(folder, exist_ok=True)
    return folder

def absolute_urls(base_url: str, urls):
    out = []
    for u in urls:
        if not u: 
            continue
        out.append(urljoin(base_url, u))
    return list(dict.fromkeys(out))

def regex_fallback_pagination(base_url: str, html: str):
    """
    Fallback pagination collector if LLM pagination returns empty.
    Looks for typical page param patterns like page=2, p=2, ?page=3, /page/2 etc.
    """
    candidates = set()

    # hrefs
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(k in href for k in ["page=", "p=", "/page/"]):
            candidates.add(urljoin(base_url, href))

    # Deduplicate and keep within same domain
    domain = urlparse(base_url).netloc
    filtered = [u for u in candidates if urlparse(u).netloc == domain]
    # Attempt to sort by page number if present
    def page_num(u):
        m = re.search(r"(?:page=|p=|/page/)(\d+)", u)
        return int(m.group(1)) if m else 999999
    filtered.sort(key=page_num)
    # Always include the base page first
    pages = [base_url] + [u for u in filtered if u != base_url]
    # limit to something sane to avoid infinite scraping
    return pages[:50]

def get_all_pages(start_url: str, model: str):
    """
    Use your detect_pagination_elements. If empty, fallback to regex scanning.
    Always returns a list of page URLs, starting with the start_url.
    """
    html = fetch_html_selenium(start_url)
    markdown = html_to_markdown_with_readability(html)

    # Try LLM pagination detector
    try:
        pagination_data, token_counts, price = detect_pagination_elements(
            start_url, "", model, markdown
        )
        if isinstance(pagination_data, PaginationData):
            pages = pagination_data.page_urls or []
        elif isinstance(pagination_data, dict):
            pages = pagination_data.get("page_urls", []) or []
        else:
            pages = []
    except Exception:
        pages = []

    if not pages:
        # Fallback
        pages = regex_fallback_pagination(start_url, html)

    # Make absolute, unique and bounded
    pages = absolute_urls(start_url, pages)
    if start_url not in pages:
        pages.insert(0, start_url)
    # Cap to avoid runaway
    return pages[:100]

def extract_product_links(site: str, page_url: str, html: str):
    """
    Domain-specific product link extraction from a listing/catalog/search page.
    Returns absolute product URLs.
    """
    soup = BeautifulSoup(html, "html.parser")
    links = []

    if site == "amazon":
        # Amazon product pages have /dp/<ASIN> or /gp/product/
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if ("/dp/" in href) or ("/gp/product/" in href):
                links.append(href)

    elif site == "flipkart":
        # Flipkart product detail typically .../p/itm...
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "/p/" in href and "flipkart.com" in urljoin(page_url, href):
                links.append(href)

    elif site == "gsmarena":
        # Product detail pages like ...-xxxxx.php
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if href.endswith(".php") and "-reviews-" not in href and "news-" not in href:
                # Heuristic: phone pages often have hyphen + numeric id
                if re.search(r"-\d+\.php$", href):
                    links.append(href)

    # Normalize + dedupe + same domain
    abs_links = absolute_urls(page_url, links)
    domain = urlparse(page_url).netloc
    abs_links = [u for u in abs_links if urlparse(u).netloc == domain]
    return list(dict.fromkeys(abs_links))

def save_listings_csv(listings, folder, site):
    path = os.path.join(folder, f"{site}_smartphones.csv")
    if not listings:
        # still create an empty file with headers to signal run completed
        pd.DataFrame([], columns=SMARTPHONE_FIELDS + ["source_url"]).to_csv(path, index=False)
        return path

    # Flatten each item to dict (handles Pydantic or dict or string JSON)
    rows = []
    for item in listings:
        if item is None:
            continue
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except json.JSONDecodeError:
                continue
        if hasattr(item, "listings"):
            # Pydantic container from your pipeline
            for rec in item.listings:
                rows.append(rec if isinstance(rec, dict) else rec.dict())
        elif isinstance(item, dict) and "listings" in item:
            for rec in item["listings"]:
                rows.append(rec)
        elif isinstance(item, dict):
            rows.append(item)

    if not rows:
        pd.DataFrame([], columns=SMARTPHONE_FIELDS + ["source_url"]).to_csv(path, index=False)
        return path

    df = pd.DataFrame(rows)
    # keep a stable column order with our fields first
    ordered_cols = [c for c in SMARTPHONE_FIELDS if c in df.columns] + \
                   [c for c in df.columns if c not in SMARTPHONE_FIELDS]
    df = df[ordered_cols]
    df.to_csv(path, index=False)
    return path

# =========================
# MAIN per-site runner
# =========================

def run_site(site: str, seeds: list, model: str):
    print(f"\n=== {site.upper()} ===")
    out_folder = ts_folder(site)

    all_product_results = []
    total_in_tokens = 0
    total_out_tokens = 0
    total_cost_usd = 0.0

    # Prepare schema models once (faster)
    DynamicListingModel = create_dynamic_listing_model(SMARTPHONE_FIELDS)
    DynamicListingsContainer = create_listings_container_model(DynamicListingModel)

    for seed in seeds:
        pages = get_all_pages(seed, model)
        print(f"[{site}] Found {len(pages)} pages from seed: {seed}")

        for idx, page_url in enumerate(pages, 1):
            try:
                html = fetch_html_selenium(page_url)
                markdown = html_to_markdown_with_readability(html)
                save_raw_data(markdown, out_folder, f"{site}_page_{idx}.md")

                product_links = extract_product_links(site, page_url, html)
                print(f"[{site}] Page {idx}: {len(product_links)} product links")

                # Visit each product and parse with your LLM pipeline
                for p_i, product_url in enumerate(product_links, 1):
                    try:
                        raw_html = fetch_html_selenium(product_url)
                        md = html_to_markdown_with_readability(raw_html)

                        # Using your format_data step with our schema
                        formatted_data, token_counts = format_data(
                            md, DynamicListingsContainer, DynamicListingModel, model
                        )

                        # Annotate source URL inside items if possible
                        # (If Pydantic, we’ll add later during CSV save)
                        # Save JSON/XLSX via your existing helper for traceability
                        # (Write per-product files for debugging)
                        try:
                            _ = scrape_url(
                                product_url, SMARTPHONE_FIELDS, model, out_folder,
                                file_number=int(time.time()), markdown=md
                            )
                        except Exception:
                            # non-fatal; we already have formatted_data
                            pass

                        # Tally cost
                        in_tok, out_tok, cost = calculate_price(token_counts, model)
                        total_in_tokens += in_tok
                        total_out_tokens += out_tok
                        total_cost_usd += cost

                        # Attach URL into each record if not present
                        if isinstance(formatted_data, str):
                            try:
                                obj = json.loads(formatted_data)
                            except json.JSONDecodeError:
                                obj = {"listings": []}
                        elif hasattr(formatted_data, "dict"):
                            obj = formatted_data.dict()
                        else:
                            obj = formatted_data

                        # inject url into records
                        if isinstance(obj, dict) and "listings" in obj and isinstance(obj["listings"], list):
                            for rec in obj["listings"]:
                                if isinstance(rec, dict) and "url" in SMARTPHONE_FIELDS and not rec.get("url"):
                                    rec["url"] = product_url
                                if isinstance(rec, dict):
                                    rec.setdefault("source_url", product_url)

                        all_product_results.append(obj)

                        time.sleep(random.uniform(*DELAY_PRODUCT))
                    except Exception as e:
                        print(f"[{site}] Product error: {product_url} -> {e}")
                        continue

                time.sleep(random.uniform(*DELAY_PAGE))
            except Exception as e:
                print(f"[{site}] Page error: {page_url} -> {e}")
                continue

    csv_path = save_listings_csv(all_product_results, out_folder, site)

    print(f"[{site}] Done. CSV: {csv_path}")
    print(f"[{site}] Tokens in: {total_in_tokens} | out: {total_out_tokens} | est. cost: ${total_cost_usd:.4f}")

def main():
    # You can swap seeds with your own narrower lists if needed
    run_site("amazon", SEEDS["amazon"], SELECTED_MODEL)
    run_site("flipkart", SEEDS["flipkart"], SELECTED_MODEL)
    run_site("gsmarena", SEEDS["gsmarena"], SELECTED_MODEL)

if __name__ == "__main__":
    main()
