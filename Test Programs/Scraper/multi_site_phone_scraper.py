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
    # Core product & page
    "product_title", "brand", "category_path", "product_url", "product_id",
    "image_urls", "thumbnail_urls",

    # Pricing (MRP vs. sale)
    "mrp", "selling_price", "special_price",
    "discount_percent", "discount_amount",
    "protect_promise_fee",
    "exchange_available", "exchange_max_off", "effective_price_after_exchange",

    # Availability & delivery
    "delivery_eta_text", "delivery_eta_date", "delivery_eta_condition",
    "pincode_required",

    # Seller & trust
    "seller_name", "seller_rating", "seller_other_sellers_url",
    "gst_invoice_available", "brand_support_days", "return_policy_window",

    # Ratings & reviews (summary)
    "rating_average", "rating_count", "review_count",
    "rating_5_count", "rating_4_count", "rating_3_count", "rating_2_count", "rating_1_count",
    "rating_5_pct", "rating_4_pct", "rating_3_pct", "rating_2_pct", "rating_1_pct",
    "top_review_snippets", "top_review_authors", "top_review_dates",

    # Offers & promotions
    "offers_text", "offer_bank_axis_cc_5pct", "offer_axis_debit_5pct",
    "offer_bhim_cashback", "special_price_applied",

    # Variants (color / storage / RAM)
    "available_colors", "selected_color",
    "available_storage_options", "selected_storage",
    "available_ram_options", "selected_ram",

    # Highlights
    "highlights",

    # Warranty
    "warranty_summary", "covered_in_warranty", "domestic_warranty",

    # Full specifications (flattened as `Section | Field`)
    # General
    "General | In The Box", "General | Model Number", "General | Model Name",
    "General | Color", "General | Browse Type", "General | SIM Type",
    "General | Hybrid Sim Slot", "General | Touchscreen",
    "General | OTG Compatible", "General | Quick Charging",

    # Display Features
    "Display Features | Display Size", "Display Features | Resolution",
    "Display Features | Resolution Type", "Display Features | GPU",
    "Display Features | Display Type", "Display Features | HD Game Support",
    "Display Features | Display Colors", "Display Features | Other Display Features",

    # Os & Processor Features
    "Os & Processor Features | Operating System", "Os & Processor Features | Processor Brand",
    "Os & Processor Features | Processor Type", "Os & Processor Features | Processor Core",
    "Os & Processor Features | Primary Clock Speed", "Os & Processor Features | Secondary Clock Speed",
    "Os & Processor Features | Operating Frequency",

    # Memory & Storage Features
    "Memory & Storage Features | Internal Storage", "Memory & Storage Features | RAM",
    "Memory & Storage Features | Total Memory", "Memory & Storage Features | Expandable Storage",
    "Memory & Storage Features | Supported Memory Card Type",
    "Memory & Storage Features | Memory Card Slot Type",

    # Camera Features
    "Camera Features | Primary Camera Available", "Camera Features | Primary Camera",
    "Camera Features | Primary Camera Features", "Camera Features | Secondary Camera Available",
    "Camera Features | Secondary Camera", "Camera Features | Secondary Camera Features",
    "Camera Features | Flash", "Camera Features | HD Recording",
    "Camera Features | Full HD Recording", "Camera Features | Video Recording",
    "Camera Features | Video Recording Resolution", "Camera Features | Image Editor",
    "Camera Features | Dual Camera Lens",

    # Battery & Power Features
    "Battery & Power Features | Battery Capacity",
    "Battery & Power Features | Battery Type",
    "Battery & Power Features | Dual Battery",

    # Dimensions
    "Dimensions | Width", "Dimensions | Height", "Dimensions | Depth", "Dimensions | Weight",

    # Connectivity Features
    "Connectivity Features | Network Type", "Connectivity Features | Supported Networks",
    "Connectivity Features | Internet Connectivity", "Connectivity Features | 3G",
    "Connectivity Features | 3G Speed", "Connectivity Features | GPRS",
    "Connectivity Features | Pre-installed Browser", "Connectivity Features | Bluetooth Support",
    "Connectivity Features | Bluetooth Version", "Connectivity Features | Wi-Fi",
    "Connectivity Features | Wi-Fi Version", "Connectivity Features | Wi-Fi Hotspot",
    "Connectivity Features | NFC", "Connectivity Features | USB Connectivity",
    "Connectivity Features | USB Type/Version", "Connectivity Features | Audio Jack",
    "Connectivity Features | GPS Support", "Connectivity Features | Map Support",

    # Other Details
    "Other Details | Smartphone", "Other Details | Touchscreen Type",
    "Other Details | SIM Size", "Other Details | User Interface",
    "Other Details | Graphics PPI", "Other Details | Sensors",
    "Other Details | Upgradable Operating System", "Other Details | Series",
    "Other Details | Browser", "Other Details | Ringtones Format",

    # Multimedia Features
    "Multimedia Features | FM Radio", "Multimedia Features | FM Radio Recording",
    "Multimedia Features | Audio Formats", "Multimedia Features | Music Player",
    "Multimedia Features | Video Formats",

    # Call Features
    "Call Features | Call Wait/Hold", "Call Features | Conference Call",
    "Call Features | Hands Free", "Call Features | Video Call Support",
    "Call Features | Call Divert", "Call Features | Phone Book",
    "Call Features | Call Timer", "Call Features | Speaker Phone",
    "Call Features | Speed Dialing", "Call Features | Call Records",
    "Call Features | Logs",

    # Q&A summary
    "qa_sample_questions", "qa_sample_answers", "qa_count",

    # Frequently bought together
    "attach_reco_titles", "attach_reco_urls", "attach_reco_prices",

    # Derived insights
    "battery_wh", "price_to_ram", "price_to_storage", "display_ppi",
    "supports_5g", "nfc_available", "fast_charging_wattage",
    "os_major_version", "rear_camera_mp_primary", "front_camera_mp",
    "refresh_rate_hz", "bluetooth_version",

    # Fallbacks / generic fields from older schema (kept for compatibility)
    "title", "model", "variant", "color",
    "price", "price_currency", "discount_price", "availability",
    "ratings_count", "reviews_count", "highlights", "description",
    "bullet_points", "images", "seller", "warranty",
    "chipset", "cpu", "gpu", "ram", "storage", "expandable_storage",
    "display_size", "display_type", "display_resolution", "refresh_rate",
    "rear_camera", "rear_camera_2", "rear_camera_3", "front_camera",
    "battery_capacity", "charging", "wireless_charging",
    "os", "os_version", "network", "sim", "5g", "wifi", "bluetooth", "nfc", "gps",
    "usb", "audio_jack", "dimensions", "weight", "launch_date",
    "in_the_box", "offer_info", "return_policy", "reviews", "feedbacks",
    "url", "source_url"
]

# Seed category pages - Only Flipkart
SEEDS = {
    "flipkart": [
        # Flipkart – Mobiles category
        "https://www.flipkart.com/mobiles/pr?sid=tyy,4io",
    ]
}

# polite delays
DELAY_PAGE = (1.0, 2.0)
DELAY_PRODUCT = (1.2, 2.4)

# =========================
# HELPERS
# =========================

def ts_folder(prefix: str) -> str:
    stamp = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
    folder = os.path.join(r"E:\Cloud-AI-Native-Smartphone-Intelligence-Software\Test Programs\Scraper\output", f"{prefix}_{stamp}")
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
    if not listings:
        return None

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
        return None

    # Group by brand and save separate CSV files
    df = pd.DataFrame(rows)
    
    # Extract brand from title if brand field is not available
    if 'brand' not in df.columns and 'title' in df.columns:
        df['brand'] = df['title'].str.extract(r'^([a-zA-Z0-9]+)\s', expand=False).str.upper()
    
    if 'brand' not in df.columns:
        # If we still can't find brand, save all in one file
        path = os.path.join(folder, f"{site}_smartphones_unknown_brand.csv")
        ordered_cols = [c for c in SMARTPHONE_FIELDS if c in df.columns] + \
                      [c for c in df.columns if c not in SMARTPHONE_FIELDS]
        df[ordered_cols].to_csv(path, index=False)
        return [path]
    
    # Create a subfolder for brand CSVs
    brand_folder = os.path.join(folder, "by_brand")
    os.makedirs(brand_folder, exist_ok=True)
    
    # Group by brand and save separate CSVs
    saved_paths = []
    for brand, group in df.groupby('brand'):
        if not brand or pd.isna(brand):
            brand = 'unknown_brand'
        # Sanitize brand name for filename
        safe_brand = re.sub(r'[^a-zA-Z0-9_]', '_', str(brand).lower())
        path = os.path.join(brand_folder, f"{site}_{safe_brand}_smartphones.csv")
        
        # Keep a stable column order
        ordered_cols = [c for c in SMARTPHONE_FIELDS if c in group.columns] + \
                      [c for c in group.columns if c not in SMARTPHONE_FIELDS]
        group[ordered_cols].to_csv(path, index=False)
        saved_paths.append(path)
    
    return saved_paths

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
    # Run only Flipkart scraper
    run_site("flipkart", SEEDS["flipkart"], SELECTED_MODEL)

if __name__ == "__main__":
    main()
