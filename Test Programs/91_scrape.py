import requests
from bs4 import BeautifulSoup
import csv
import time

BASE_URL = "https://www.91mobiles.com"
MOBILE_PHONES_URL = f"{BASE_URL}/mobile-phones"
HEADERS = {'User-Agent': 'Mozilla/5.0'}
BRANDS_TO_FETCH = 3
MODELS_PER_BRAND = 2

def get_brand_links():
    print(f"Fetching brands from: {MOBILE_PHONES_URL}")
    response = requests.get(MOBILE_PHONES_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    brands = []
    # Brand filter block
    brand_block = soup.find("div", class_="filter_box brandFilter")
    if not brand_block:
        print("  Could not find brand filter block! Check site structure.")
        return brands
    # All brand <a> links
    for a in brand_block.select("a")[:BRANDS_TO_FETCH]:
        brand_name = a.get_text(strip=True)
        href = a.get('href')
        if href and not href.startswith("http"):
            brand_url = BASE_URL + href
            brands.append((brand_name, brand_url))
    print(f"Found {len(brands)} brands: {[b[0] for b in brands]}")
    return brands


def get_model_links(brand_url):
    print(f"  Fetching models from: {brand_url}")
    response = requests.get(brand_url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    model_links = []
    for card in soup.select("div.finder_snipet_wrap")[:MODELS_PER_BRAND]:
        a = card.find("a", class_="name_ga_event")
        if a and a.get("href"):
            href = a.get("href")
            if not href.startswith("http"):
                href = BASE_URL + href
            model_links.append(href)
    print(f"    Found {len(model_links)} models")
    return model_links

def scrape_model_page(model_url):
    print(f"    Scraping model: {model_url}")
    response = requests.get(model_url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    name = soup.find("h1", {"class": "heading"})
    name = name.text.strip() if name else ''

    price_tag = soup.select_one("span#bestprice")
    price = price_tag.text.strip() if price_tag else ''

    highlights = []
    for li in soup.select("ul.highlights_list li"):
        highlights.append(li.text.strip())
    highlights_str = " | ".join(highlights)

    specs = {}
    spec_table = soup.find("table", {"class": "specs"})
    if spec_table:
        for row in spec_table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                specs[th.text.strip()] = td.text.strip()
    else:
        for row in soup.select("div.spec_box"):
            key = row.find("div", class_="lft")
            val = row.find("div", class_="rgt")
            if key and val:
                specs[key.text.strip()] = val.text.strip()

    # Some common fields
    camera = specs.get("Camera", '')
    battery = specs.get("Battery", '')
    display = specs.get("Display", '')
    processor = specs.get("Processor", '')
    ram = specs.get("RAM", '')

    return {
        "Name": name,
        "URL": model_url,
        "Price": price,
        "Highlights": highlights_str,
        "Display": display,
        "Processor": processor,
        "RAM": ram,
        "Camera": camera,
        "Battery": battery,
    }

def main():
    all_rows = []
    brands = get_brand_links()
    for brand_name, brand_url in brands:
        model_links = get_model_links(brand_url)
        for model_url in model_links:
            try:
                row = scrape_model_page(model_url)
                row["Brand"] = brand_name
                all_rows.append(row)
            except Exception as e:
                print(f"      Failed to scrape {model_url}: {e}")
            time.sleep(1)  # polite delay
    # Write to CSV
    fieldnames = ["Brand", "Name", "URL", "Price", "Highlights", "Display", "Processor", "RAM", "Camera", "Battery"]
    with open("91mobiles_scrape_full.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)
    print(f"\nScraping complete! Saved {len(all_rows)} rows to 91mobiles_scrape_full.csv")

if __name__ == "__main__":
    main()
