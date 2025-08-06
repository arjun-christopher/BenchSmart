import requests
from bs4 import BeautifulSoup
import csv
import random
import time

BASE_URL = "https://www.gsmarena.com/"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    # ...add more if you like
]

EXTRA_HEADERS = {
    "Referer": "https://www.google.com/",
    "Accept-Language": "en-US,en;q=0.9"
}

def polite_get(url, session, tries=3):
    for i in range(tries):
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            **EXTRA_HEADERS
        }
        try:
            resp = session.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp.text
            elif resp.status_code == 429:
                print(" [!] Rate limited. Waiting 5 minutes...")
                time.sleep(300)
            else:
                print(f" [!] HTTP {resp.status_code} for {url}")
        except Exception as e:
            print(f" [!] Request error: {e}")
        time.sleep(random.uniform(20, 40))  # Much slower!
    return None

# Use a session everywhere in your script:
session = requests.Session()
html = polite_get("https://www.gsmarena.com/makers.php3", session)
# ...and so on

def get_brand_links(limit=2):
    html = polite_get(BASE_URL + "makers.php3")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for table in soup.find_all("table"):
        for a in table.find_all("a"):
            if a["href"].endswith(".php") and "-phones-" in a["href"]:
                links.append((a.text.strip(), BASE_URL + a["href"]))
            if len(links) == limit:
                return links
    return links

def get_phone_links(brand_url, limit=2):
    html = polite_get(brand_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    makers_div = soup.find("div", class_="makers")
    links = []
    if makers_div:
        for a in makers_div.find_all("a"):
            links.append(BASE_URL + a["href"])
            if len(links) == limit:
                return links
    return links

def get_phone_specs(phone_url):
    html = polite_get(phone_url)
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    specs = {}
    # Basic info
    h1 = soup.find("h1")
    specs["Device Name"] = h1.text.strip() if h1 else ""
    bread = soup.find("div", {"class": "breadcrumb"})
    if bread and len(bread.find_all("a")) > 1:
        specs["Brand"] = bread.find_all("a")[1].text.strip()
    specs["Device URL"] = phone_url
    # All spec fields
    specs_list = soup.find("div", id="specs-list")
    if specs_list:
        for table in specs_list.find_all("table"):
            category = table.find("th")
            cat_prefix = (category.text.strip() + ": ") if category else ""
            for tr in table.find_all("tr"):
                th = tr.find("td", class_="ttl")
                td = tr.find("td", class_="nfo")
                if th and td:
                    key = cat_prefix + th.text.strip()
                    val = td.text.strip().replace('\n', '; ')
                    specs[key] = val
    return specs

# --- MAIN SCRAPE ---
all_rows = []
all_keys = set()

brand_links = get_brand_links(limit=2)
print("Brands to scrape:", [b[0] for b in brand_links])

for brand, brand_url in brand_links:
    print(f"\nScraping brand: {brand}")
    phone_links = get_phone_links(brand_url, limit=2)
    print(f"  Found {len(phone_links)} phones.")
    for phone_url in phone_links:
        print(f"    Scraping: {phone_url}")
        specs = get_phone_specs(phone_url)
        if specs:
            all_rows.append(specs)
            all_keys.update(specs.keys())
        else:
            print("      [!] Could not fetch specs for", phone_url)
        # Polite delay
        time.sleep(random.uniform(8, 15))

# --- SAVE TO CSV ---
if all_rows:
    all_keys = sorted(all_keys)
    with open('gsmarena_2brands_2phones_polite.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=all_keys)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)
    print(f"\n✅ Done! Saved {len(all_rows)} phones with {len(all_keys)} fields to gsmarena_2brands_2phones_polite.csv")
else:
    print("\n[!] No phones scraped. Check output or try again later.")

