import requests
from bs4 import BeautifulSoup
import csv
import time

BASE_URL = "https://browser.geekbench.com/ios-benchmarks"
score_tabs = {
    "Single-Core Score": {"test": None},
    "Multi-Core Score": {"test": "multicore"},
    "Metal Score": {"test": "metal"},
}
headers = {"User-Agent": "Mozilla/5.0"}
MAX_PAGES = 3  # Adjust as needed

all_rows = []

def build_url(test, page):
    if test and page > 1:
        return f"{BASE_URL}?test={test}&page={page}"
    elif test:
        return f"{BASE_URL}?test={test}"
    elif page > 1:
        return f"{BASE_URL}?page={page}"
    else:
        return BASE_URL

for score_type, opts in score_tabs.items():
    print(f"Scraping {score_type}...")
    for page in range(1, MAX_PAGES + 1):
        url = build_url(opts["test"], page)
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            break
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.find_all("tr")
        found = 0
        for row in rows:
            name_td = row.find("td", class_="name")
            score_td = row.find("td", class_="score")
            if name_td and score_td:
                device_link_tag = name_td.find("a")
                if device_link_tag:
                    device_name = device_link_tag.text.strip()
                    device_url = "https://browser.geekbench.com" + device_link_tag["href"]
                    chipset = name_td.find("div", class_="description")
                    chipset = chipset.text.strip() if chipset else ""
                    # Record only the score for the current score_type, leave others blank
                    row_dict = {
                        'Device Name': device_name,
                        'Chipset': chipset,
                        'Device URL': device_url,
                        'Single-Core Score': '',
                        'Multi-Core Score': '',
                        'Metal Score': '',
                    }
                    row_dict[score_type] = score_td.text.strip()
                    all_rows.append(row_dict)
                    found += 1
        print(f"  Page {page}: {found} entries")
        if not found:
            break
        time.sleep(0.3)

# Write to CSV
with open(r'E:\BenchSmart\Test Programs\Benchmark Scrape\Scraper Output\ios_benchmarks.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        'Device Name', 'Chipset', 'Device URL',
        'Single-Core Score', 'Multi-Core Score', 'Metal Score'
    ])
    writer.writeheader()
    for row in all_rows:
        writer.writerow(row)

print(f"Scraping complete! Saved {len(all_rows)} rows to geekbench_ios_benchmarks_full_allrows.csv")
