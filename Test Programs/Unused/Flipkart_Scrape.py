from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import csv
import random

options = Options()
options.headless = False
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0 Safari/537.36")
driver = webdriver.Chrome(options=options)

def polite_sleep():
    time.sleep(random.uniform(5, 9))

def close_popup():
    try:
        close_btn = driver.find_element(By.XPATH, "//button[contains(text(),'✕')]")
        close_btn.click()
        print("Closed login popup.")
        time.sleep(1)
    except:
        pass

driver.get("https://www.flipkart.com/search?q=smartphone")
time.sleep(4)
close_popup()
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(3)

product_links = set()
links = driver.find_elements(By.XPATH, "//a[contains(@href, '/p/')]")
for link in links:
    href = link.get_attribute('href')
    if href and "/p/" in href:
        if not href.startswith("http"):
            href = "https://www.flipkart.com" + href
        product_links.add(href)
print(f"Found {len(product_links)} product links.")

product_links = list(product_links)[:3]  # Test on 3 products first

all_data = []
for idx, url in enumerate(product_links, 1):
    print(f"\n[{idx}] Scraping: {url}")
    driver.get(url)
    polite_sleep()
    close_popup()
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    try:
        name = WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span.B_NuCI"))
        ).text
    except:
        name = ''
    try:
        price = WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div._30jeq3._16Jk6d"))
        ).text
    except:
        price = ''
    try:
        rating = driver.find_element(By.CSS_SELECTOR, "div._3LWZlK").text
    except:
        rating = ''
    specs = {}
    try:
        for row in driver.find_elements(By.CSS_SELECTOR, "div._1UhVsV ._21Ahn- tr"):
            try:
                key = row.find_element(By.CSS_SELECTOR, "td._1hKmbr").text
                val = row.find_element(By.CSS_SELECTOR, "td.URwL2w").text
                specs[key] = val
            except:
                continue
    except:
        pass
    data = {
        'Product URL': url,
        'Name': name,
        'Price': price,
        'Rating': rating,
        'Specs': specs
    }
    print(data)
    all_data.append(data)
    polite_sleep()

with open('flipkart_mobiles_sample_fixed.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Product URL', 'Name', 'Price', 'Rating', 'Specs'])
    writer.writeheader()
    for item in all_data:
        item['Specs'] = str(item['Specs'])
        writer.writerow(item)

driver.quit()
print("\n✅ Done! Data saved to flipkart_mobiles_sample_fixed.csv")
