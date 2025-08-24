import json
import requests

url = 'https://parseapi.back4app.com/classes/Dataset_Cell_Phones_Model_Brand?limit=500000&order=Brand,Model'
headers = {
    'X-Parse-Application-Id': 'MEqvn3N742oOXsF33z6BFeezRkW8zXXh4nIwOQUT',  # Fake app's application id
    'X-Parse-Master-Key': 'uZ1r1iHnOQr5K4WggIibVczBZSPpWfYbSRpD6INw'      # Fake app's readonly master key
}

# Fetch data
response = requests.get(url, headers=headers)
data = response.json()

# Save data to JSON file
with open(r"E:\BenchSmart\Test Programs\Smartphones Spec Datasets\Back4app_Phones_Data.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print("Data saved to back4app_Phones_Data.json")
