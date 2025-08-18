import os
# Set config dir to the folder containing kaggle.json
os.environ['KAGGLE_CONFIG_DIR'] = r"E:\Cloud-AI-Native-Smartphone-Intelligence-Software\Test Programs"  # Change this to your actual user name
from kaggle.api.kaggle_api_extended import KaggleApi

datasets = [
    "artempozdniakov/ukrainian-market-mobile-phones-data",
    "abhijitdahatonde/real-world-smartphones-dataset",
    "informrohit1/smartphones-dataset",
    "chaudharisanika/smartphones-dataset",
    "dnyaneshyeole/flipkart-smartphones-dataset",
    "shrutiambekar/smartphone-specifications-and-prices-in-india",
    "pranav941/evolution-of-smartphones",
    "ramjasmaurya/5g-smartphones-available-in-india",
    "rohsanyadav/smartphones-dataset",
    # "ankitkalauni/amazon-top-rated-smartphones-accessories-2021",
    "pantanjali/iphones-on-ecommerce-website-amazon",
]

download_dir = r"E:\Cloud-AI-Native-Smartphone-Intelligence-Software\Test Programs\kaggle_datasets"

api = KaggleApi()
api.authenticate()

if not os.path.exists(download_dir):
    os.makedirs(download_dir)

for dataset in datasets:
    print(f"Downloading {dataset} ...")
    api.dataset_download_files(dataset, path=download_dir, unzip=True)
    print(f"Downloaded: {dataset}")

print("All downloads complete.")
