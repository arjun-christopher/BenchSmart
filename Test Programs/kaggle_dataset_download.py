import os
# Set config dir to the folder containing kaggle.json
os.environ['KAGGLE_CONFIG_DIR'] = r"E:\BenchSmart\Test Programs"  # Change this to your actual user name
from kaggle.api.kaggle_api_extended import KaggleApi

datasets = [
    "artempozdniakov/ukrainian-market-mobile-phones-data",   #ok
    # "abhijitdahatonde/real-world-smartphones-dataset",    #ok but model must be cleaned
    "informrohit1/smartphones-dataset",   #ok
    "chaudharisanika/smartphones-dataset",    #ok
    "dnyaneshyeole/flipkart-smartphones-dataset",    #ok
    "shrutiambekar/smartphone-specifications-and-prices-in-india",   #ok
    "pranav941/evolution-of-smartphones",    #ok
    "ramjasmaurya/5g-smartphones-available-in-india",    #ok
    "rohsanyadav/smartphones-dataset",  #ok
    # "ankitkalauni/amazon-top-rated-smartphones-accessories-2021",   #no
    "pantanjali/iphones-on-ecommerce-website-amazon",
]

download_dir = r"E:\BenchSmart\Test Programs\kaggle_datasets"

api = KaggleApi()
api.authenticate()

if not os.path.exists(download_dir):
    os.makedirs(download_dir)

for dataset in datasets:
    print(f"Downloading {dataset} ...")
    api.dataset_download_files(dataset, path=download_dir, unzip=True)
    print(f"Downloaded: {dataset}")

print("All downloads complete.")
