import os
# Set config dir to the folder containing kaggle.json
os.environ['KAGGLE_CONFIG_DIR'] = r"E:\Cloud-AI-Native-Smartphone-Intelligence-Software\Test Programs"  # Change this to your actual user name
from kaggle.api.kaggle_api_extended import KaggleApi

datasets = [
    "zynicide/wine-reviews"
]

download_dir = "kaggle_datasets"

api = KaggleApi()
api.authenticate()

if not os.path.exists(download_dir):
    os.makedirs(download_dir)

for dataset in datasets:
    print(f"Downloading {dataset} ...")
    api.dataset_download_files(dataset, path=download_dir, unzip=True)
    print(f"Downloaded: {dataset}")

print("All downloads complete.")
