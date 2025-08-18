import os
import re
import json
import uuid
import math
import glob
import hashlib
from pathlib import Path
from collections import defaultdict
from difflib import SequenceMatcher

import pandas as pd

# === CONFIG ===
INPUT_DIR = r"E:\Cloud-AI-Native-Smartphone-Intelligence-Software\Test Programs\kaggle_datasets"
OUTPUT_DIR = r"E:\Cloud-AI-Native-Smartphone-Intelligence-Software\Test Programs\kaggle_merged_json"  # one JSON per brand
LOG_FILE = r"E:\Cloud-AI-Native-Smartphone-Intelligence-Software\Test Programs\kaggle_merge_log.txt"

# Create output dir
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---- Utility: logging (simple) ----
def log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")
    print(msg)

# ---- Column name normalization helpers ----
def clean_colname(name: str) -> str:
    """
    Normalize column names to a simple comparable token stream.
    """
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def token_set(name: str) -> set:
    return set(clean_colname(name).split())

def sim_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, clean_colname(a), clean_colname(b)).ratio()

# Canonical schema seeds (extend freely).
CANONICAL_KEYS = {
    # identity
    "brand": {"brand", "brand_name", "manufacturer", "company"},
    "model": {"model", "model_name", "phone_model", "device", "device_name", "product_name", "name", "title"},
    "variant": {"variant", "version", "trim"},

    # price/ratings
    "price": {"price", "price_inr", "price_rupee", "price_rs", "current_price", "discount_price"},
    "mrp": {"mrp", "list_price", "original_price"},
    "rating": {"rating", "ratings", "avg_rating"},
    "review_count": {"review_count", "num_reviews", "ratings_count", "reviews_count"},

    # performance
    "chipset": {"chipset", "soc", "processor", "processor_model"},
    "cpu": {"cpu", "processor_name", "cpu_model"},
    "gpu": {"gpu", "graphics"},
    "ram": {"ram", "memory_ram", "ram_gb", "ram_size"},
    "storage": {"storage", "rom", "internal_storage", "memory_storage", "storage_gb"},

    # display
    "display_size": {"display", "display_size", "screen_size", "screen"},
    "display_type": {"display_type", "panel_type"},
    "refresh_rate": {"refresh_rate", "screen_refresh_rate"},
    "resolution": {"resolution", "screen_resolution"},

    # camera
    "camera_main": {"rear_camera", "main_camera", "primary_camera", "camera", "rear_cam"},
    "camera_front": {"front_camera", "selfie_camera", "secondary_camera"},

    # battery/charging
    "battery_capacity": {"battery", "battery_capacity", "battery_mah"},
    "charging": {"charging", "fast_charging", "charging_speed", "charger_watt", "charging_watt"},

    # os/network
    "os": {"os", "operating_system", "android_version", "ios_version", "software"},
    "network": {"network", "network_technology", "connectivity"},
    "sim": {"sim", "sim_type", "sim_slots"},
    "nfc": {"nfc"},
    "wifi": {"wifi", "wi fi"},
    "bluetooth": {"bluetooth"},

    # build
    "dimensions": {"dimensions", "size"},
    "weight": {"weight"},
    "colors": {"colors", "colour", "color"},

    # misc
    "release_date": {"release_date", "launch_date", "announced"},
    "image_url": {"image_url", "img_url", "image", "thumbnail"},
    "usb": {"usb", "usb_type", "usb_port"},
    "sensors": {"sensors", "sensor_list"},
    "warranty": {"warranty"},
    "seller": {"seller", "seller_name", "store"},
    "url": {"url", "product_url", "link"},
}

# Flatten alias -> canonical map
ALIAS_TO_CANON = {}
for canon, aliases in CANONICAL_KEYS.items():
    for a in aliases:
        ALIAS_TO_CANON[clean_colname(a)] = canon

# "Always try these first" when hunting brand/model
PRIORITY_BRAND_CANDS = ["brand", "brand_name", "manufacturer", "company"]
PRIORITY_MODEL_CANDS = ["model", "model_name", "phone_model", "device", "device_name", "product_name", "name", "title"]

def best_canonical(colname: str) -> str | None:
    """
    Map an arbitrary column name to a canonical key using:
      1) direct alias hit
      2) fuzzy match against known aliases
      3) fuzzy match against canonical keys themselves
    """
    if not colname:
        return None
    cleaned = clean_colname(colname)

    # 1) Direct alias
    if cleaned in ALIAS_TO_CANON:
        return ALIAS_TO_CANON[cleaned]

    # 2) Fuzzy over all known aliases
    best, best_score = None, 0.0
    for alias, canon in ALIAS_TO_CANON.items():
        score = sim_ratio(cleaned, alias)
        # token overlap bonus
        tok_overlap = len(token_set(cleaned) & token_set(alias))
        score += 0.05 * tok_overlap
        if score > best_score:
            best, best_score = canon, score

    # Threshold: reasonably strict to avoid bad merges
    if best and best_score >= 0.85:
        return best

    # 3) Fuzzy over canonical keys themselves (fallback)
    for canon in CANONICAL_KEYS.keys():
        score = sim_ratio(cleaned, canon)
        if score > best_score:
            best, best_score = canon, score

    if best and best_score >= 0.90:
        return best

    # No confident match
    return None

# ---- Brand/Model extraction ----
def find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = list(df.columns)
    # Try exact/canonical-first
    for c in candidates:
        for col in cols:
            if clean_colname(col) == clean_colname(c):
                return col
    # Try fuzzy
    best, best_score = None, 0.0
    for col in cols:
        score = 0.0
        for c in candidates:
            score = max(score, sim_ratio(col, c))
        if score > best_score:
            best, best_score = col, score
    return best if best_score >= 0.80 else None

def guess_brand_model(df: pd.DataFrame) -> tuple[str | None, str | None]:
    brand_col = find_column(df, PRIORITY_BRAND_CANDS)
    model_col = find_column(df, PRIORITY_MODEL_CANDS)
    return brand_col, model_col

def split_brand_from_model(model_val: str) -> tuple[str | None, str]:
    """
    Heuristic: if model starts with a known brand token, split it.
    E.g., 'Samsung Galaxy S21' -> ('Samsung', 'Galaxy S21')
    """
    if not model_val:
        return None, model_val
    tokens = model_val.strip().split()
    if len(tokens) <= 1:
        return None, model_val
    first = tokens[0]
    # Brand candidate if capitalized and short-ish
    if first and (first[0].isupper() or first.isupper()) and 2 <= len(first) <= 12:
        return first, " ".join(tokens[1:]).strip()
    return None, model_val

# ---- Unique ID generation ----
def stable_phone_id(brand: str, model: str) -> str:
    """
    Deterministic UUID5 over normalized 'brand|model'.
    """
    base = f"{brand.strip().lower()}|{model.strip().lower()}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))

# ---- Safe JSON IO per brand ----
def brand_file_path(brand: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9_\-]+", "_", brand.strip())
    return Path(OUTPUT_DIR) / f"{safe}.json"

def load_brand_db(brand: str) -> dict:
    path = brand_file_path(brand)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"brand": brand, "phones": {}}  # phones: id -> phone_obj

def save_brand_db(brand: str, data: dict):
    path = brand_file_path(brand)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ---- Merge rule: keep first non-empty ----
def is_meaningful(val) -> bool:
    if val is None:
        return False
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return False
    s = str(val).strip()
    if s == "" or s.lower() in {"na", "n/a", "none", "null", "nan"}:
        return False
    return True

def maybe_cast_number(s: str):
    """
    Try to cast numeric-looking strings to int/float; else return original.
    """
    if not isinstance(s, str):
        return s
    st = s.strip().replace(",", "")
    # simple battery like '5000 mAh' -> '5000'
    m = re.match(r"^(\d+(?:\.\d+)?)", st)
    if m:
        num = m.group(1)
        try:
            if "." in num:
                return float(num)
            return int(num)
        except:
            return s
    return s

def merge_attributes(existing: dict, incoming: dict):
    """
    Merge incoming attributes into existing attributes following:
      - If existing has a value for a key, keep it (skip overwrite).
      - For ram, storage, colors, price: store as list if different values exist.
      - If new key, add it.
      - Normalize / cast some numeric-like values.
    """
    # Fields that should be stored as lists when different values exist
    list_fields = {'ram', 'storage', 'colors', 'price', 'color'}
    
    for k, v in incoming.items():
        if not is_meaningful(v):
            continue
            
        normalized_v = maybe_cast_number(v)
        
        if k not in existing or not is_meaningful(existing.get(k)):
            # New key or existing key has no meaningful value
            existing[k] = normalized_v
        elif k in list_fields:
            # Special handling for list fields
            existing_val = existing[k]
            
            # Convert existing value to list if it's not already
            if not isinstance(existing_val, list):
                existing_val = [existing_val]
            
            # Check if the new value is different from all existing values
            is_different = True
            for existing_item in existing_val:
                # Compare normalized values
                if str(normalized_v).lower().strip() == str(existing_item).lower().strip():
                    is_different = False
                    break
            
            # Add to list if different
            if is_different:
                existing_val.append(normalized_v)
                existing[k] = existing_val
            else:
                existing[k] = existing_val
        # For non-list fields, keep existing value (don't overwrite)

# ---- Row -> normalized attribute dict ----
def row_to_attributes(row: pd.Series, brand_col: str, model_col: str) -> tuple[str | None, str | None, dict]:
    """
    Returns (brand, model, attributes) for a row.
    """
    brand_val = str(row.get(brand_col, "")).strip() if brand_col else ""
    model_val = str(row.get(model_col, "")).strip() if model_col else ""

    # If brand missing, try to split from model
    if not is_meaningful(brand_val) and is_meaningful(model_val):
        guess_b, new_model = split_brand_from_model(model_val)
        if guess_b:
            brand_val = guess_b
            model_val = new_model

    if not is_meaningful(brand_val) or not is_meaningful(model_val):
        return None, None, {}

    attributes = {}
    for raw_col, val in row.items():
        if raw_col == brand_col or raw_col == model_col:
            continue
        canon = best_canonical(str(raw_col))
        if canon is None:
            # keep a safe fallback name (normalized but prefixed to avoid collision)
            canon = f"attr_{clean_colname(str(raw_col))}"
        attributes[canon] = val
    return brand_val, model_val, attributes

# ---- Main processing ----
def process_csv_file(csv_path: str):
    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding="latin-1")
    except Exception as e:
        log(f"[SKIP] {csv_path} -> read error: {e}")
        return

    if df.empty:
        log(f"[SKIP] {csv_path} -> empty file")
        return

    brand_col, model_col = guess_brand_model(df)
    if brand_col is None and model_col is None:
        log(f"[WARN] {csv_path} -> no obvious brand/model columns; attempting generic heuristics.")
        # As a last resort, try to find a single 'name' column as model
        # Brand may be parsed from model by split_brand_from_model
        possible = [c for c in df.columns if clean_colname(c) in {"name", "title", "product_name"}]
        model_col = possible[0] if possible else None

    if model_col is None:
        log(f"[SKIP] {csv_path} -> couldn't find model column.")
        return

    # Process rows
    n_rows = 0
    n_merged = 0
    for _, row in df.iterrows():
        brand, model, attrs = row_to_attributes(row, brand_col, model_col)
        if not (brand and model):
            continue

        brand = str(brand).strip()
        model = str(model).strip()

        db = load_brand_db(brand)
        phones = db["phones"]

        phone_id = stable_phone_id(brand, model)
        # Initialize if new
        if phone_id not in phones:
            phones[phone_id] = {
                "id": phone_id,
                "brand": brand,
                "model": model,
                "attributes": {}
            }

        # Merge attributes: keep first non-empty value per key
        merge_attributes(phones[phone_id]["attributes"], attrs)

        # Persist occasionally to avoid big memory for huge datasets
        db["phones"] = phones
        save_brand_db(brand, db)

        n_rows += 1
        n_merged += 1

    log(f"[OK] {csv_path} -> processed {n_rows} rows, merged {n_merged} entries.")

def main():
    # fresh log
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    log("=== Start merging Kaggle smartphone CSVs ===")

    # Find all CSVs recursively
    csv_files = glob.glob(os.path.join(INPUT_DIR, "**", "*.csv"), recursive=True)
    if not csv_files:
        log(f"[INFO] No CSV files found under: {INPUT_DIR}")
        return

    for csv_path in csv_files:
        log(f"--> {csv_path}")
        process_csv_file(csv_path)

    log("=== Done. One JSON per brand has been written to OUTPUT_DIR ===")
    log(f"OUTPUT_DIR = {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
