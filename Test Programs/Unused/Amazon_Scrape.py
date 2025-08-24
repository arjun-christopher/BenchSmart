#!/usr/bin/env python3
# path: scripts/amazon_in_smartphones_to_csv_rawsig.py
"""
Amazon.in Smartphones → CSV (PA-API v5, no SDK)

Why this file
- Your environment can't use `paapi5-python-sdk`. This script calls the REST API directly and signs
  requests with AWS Signature v4 (pure Python + `requests`).
- Avoids HTML scraping to respect Amazon's Terms of Use.

What it does
- Searches Amazon.in for smartphones via PA-API `SearchItems`.
- Paginates using `NextToken` until page cap or no more pages.
- Extracts title/brand/model/color, price, availability, aggregate ratings, ASIN, links, image.
- Streams rows to a CSV.

Requirements
- Python 3.7+
- `pip install requests python-dotenv` (dotenv optional)
- Set credentials via env or CLI flags:
  PAAPI_ACCESS_KEY, PAAPI_SECRET_KEY, PAAPI_PARTNER_TAG

Usage
- Basic:      `python scripts/amazon_in_smartphones_to_csv_rawsig.py -o data/phones.csv`
- Custom kw:  `python scripts/amazon_in_smartphones_to_csv_rawsig.py -k "Android 5G" -p 20 -o out.csv`

Notes
- PA-API returns only aggregate review data, not full review text.
- Backoff handles 429/5xx; tune with `--sleep`.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import hmac
import json
import os
import sys
import time
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv(*_args, **_kwargs):  # type: ignore
        return False


# -------- Configuration --------
MARKETPLACES = {
    # PA-API docs map IN to eu-west-1 for signing
    "IN": {
        "host": "webservices.amazon.in",
        "region": "eu-west-1",
        "marketplace": "www.amazon.in",
    }
}

RESOURCES: List[str] = [
    "ItemInfo.Title",
    "ItemInfo.ByLineInfo",
    "ItemInfo.ManufactureInfo",
    "ItemInfo.ProductInfo",
    "ItemInfo.TechnicalInfo",
    "ItemInfo.Classifications",
    "ItemInfo.Features",
    "Images.Primary.Large",
    "DetailPageURL",
    "Offers.Listings.Price",
    "Offers.Listings.SavingBasis",
    "Offers.Listings.Availability.Message",
    "Offers.Summaries.LowestPrice",
    "Offers.Summaries.HighestPrice",
    "CustomerReviews.Count",
    "CustomerReviews.StarRating",
]

SERVICE = "ProductAdvertisingAPI"
SEARCH_TARGET = "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.SearchItems"
ENDPOINT_PATH = "/paapi5/searchitems"


# -------- AWS SigV4 (minimal) --------
def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _signature_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode("utf-8"), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    return k_signing


def build_headers(
    *,
    host: str,
    region: str,
    target: str,
    payload: str,
    access_key: str,
    secret_key: str,
) -> Dict[str, str]:
    # Why: PA-API expects these specific headers in signature.
    amz_date = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]

    canonical_headers = {
        "content-encoding": "amz-1.0",
        "content-type": "application/json; charset=utf-8",
        "host": host,
        "x-amz-date": amz_date,
        "x-amz-target": target,
    }

    signed_headers = ";".join(sorted(canonical_headers))

    canonical_headers_str = "".join(f"{k}:{canonical_headers[k]}\n" for k in sorted(canonical_headers))
    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    canonical_request = "\n".join(
        [
            "POST",
            ENDPOINT_PATH,
            "",
            canonical_headers_str,
            signed_headers,
            payload_hash,
        ]
    )

    credential_scope = f"{date_stamp}/{region}/{SERVICE}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )

    signing_key = _signature_key(secret_key, date_stamp, region, SERVICE)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    headers = {
        **canonical_headers,
        "accept": "application/json",
        "authorization": authorization,
    }
    return headers


# -------- API client (no SDK) --------
def _post(
    *, url: str, headers: Dict[str, str], payload: Dict, timeout: float
) -> Tuple[int, Dict]:
    body = json.dumps(payload, separators=(",", ":"))  # stabilize signature
    signed = build_headers(
        host=headers["host"],
        region=headers["region"],  # injected below
        target=headers["x-amz-target"],
        payload=body,
        access_key=headers["access_key"],
        secret_key=headers["secret_key"],
    )

    # Remove helpers
    signed.pop("region", None)
    signed.pop("access_key", None)
    signed.pop("secret_key", None)

    r = requests.post(url, headers=signed, data=body, timeout=timeout)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    return r.status_code, data


def paapi_search_items(
    *,
    host: str,
    region: str,
    marketplace: str,
    partner_tag: str,
    access_key: str,
    secret_key: str,
    keywords: str,
    resources: List[str],
    next_token: Optional[str] = None,
    item_count: int = 10,
    timeout: float = 20.0,
) -> Tuple[int, Dict]:
    url = f"https://{host}{ENDPOINT_PATH}"
    headers = {
        "host": host,
        "region": region,
        "x-amz-target": SEARCH_TARGET,
        "access_key": access_key,
        "secret_key": secret_key,
    }

    payload: Dict = {
        "PartnerTag": partner_tag,
        "PartnerType": "Associates",
        "Marketplace": marketplace,
        "Resources": resources,
    }

    if next_token:
        payload["NextToken"] = next_token
    else:
        payload.update({
            "Keywords": keywords,
            "SearchIndex": "Electronics",
            "ItemCount": max(1, min(10, item_count)),  # PA-API caps at 10
        })

    return _post(url=url, headers=headers, payload=payload, timeout=timeout)


# -------- Data shaping --------
def _get(obj: Dict, *path: str):
    cur = obj
    for key in path:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(key)
        elif isinstance(cur, list):
            try:
                idx = int(key)
            except Exception:
                return None
            if 0 <= idx < len(cur):
                cur = cur[idx]
            else:
                return None
        else:
            return None
    return cur


def normalize_item(item: Dict) -> Dict[str, str]:
    def s(x) -> str:
        return "" if x is None else str(x)

    asin = s(item.get("ASIN"))
    title = s(_get(item, "ItemInfo", "Title", "DisplayValue"))
    brand = s(_get(item, "ItemInfo", "ByLineInfo", "Brand", "DisplayValue"))
    model = s(_get(item, "ItemInfo", "ProductInfo", "Model", "DisplayValue"))
    color = s(_get(item, "ItemInfo", "Classifications", "Color", "DisplayValue"))

    listing0 = _get(item, "Offers", "Listings", 0) or {}
    price_amount = s(_get(listing0, "Price", "Amount"))
    price_currency = s(_get(listing0, "Price", "Currency"))
    list_price_amount = s(_get(listing0, "SavingBasis", "Amount"))
    list_price_currency = s(_get(listing0, "SavingBasis", "Currency"))
    availability = s(_get(listing0, "Availability", "Message"))

    rating = s(_get(item, "CustomerReviews", "StarRating"))
    ratings_count = s(_get(item, "CustomerReviews", "Count"))

    primary_image = s(_get(item, "Images", "Primary", "Large", "URL"))
    detail_page_url = s(item.get("DetailPageURL"))

    return {
        "asin": asin,
        "title": title,
        "brand": brand,
        "model": model,
        "color": color,
        "price_amount": price_amount,
        "price_currency": price_currency,
        "list_price_amount": list_price_amount,
        "list_price_currency": list_price_currency,
        "availability": availability,
        "rating": rating,
        "ratings_count": ratings_count,
        "primary_image": primary_image,
        "detail_page_url": detail_page_url,
    }


def write_csv(rows: Iterable[Dict[str, str]], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    writer = None
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        for row in rows:
            if writer is None:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                writer.writeheader()
            writer.writerow(row)


# -------- Orchestration --------
def fetch_rows(
    *,
    access_key: str,
    secret_key: str,
    partner_tag: str,
    marketplace: str,
    host: str,
    region: str,
    keywords: str,
    max_pages: int,
    base_sleep: float,
) -> Iterator[Dict[str, str]]:
    """Generator that handles pagination and throttling."""
    pages = 0
    next_token: Optional[str] = None
    backoff = base_sleep

    while True:
        status, data = paapi_search_items(
            host=host,
            region=region,
            marketplace=marketplace,
            partner_tag=partner_tag,
            access_key=access_key,
            secret_key=secret_key,
            keywords=keywords,
            resources=RESOURCES,
            next_token=next_token,
        )

        # Error handling & backoff
        errors = data.get("Errors") if isinstance(data, dict) else None
        if status >= 500 or status == 429 or errors:
            # Why: Respect rate limits; escalate on persistent failure.
            time.sleep(min(30.0, max(1.0, backoff)))
            backoff = min(30.0, backoff * 1.7)
            if backoff > 30.0:
                raise RuntimeError(f"PA-API throttled or error: status={status}, errors={errors}")
            continue

        backoff = base_sleep

        search_result = (data or {}).get("SearchResult") or {}
        items = search_result.get("Items") or []
        for it in items:
            yield normalize_item(it)

        pages += 1
        next_token = search_result.get("NextToken")
        if not next_token:
            break
        if max_pages and pages >= max_pages:
            break
        if base_sleep:
            time.sleep(base_sleep)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Amazon.in smartphones → CSV via PA-API v5 (no SDK)")
    p.add_argument("--keywords", "-k", default="smartphone", help="Search keywords (default: smartphone)")
    p.add_argument("--max-pages", "-p", type=int, default=10, help="Max pages via NextToken")
    p.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between pages")
    p.add_argument("--output", "-o", default="data/amazon_in_smartphones.csv", help="CSV output path")

    p.add_argument("--access-key", default=os.getenv("PAAPI_ACCESS_KEY"), help="PA-API access key")
    p.add_argument("--secret-key", default=os.getenv("PAAPI_SECRET_KEY"), help="PA-API secret key")
    p.add_argument("--partner-tag", default=os.getenv("PAAPI_PARTNER_TAG"), help="Your Associate tag")

    p.add_argument("--host", default=os.getenv("PAAPI_HOST", MARKETPLACES["IN"]["host"]), help="PA-API host")
    p.add_argument("--region", default=os.getenv("PAAPI_REGION", MARKETPLACES["IN"]["region"]), help="Signing region")
    p.add_argument(
        "--marketplace",
        default=os.getenv("PAAPI_MARKETPLACE", MARKETPLACES["IN"]["marketplace"]),
        help="Marketplace domain",
    )

    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv()  # allow .env if present
    args = parse_args(argv)

    missing = [
        k for k, v in {
            "access key": args.access_key,
            "secret key": args.secret_key,
            "partner tag": args.partner_tag,
        }.items() if not v
    ]
    if missing:
        print(f"Missing credentials: {', '.join(missing)}", file=sys.stderr)
        return 2

    rows = fetch_rows(
        access_key=args.access_key,
        secret_key=args.secret_key,
        partner_tag=args.partner_tag,
        marketplace=args.marketplace,
        host=args.host,
        region=args.region,
        keywords=args.keywords,
        max_pages=args.max_pages,
        base_sleep=args.sleep,
    )

    try:
        write_csv(rows, args.output)
    except KeyboardInterrupt:  # pragma: no cover
        print("Interrupted.")
        return 130

    print(f"Wrote CSV: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
