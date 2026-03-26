#!/usr/bin/env python3
"""
fetch_prices.py
Fetches fresh-product prices and active promotions for every store in stores.csv.
Saves a unified JSON to data/prices.json.

Feed types supported:
  publishedprices  — Cerberus-based portal (e.g. url.publishedprices.co.il)
  shufersal        — Azure-blob portal    (prices.shufersal.co.il)  [planned]
"""

import csv
import gzip
import json
import os
import re
import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

import requests
import urllib3

urllib3.disable_warnings()

# Force UTF-8 stdout/stderr on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
STORES_CSV  = Path("stores.csv")
DATA_DIR    = Path("data")
OUTPUT_FILE = DATA_DIR / "prices.json"
FRESH_RE    = re.compile(r"(?<!\S)טרי(?!\S)")   # word-boundary for Hebrew

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def decode_xml_bytes(data: bytes) -> str:
    """Decompress gzip if needed, then decode to str (handles UTF-16 LE / UTF-8 BOM)."""
    if data[:2] == b"\x1f\x8b":                     # gzip magic
        with gzip.open(BytesIO(data)) as f:
            data = f.read()
    if data[:2] == b"\xff\xfe":                      # UTF-16 LE BOM
        return data.decode("utf-16-le").lstrip("\ufeff")
    if data[:3] == b"\xef\xbb\xbf":                 # UTF-8 BOM
        return data[3:].decode("utf-8")
    return data.decode("utf-8")


def parse_dt(raw: Optional[str]) -> Optional[datetime]:
    """Parse ISO-ish datetime from Israeli XML (e.g. '2026-12-31T23:59:00.000')."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw[:19])      # strip milliseconds
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# PublishedPrices fetcher  (Cerberus FTP web client)
# ---------------------------------------------------------------------------
class PublishedPricesFetcher:
    def __init__(self, portal_url: str, username: str, password: str,
                 price_prefix: str, promo_prefix: str):
        self.base         = portal_url.rstrip("/")
        self.username     = username
        self.password     = password
        self._verify      = False   # publishedprices.co.il — skip SSL verify
        self.s            = requests.Session()
        self.s.headers.update({"User-Agent": "pricetop/1.0"})
        # derive store tag from prefix: "PriceFull7290103152017-001-014" → "-014"
        m = re.search(r'(-\d{3})$', price_prefix)
        self.store_tag = m.group(1) if m else price_prefix

    # --- auth ---
    def _get_csrf(self, url: str) -> str:
        r = self.s.get(url, timeout=30, verify=self._verify)
        r.raise_for_status()
        m = re.search(r'csrftoken"\s+content="([^"]+)"', r.text)
        return m.group(1) if m else ""

    def login(self):
        csrf = self._get_csrf(f"{self.base}/login")
        self.s.post(
            f"{self.base}/login/user",
            data={"username": self.username, "password": self.password,
                  "csrftoken": csrf, "r": ""},
            timeout=30, verify=self._verify,
        )

    # --- file listing ---
    def _find_latest(self, kind: str, csrf: str) -> Optional[dict]:
        """Find latest file of kind 'Price' or 'Promo' for this store tag (e.g. -014)."""
        r = self.s.post(
            f"{self.base}/file/json/dir",
            data={"path": "/", "iDisplayLength": 200, "iDisplayStart": 0,
                  "sSearch": self.store_tag, "sEcho": 1, "csrftoken": csrf},
            timeout=30, verify=self._verify,
        )
        r.raise_for_status()
        files = r.json().get("aaData", [])
        matching = [f for f in files if f.get("fname", "").startswith(kind)]
        return sorted(matching, key=lambda x: x["time"])[-1] if matching else None

    # --- download (raw) ---
    def _download(self, fname: str) -> bytes:
        r = self.s.get(f"{self.base}/file/d/{fname}", timeout=120, verify=self._verify)
        r.raise_for_status()
        return r.content

    # --- get from local cache or download and cache ---
    def _get_or_download(self, fname: str) -> bytes:
        local = DATA_DIR / fname
        if local.exists():
            print(f"    [cache]    {fname}")
            return local.read_bytes()
        print(f"    [download] {fname}")
        data = self._download(fname)
        local.write_bytes(data)
        return data

    # --- promo map: ItemCode -> promo dict (active only) ---
    def _build_promo_map(self, promo_xml: ET.Element) -> dict:
        today    = datetime.now()
        promo_map: dict = {}

        for promo in promo_xml.findall(".//Promotion"):
            start = parse_dt(promo.findtext("PromotionStartDateTime"))
            end   = parse_dt(promo.findtext("PromotionEndDateTime"))
            if not start or not end:
                continue
            if not (start <= today <= end):
                continue

            desc     = promo.findtext("PromotionDescription", "")
            pid      = promo.findtext("PromotionID", "")
            end_date = (promo.findtext("PromotionEndDateTime") or "")[:10]
            club     = promo.findtext("ClubID", "0") not in ("0", "")

            for pi in promo.findall(".//PromotionItem"):
                code = pi.findtext("ItemCode")
                if not code:
                    continue
                raw_dp = pi.findtext("DiscountedPrice", "")
                raw_dr = pi.findtext("DiscountRate", "")
                entry  = {
                    "id":             pid,
                    "description":    desc,
                    "endDate":        end_date,
                    "clubOnly":       club,
                    "minQty":         pi.findtext("MinQty"),
                    "discountedPrice": round(float(raw_dp), 2) if raw_dp else None,
                    "discountRate":    round(float(raw_dr), 2) if raw_dr else None,
                }
                # prefer entry with an explicit discounted price
                if code not in promo_map or entry["discountedPrice"] is not None:
                    promo_map[code] = entry

        return promo_map

    # --- main fetch ---
    def _list_all_files(self, csrf: str) -> list:
        """Fetch full file listing from the portal (no prefix filter)."""
        r = self.s.post(
            f"{self.base}/file/json/dir",
            data={"path": "/", "iDisplayLength": 200, "iDisplayStart": 0,
                  "sSearch": "", "sEcho": 1, "csrftoken": csrf},
            timeout=30, verify=self._verify,
        )
        r.raise_for_status()
        return r.json().get("aaData", [])

    def fetch(self) -> Optional[dict]:
        self.login()
        csrf = self._get_csrf(f"{self.base}/file")

        # --- DEBUG: print all files visible in the portal ---
        all_files = self._list_all_files(csrf)
        print(f"  [DEBUG] {len(all_files)} files visible in portal:")
        for f in sorted(all_files, key=lambda x: x.get("fname", "")):
            size_kb = f.get("size", 0) // 1024
            print(f"    {f.get('fname', '?')}  ({size_kb} KB)")

        # --- find latest Price and Promo files for this store ---
        latest_price = self._find_latest("Price", csrf)
        latest_promo = self._find_latest("Promo", csrf)

        if not latest_price:
            print(f"  WARNING: no Price file found for store tag {self.store_tag}")
            return None

        print(f"  Price file : {latest_price['fname']}  ({latest_price['size']//1024} KB)")

        # --- prices ---
        price_xml  = ET.fromstring(decode_xml_bytes(self._get_or_download(latest_price["fname"])))
        all_items  = price_xml.findall(".//Item")
        fresh_items = [i for i in all_items
                       if FRESH_RE.search(i.findtext("ItemName") or "")]

        print(f"  Total items: {len(all_items)}  Fresh: {len(fresh_items)}")

        # --- promos ---
        promo_map: dict = {}
        if not latest_promo:
            print(f"  WARNING: no Promo file found for store tag {self.store_tag}")
        else:
            print(f"  Promo file : {latest_promo['fname']}  ({latest_promo['size']//1024} KB)")
            promo_xml = ET.fromstring(decode_xml_bytes(self._get_or_download(latest_promo["fname"])))
            promo_map = self._build_promo_map(promo_xml)

        # --- build product list ---
        products = []
        for item in sorted(fresh_items, key=lambda x: x.findtext("ItemName") or ""):
            code = item.findtext("ItemCode", "")
            products.append({
                "name":         item.findtext("ItemName", ""),
                "price":        round(float(item.findtext("ItemPrice")         or 0), 2),
                "unitPrice":    round(float(item.findtext("UnitOfMeasurePrice") or 0), 2),
                "unit":         item.findtext("UnitOfMeasure", ""),
                "quantity":     item.findtext("Quantity", ""),
                "updateDate":   item.findtext("PriceUpdateDate", ""),
                "itemCode":     code,
                "manufacturer": item.findtext("ManufacturerName", ""),
                "promo":        promo_map.get(code),
            })

        promo_count = sum(1 for p in products if p["promo"])
        print(f"  Promos on fresh products: {promo_count}")

        return {
            "sourceFile": latest_price["fname"],
            "promoFile":  latest_promo["fname"] if latest_promo else None,
            "totalItems": len(all_items),
            "freshCount": len(fresh_items),
            "promoCount": promo_count,
            "products":   products,
        }


# ---------------------------------------------------------------------------
# Shufersal fetcher  (placeholder — Azure blob portal)
# ---------------------------------------------------------------------------
class ShufersalFetcher:
    def __init__(self, store_id: str, **_):
        self.store_id = store_id

    def fetch(self) -> dict:
        raise NotImplementedError(
            "Shufersal fetcher not yet implemented. "
            "Add store_id to stores.csv and implement this class."
        )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
FETCHERS = {
    "publishedprices": lambda row: PublishedPricesFetcher(
        portal_url   = row["portal_url"],
        username     = row["משתמש"],
        password     = row["סיסמא"],
        price_prefix = row["price_prefix"],
        promo_prefix = row.get("promo_prefix", ""),
    ),
    "shufersal": lambda row: ShufersalFetcher(store_id=row.get("store_id", "")),
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if not STORES_CSV.exists():
        print(f"ERROR: {STORES_CSV} not found", file=sys.stderr)
        sys.exit(1)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    results  = []
    errors   = []

    with open(STORES_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            chain     = row["רשת"]
            store     = row["סניף"]
            feed_type = row["סוג_פיד"]

            print(f"\n=== {chain} / {store} ({feed_type}) ===")

            builder = FETCHERS.get(feed_type)
            if not builder:
                msg = f"Unknown feed type: {feed_type}"
                print(f"  WARNING: {msg}", file=sys.stderr)
                errors.append({"chain": chain, "store": store, "error": msg})
                continue

            try:
                data = builder(row).fetch()
                if data is None:
                    msg = "no price file found (warning only)"
                    print(f"  WARNING: {msg}")
                    errors.append({"chain": chain, "store": store, "error": msg})
                    continue
                results.append({
                    "chain":     chain,
                    "store":     store,
                    "feedType":  feed_type,
                    "fetchTime": datetime.now().strftime("%d/%m/%Y %H:%M"),
                    **data,
                })
            except Exception as exc:
                msg = str(exc)
                print(f"  ERROR: {msg}", file=sys.stderr)
                errors.append({"chain": chain, "store": store, "error": msg})

    output = {
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
        "storeCount":  len(results),
        "stores":      results,
        **({"errors": errors} if errors else {}),
    }

    OUTPUT_FILE.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nSaved {len(results)} store(s) → {OUTPUT_FILE}")
    if errors:
        print(f"WARNING: {len(errors)} store(s) had issues — check errors[] in JSON", file=sys.stderr)


if __name__ == "__main__":
    main()
