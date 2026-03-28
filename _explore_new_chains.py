#!/usr/bin/env python3
"""
_explore_new_chains.py
One-time exploration: login to each new chain, download the stores list,
search for Hadera stores, and report.
Run: python _explore_new_chains.py
"""
import sys, re, json, gzip
from io import BytesIO
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET
import requests, urllib3

urllib3.disable_warnings()
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

HADERA_RE = re.compile(r'חדר', re.UNICODE)  # matches חדרה, חדר, etc.
HADERA_CODES = {"6500", "6501", "6502", "6503", "6504", "6505"}  # known Hadera city codes

# ── Chains to explore ────────────────────────────────────────────────────────
CHAINS = [
    {"name": "דור אלון",         "user": "doralon",       "pass": "",         "base": "https://url.publishedprices.co.il"},
    {"name": "טיב טעם",          "user": "TivTaam",       "pass": "",         "base": "https://url.publishedprices.co.il"},
    {"name": "סאלח דבאח ובניו",  "user": "SalachD",       "pass": "12345",    "base": "https://url.publishedprices.co.il"},
    {"name": "סטופ מרקט",        "user": "Stop_Market",   "pass": "",         "base": "https://url.retail.publishedprices.co.il"},
    {"name": "פוליצר חדרה",      "user": "politzer",      "pass": "",         "base": "https://url.publishedprices.co.il"},
    {"name": "יילו (Paz)",        "user": "Paz_bo",        "pass": "paz468",   "base": "https://url.publishedprices.co.il"},
    {"name": "סופר יודה",        "user": "yuda_ho",       "pass": "Yud@147",  "base": "https://publishedprices.co.il"},
    {"name": "פרשמרקט",          "user": "freshmarket",   "pass": "",         "base": "https://url.publishedprices.co.il"},
    {"name": "קשת טעמים",        "user": "Keshet",        "pass": "",         "base": "https://url.publishedprices.co.il"},
    {"name": "סופר קופיקס",      "user": "SuperCofixApp", "pass": "",         "base": "https://url.publishedprices.co.il"},
]

def decode_bytes(data: bytes) -> str:
    if data[:2] == b"\x1f\x8b":
        with gzip.open(BytesIO(data)) as f:
            data = f.read()
    if data[:2] == b"\xff\xfe":
        return data.decode("utf-16-le").lstrip("\ufeff")
    if data[:3] == b"\xef\xbb\xbf":
        return data[3:].decode("utf-8")
    return data.decode("utf-8")

def get_csrf(session, url):
    r = session.get(url, timeout=30, verify=False)
    m = re.search(r'csrftoken"\s+content="([^"]+)"', r.text)
    return m.group(1) if m else ""

def login(session, base, user, pw):
    csrf = get_csrf(session, f"{base}/login")
    session.post(f"{base}/login/user",
                 data={"username": user, "password": pw, "csrftoken": csrf, "r": ""},
                 timeout=30, verify=False)

def find_stores_file(session, base, chain_id, csrf):
    r = session.post(f"{base}/file/json/dir",
                     data={"path": "/", "iDisplayLength": 100, "iDisplayStart": 0,
                           "sSearch": f"Stores{chain_id}", "sEcho": 1, "csrftoken": csrf},
                     timeout=30, verify=False)
    r.raise_for_status()
    files = [f for f in r.json().get("aaData", [])
             if f.get("fname","").startswith(f"Stores{chain_id}")]
    return sorted(files, key=lambda x: x["time"])[-1] if files else None

def get_chain_id_from_file(session, base, csrf):
    """List all files and find a chain ID from the first PriceFull/Stores filename."""
    r = session.post(f"{base}/file/json/dir",
                     data={"path": "/", "iDisplayLength": 200, "iDisplayStart": 0,
                           "sSearch": "", "sEcho": 1, "csrftoken": csrf},
                     timeout=30, verify=False)
    r.raise_for_status()
    files = r.json().get("aaData", [])
    for f in files:
        fname = f.get("fname", "")
        m = re.search(r'(\d{13})', fname)
        if m:
            return m.group(1), fname
    return None, None

def explore_chain(chain):
    name = chain["name"]
    base = chain["base"].rstrip("/")
    print(f"\n{'='*60}")
    print(f"  {name}  ({chain['user']})")
    print(f"{'='*60}")

    s = requests.Session()
    s.headers.update({"User-Agent": "pricetop/1.0"})

    # Login
    try:
        login(s, base, chain["user"], chain["pass"])
        print("  ✓ Login OK")
    except Exception as e:
        print(f"  ✗ Login FAILED: {e}")
        return None

    # CSRF for file operations
    try:
        csrf = get_csrf(s, f"{base}/file")
        print(f"  ✓ File CSRF: {csrf[:16]}...")
    except Exception as e:
        print(f"  ✗ CSRF FAILED: {e}")
        return None

    # Discover chain_id
    chain_id, sample_file = get_chain_id_from_file(s, base, csrf)
    if not chain_id:
        print("  ✗ Could not detect chain ID from file listing")
        return None
    print(f"  ✓ Chain ID: {chain_id}  (from: {sample_file})")

    # Check cache
    cache_path = DATA_DIR / f"stores_{chain_id}.json"
    if cache_path.exists():
        print(f"  [cache] {cache_path.name}")
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        stores = data.get("stores", [])
    else:
        # Find Stores file
        stores_file = find_stores_file(s, base, chain_id, csrf)
        if not stores_file:
            print(f"  ✗ No Stores file found for chain {chain_id}")
            return None
        print(f"  ✓ Stores file: {stores_file['fname']}  ({stores_file['size']//1024} KB)")

        # Download
        r = s.get(f"{base}/file/d/{stores_file['fname']}", timeout=60, verify=False)
        r.raise_for_status()
        raw = r.content
        xml_text = decode_bytes(raw)
        root = ET.fromstring(xml_text)

        stores = []
        for store in root.findall(".//Store"):
            stores.append({
                "storeId": (store.findtext("StoreId") or store.findtext("StoreID") or "").strip(),
                "name":    store.findtext("StoreName", "").strip(),
                "address": store.findtext("Address", "").strip(),
                "city":    store.findtext("City", "").strip(),
                "zip":     store.findtext("ZIPCode", "").strip(),
            })

        # Cache
        cache_path.write_text(
            json.dumps({"chainId": chain_id, "chainName": name,
                        "fetchedAt": datetime.now().isoformat(timespec="seconds"),
                        "stores": stores},
                       ensure_ascii=False, indent=2),
            encoding="utf-8")
        print(f"  ✓ Cached {len(stores)} stores → {cache_path.name}")

    # Search for Hadera
    hadera = [st for st in stores
              if HADERA_RE.search(st.get("name","") + " " + st.get("address","") + " " + st.get("city",""))
              or st.get("city","").strip() in HADERA_CODES]

    print(f"\n  Total stores in chain: {len(stores)}")
    if hadera:
        print(f"  *** {len(hadera)} HADERA STORE(S) FOUND: ***")
        for st in hadera:
            print(f"      StoreID={st['storeId']}  |  {st['name']}  |  {st['address']}  |  city={st['city']}")
    else:
        print("  — No Hadera stores found")

    return {"chain_id": chain_id, "chain_name": name, "hadera_stores": hadera}


def main():
    results = []
    for chain in CHAINS:
        try:
            r = explore_chain(chain)
            if r:
                results.append(r)
        except Exception as e:
            print(f"  ✗ UNEXPECTED ERROR: {e}")
        input("\n  [Enter] לעבור לרשת הבאה...")

    print("\n\n" + "="*60)
    print("  סיכום — רשתות עם סניפים בחדרה:")
    print("="*60)
    found = [r for r in results if r and r["hadera_stores"]]
    if found:
        for r in found:
            print(f"  {r['chain_name']}  (chain_id={r['chain_id']})")
            for st in r["hadera_stores"]:
                print(f"    → StoreID={st['storeId']}  {st['name']}  {st['address']}")
    else:
        print("  לא נמצאו רשתות עם סניפים בחדרה")

if __name__ == "__main__":
    main()
