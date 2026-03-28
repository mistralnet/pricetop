#!/usr/bin/env python3
"""
_rebuild_stores_csv.py
Downloads Stores XML for every chain, then rebuilds stores.csv so it contains
ALL stores from ALL chains.

Schema of new stores.csv:
  רשת, chain_id, סניף, store_id, subchain_id, עיר, כתובת, zip,
  משתמש, סיסמא, סוג_פיד, portal_url, price_prefix, promo_prefix

Rules:
  - price_prefix / promo_prefix filled ONLY for stores in TARGET_CITIES.
  - For all other stores those two columns are left empty — fetch_prices.py skips them.
  - City identification: city-code 6500, city-name containing "חדר", or Hadera ZIP range.

Run:
  python _rebuild_stores_csv.py
"""

import csv, re, sys, json, gzip
from io import BytesIO
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET
import requests, urllib3

urllib3.disable_warnings()
sys.stdout.reconfigure(encoding="utf-8")

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR    = Path("data")
STORES_CSV  = Path("stores.csv")
TARGET_CITIES = {"חדרה"}          # cities to fetch price data for

# Hadera detection (some chains use city code, some name, some wrong / empty)
HADERA_RE   = re.compile(r'חדר', re.UNICODE)
HADERA_CODES = {"6500", "6501", "6502"}
HADERA_ZIP   = re.compile(r'^38[0-3]\d{4}$')   # Israeli ZIPs 3800000-3839999

# ── Chain definitions ─────────────────────────────────────────────────────────
# subchain: "001" for standard chains; "" for chains whose filename has no SubChainID
# no_subchain_stores: set of store IDs that lack SubChainID in *this* chain's filenames
CHAINS = [
    {
        "name":    "אושר עד",
        "user":    "osherad",
        "pass":    "",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "001",
        "no_subchain_stores": set(),
    },
    {
        "name":    "רמי לוי",
        "user":    "RamiLevi",
        "pass":    "",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "",           # Rami Levy has NO SubChainID in filenames
        "no_subchain_stores": set(),
    },
    {
        "name":    "יוחננוף",
        "user":    "yohananof",
        "pass":    "",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "",
        "no_subchain_stores": set(),
    },
    {
        "name":    "דור אלון",
        "user":    "doralon",
        "pass":    "",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "001",
        "no_subchain_stores": {"968", "992"},  # these two lack SubChainID
    },
    {
        "name":    "טיב טעם",
        "user":    "TivTaam",
        "pass":    "",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "",
        "no_subchain_stores": set(),
    },
    {
        "name":    "סאלח דבאח ובניו",
        "user":    "SalachD",
        "pass":    "12345",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "001",
        "no_subchain_stores": set(),
    },
    {
        "name":    "סטופ מרקט",
        "user":    "Stop_Market",
        "pass":    "",
        "base":    "https://url.retail.publishedprices.co.il",
        "subchain": "001",
        "no_subchain_stores": set(),
    },
    {
        "name":    "פוליצר",
        "user":    "politzer",
        "pass":    "",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "001",
        "no_subchain_stores": set(),
    },
    {
        "name":    "יילו",
        "user":    "Paz_bo",
        "pass":    "paz468",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "001",
        "no_subchain_stores": set(),
    },
    {
        "name":    "פרשמרקט",
        "user":    "freshmarket",
        "pass":    "",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "001",
        "no_subchain_stores": set(),
    },
    {
        "name":    "קשת טעמים",
        "user":    "Keshet",
        "pass":    "",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "001",
        "no_subchain_stores": set(),
        "no_stores_file": True,   # portal has no Stores XML
    },
    {
        "name":    "סופר קופיקס",
        "user":    "SuperCofixApp",
        "pass":    "",
        "base":    "https://url.publishedprices.co.il",
        "subchain": "001",
        "no_subchain_stores": set(),
        "no_price_files": True,   # only Stores XML; price data via Rami Levy
    },
]

# ── Known Hadera stores (price_prefix + promo_prefix) ─────────────────────────
# Key: (chain_id, store_id)  →  (price_prefix, promo_prefix)
KNOWN_PREFIXES: dict[tuple, tuple] = {
    ("7290103152017", "014"): ("PriceFull7290103152017-001-014", "PromoFull7290103152017-001-014"),
    ("7290058140886", "044"): ("PriceFull7290058140886-044",     "PromoFull7290058140886-044"),
    ("7290058140886", "058"): ("PriceFull7290058140886-058",     "PromoFull7290058140886-058"),
    ("7290058140886", "716"): ("PriceFull7290058140886-716",     "PromoFull7290058140886-716"),
    ("7290058140886", "717"): ("PriceFull7290058140886-717",     "PromoFull7290058140886-717"),
    ("7290803800003", "022"): ("PriceFull7290803800003-022",     "Promo7290803800003-022"),
    ("7290803800003", "036"): ("PriceFull7290803800003-036",     "PromoFull7290803800003-036"),
    ("7290492000005", "594"): ("PriceFull7290492000005-001-594", "PromoFull7290492000005-001-594"),
    ("7290492000005", "968"): ("PriceFull7290492000005-968",     "PromoFull7290492000005-968"),
    ("7290492000005", "992"): ("PriceFull7290492000005-992",     "PromoFull7290492000005-992"),
    ("7290644700005", "308"): ("PriceFull7290644700005-001-308", "PromoFull7290644700005-001-308"),
    ("7290644700005", "324"): ("PriceFull7290644700005-001-324", "PromoFull7290644700005-001-324"),
    ("7290644700005", "325"): ("PriceFull7290644700005-001-325", "PromoFull7290644700005-001-325"),
    ("7291059100008", "001"): ("PriceFull7291059100008-001-001", "PromoFull7291059100008-001-001"),
    ("7291059100008", "004"): ("PriceFull7291059100008-001-004", "PromoFull7291059100008-001-004"),
}

# ── Helpers ───────────────────────────────────────────────────────────────────
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

def is_hadera(store: dict) -> bool:
    city = store.get("city", "").strip()
    if city in HADERA_CODES or HADERA_RE.search(city):
        return True
    name = store.get("name", "")
    addr = store.get("address", "")
    if HADERA_RE.search(name) or HADERA_RE.search(addr):
        return True
    if HADERA_ZIP.match(store.get("zip", "")):
        return True
    return False

def resolve_city(store: dict) -> str:
    """Return a human-readable city name for display."""
    city = store.get("city", "").strip()
    if city in HADERA_CODES or HADERA_RE.search(city):
        return "חדרה"
    if HADERA_ZIP.match(store.get("zip", "")):
        return "חדרה"
    # Try store name / address for a hint
    for field in (store.get("name",""), store.get("address","")):
        if HADERA_RE.search(field):
            return "חדרה"
    return city if city else "—"

def make_prefix(chain: dict, chain_id: str, store_id: str, kind: str) -> str:
    """Derive price/promo prefix for a store based on chain format."""
    sid = store_id.zfill(3)
    if store_id in chain.get("no_subchain_stores", set()):
        return f"{kind}{chain_id}-{sid}"
    sub = chain.get("subchain", "001")
    if sub:
        return f"{kind}{chain_id}-{sub}-{sid}"
    return f"{kind}{chain_id}-{sid}"

# ── Per-chain store download ───────────────────────────────────────────────────
def fetch_chain_stores(chain: dict) -> tuple[str | None, list[dict]]:
    """Login, download Stores XML, return (chain_id, stores_list)."""
    name = chain["name"]
    base = chain["base"].rstrip("/")

    if chain.get("no_stores_file"):
        print(f"  [{name}] ⚠ no Stores file — skipping")
        return None, []

    s = requests.Session()
    s.headers.update({"User-Agent": "pricetop/1.0"})

    try:
        csrf = get_csrf(s, f"{base}/login")
        s.post(f"{base}/login/user",
               data={"username": chain["user"], "password": chain["pass"],
                     "csrftoken": csrf, "r": ""},
               timeout=30, verify=False)
        csrf2 = get_csrf(s, f"{base}/file")
    except Exception as e:
        print(f"  [{name}] ✗ login/CSRF: {e}")
        return None, []

    # Discover chain_id from file listing
    r = s.post(f"{base}/file/json/dir",
               data={"path": "/", "iDisplayLength": 200, "iDisplayStart": 0,
                     "sSearch": "", "sEcho": 1, "csrftoken": csrf2},
               timeout=30, verify=False)
    files = r.json().get("aaData", [])
    chain_id = None
    for f in files:
        m = re.search(r"(\d{13})", f.get("fname", ""))
        if m:
            chain_id = m.group(1)
            break
    if not chain_id:
        print(f"  [{name}] ✗ could not detect chain_id")
        return None, []

    # Check local cache first
    cache_path = DATA_DIR / f"stores_{chain_id}.json"
    if cache_path.exists():
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        stores = data.get("stores", [])
        print(f"  [{name}] ✓ cache ({len(stores)} stores)")
        return chain_id, stores

    # Find Stores XML
    stores_files = [f for f in files if f.get("fname", "").startswith(f"Stores{chain_id}")]
    if not stores_files:
        r2 = s.post(f"{base}/file/json/dir",
                    data={"path": "/", "iDisplayLength": 50, "iDisplayStart": 0,
                          "sSearch": f"Stores{chain_id}", "sEcho": 1, "csrftoken": csrf2},
                    timeout=30, verify=False)
        stores_files = [f for f in r2.json().get("aaData", [])
                        if f.get("fname", "").startswith(f"Stores{chain_id}")]
    if not stores_files:
        print(f"  [{name}] ✗ no Stores file")
        return chain_id, []

    sf = sorted(stores_files, key=lambda x: x["time"])[-1]
    raw = s.get(f"{base}/file/d/{sf['fname']}", timeout=60, verify=False).content
    root = ET.fromstring(decode_bytes(raw))

    stores = []
    for st in root.findall(".//Store"):
        stores.append({
            "storeId":   (st.findtext("StoreId") or st.findtext("StoreID") or "").strip(),
            "name":      st.findtext("StoreName", "").strip(),
            "address":   st.findtext("Address", "").strip(),
            "city":      st.findtext("City", "").strip(),
            "zip":       (st.findtext("ZipCode") or st.findtext("ZIPCode") or "").strip(),
            "subchainId":(st.findtext("SubChainID") or "").strip(),
        })

    cache_path.write_text(
        json.dumps({"chainId": chain_id, "chainName": name,
                    "fetchedAt": datetime.now().isoformat(timespec="seconds"),
                    "stores": stores},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  [{name}] ✓ {len(stores)} stores → {cache_path.name}")
    return chain_id, stores

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    DATA_DIR.mkdir(exist_ok=True)

    print("Downloading stores for all chains (parallel)...")
    chain_results: dict[str, tuple] = {}   # name → (chain_id, stores)

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fetch_chain_stores, ch): ch for ch in CHAINS}
        for future in as_completed(futures):
            ch = futures[future]
            chain_id, stores = future.result()
            chain_results[ch["name"]] = (ch, chain_id, stores)

    # ── Build rows ────────────────────────────────────────────────────────────
    fieldnames = [
        "רשת", "chain_id", "סניף", "store_id", "subchain_id",
        "עיר", "כתובת", "zip",
        "משתמש", "סיסמא", "סוג_פיד", "portal_url",
        "price_prefix", "promo_prefix",
    ]

    rows = []
    for chain_def in CHAINS:
        name = chain_def["name"]
        chain, chain_id, stores = chain_results.get(name, (chain_def, None, []))

        if chain_def.get("no_price_files"):
            # Metadata-only chain (Super Cofix) — list stores but mark clearly
            for st in stores:
                city_display = resolve_city(st)
                rows.append({
                    "רשת":         name,
                    "chain_id":    chain_id or "",
                    "סניף":        st["name"],
                    "store_id":    st["storeId"],
                    "subchain_id": chain_def.get("subchain", "001"),
                    "עיר":         city_display,
                    "כתובת":       st["address"],
                    "zip":         st.get("zip", ""),
                    "משתמש":       chain_def["user"],
                    "סיסמא":       chain_def["pass"],
                    "סוג_פיד":     "publishedprices",
                    "portal_url":  chain_def["base"],
                    "price_prefix": "",   # no price files for this chain
                    "promo_prefix": "",
                })
            continue

        for st in stores:
            sid    = st["storeId"]
            city_d = resolve_city(st)
            hadera = is_hadera(st)

            # Determine subchain for this specific store
            subchain = chain_def.get("subchain", "001")
            if sid in chain_def.get("no_subchain_stores", set()):
                subchain = ""

            # price_prefix: filled only if Hadera (and we know the format)
            pp, pp2 = "", ""
            if hadera and chain_id:
                key = (chain_id, sid)
                if key in KNOWN_PREFIXES:
                    pp, pp2 = KNOWN_PREFIXES[key]
                else:
                    # Auto-derive using chain format rules
                    pp  = make_prefix(chain_def, chain_id, sid, "PriceFull")
                    pp2 = make_prefix(chain_def, chain_id, sid, "PromoFull")

            rows.append({
                "רשת":         name,
                "chain_id":    chain_id or "",
                "סניף":        st["name"],
                "store_id":    sid,
                "subchain_id": subchain,
                "עיר":         city_d,
                "כתובת":       st["address"],
                "zip":         st.get("zip", ""),
                "משתמש":       chain_def["user"],
                "סיסמא":       chain_def["pass"],
                "סוג_פיד":     "publishedprices",
                "portal_url":  chain_def["base"],
                "price_prefix": pp,
                "promo_prefix": pp2,
            })

    # ── Write CSV ─────────────────────────────────────────────────────────────
    with open(STORES_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    total = len(rows)
    hadera_rows = sum(1 for r in rows if r["price_prefix"])
    print(f"\n✓ stores.csv rebuilt: {total} stores total, {hadera_rows} with price data (Hadera)")
    hadera_chains = {}
    for r in rows:
        if r["price_prefix"]:
            hadera_chains.setdefault(r["רשת"], []).append(r["סניף"])
    for chain_name, stores_list in hadera_chains.items():
        print(f"  {chain_name}: {', '.join(stores_list)}")

if __name__ == "__main__":
    main()
