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
import threading
from concurrent.futures import ThreadPoolExecutor
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
MAX_WORKERS = 4                                  # max parallel stores

# ---------------------------------------------------------------------------
# Thread-safety: per-chain lock for stores-cache download
# ---------------------------------------------------------------------------
_chain_locks: dict[str, threading.Lock] = {}
_chain_locks_guard = threading.Lock()

def _chain_lock(chain_id: str) -> threading.Lock:
    with _chain_locks_guard:
        if chain_id not in _chain_locks:
            _chain_locks[chain_id] = threading.Lock()
        return _chain_locks[chain_id]

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
                 price_prefix: str, promo_prefix: str,
                 prefetched_cookies: Optional[dict] = None):
        self.base         = portal_url.rstrip("/")
        self.username     = username
        self.password     = password
        self._verify      = False   # publishedprices.co.il — skip SSL verify
        self.s            = requests.Session()
        self.s.headers.update({"User-Agent": "pricetop/1.0"})
        self._log: list[str] = []
        self.promo_prefix = promo_prefix

        # derive store_tag from last 3-digit group: "PriceFull7290058140886-044" → "-044"
        m = re.search(r'(-\d{3})$', price_prefix)
        self.store_tag = m.group(1) if m else price_prefix
        # derive chain_id (13 digits after the type prefix): "PriceFull7290058140886-044" → "7290058140886"
        mc = re.match(r'\D+(\d{13})', price_prefix)
        self.chain_id = mc.group(1) if mc else ""

        # Pre-seed cookies from chain-level login — skip login() later
        self._logged_in = False
        if prefetched_cookies:
            self.s.cookies.update(prefetched_cookies)
            self._logged_in = True

    def _p(self, msg: str) -> None:
        """Append message to this store's log buffer."""
        self._log.append(msg)

    # --- auth ---
    def _get_csrf(self, url: str) -> str:
        r = self.s.get(url, timeout=30, verify=self._verify)
        r.raise_for_status()
        m = re.search(r'csrftoken"\s+content="([^"]+)"', r.text)
        return m.group(1) if m else ""

    def login(self):
        if self._logged_in:
            return
        csrf = self._get_csrf(f"{self.base}/login")
        self.s.post(
            f"{self.base}/login/user",
            data={"username": self.username, "password": self.password,
                  "csrftoken": csrf, "r": ""},
            timeout=30, verify=self._verify,
        )
        self._logged_in = True

    # --- file listing ---
    # Matches store_tag immediately before a timestamp (8+ digits).
    # Example: store_tag="-001" matches "-001-20260328" but NOT "-001-002-20260328"
    # This handles the edge case where SubChainID == StoreID (e.g. politzer store 001,
    # SubChain 001) — the old "-001-" substring check would match all stores in SubChain 001.
    _STORE_TAG_RE = re.compile(r'%s-\d{8}')

    def _find_latest(self, kind: str, csrf: str) -> Optional[dict]:
        """Find latest file whose name starts with <kind> and whose store segment
        appears immediately before a timestamp (YYYYMMDD), avoiding false matches
        when SubChainID == StoreID."""
        r = self.s.post(
            f"{self.base}/file/json/dir",
            data={"path": "/", "iDisplayLength": 200, "iDisplayStart": 0,
                  "sSearch": self.store_tag, "sEcho": 1, "csrftoken": csrf},
            timeout=30, verify=self._verify,
        )
        r.raise_for_status()
        files = r.json().get("aaData", [])
        tag_re = re.compile(re.escape(self.store_tag) + r'-\d{8}')
        matching = [f for f in files
                    if f.get("fname", "").startswith(kind)
                    and tag_re.search(f.get("fname", ""))]
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
            self._p(f"    [cache]    {fname}")
            return local.read_bytes()
        self._p(f"    [download] {fname}")
        data = self._download(fname)
        local.write_bytes(data)
        return data

    # --- promo map: ItemCode -> promo dict (active only) ---
    def _build_promo_map(self, promo_xml: ET.Element) -> dict:
        """
        Builds ItemCode → promo dict for all active promotions.
        Handles two XML formats:
          Osher Ad:  PromotionStartDateTime/PromotionEndDateTime (combined)
                     <PromotionItem> with per-item DiscountedPrice
                     <ClubID>
          Rami Levy: PromotionStartDate+PromotionStartHour / PromotionEndDate+PromotionEndHour
                     <Item> children (no per-item discount) + promotion-level DiscountedPrice
                     <Clubs>
        """
        today     = datetime.now()
        promo_map: dict = {}

        for promo in promo_xml.findall(".//Promotion"):
            # --- date: try combined (Osher Ad) then separate fields (Rami Levy) ---
            start = parse_dt(promo.findtext("PromotionStartDateTime"))
            if start is None:
                d = promo.findtext("PromotionStartDate", "")
                h = promo.findtext("PromotionStartHour", "00:00:00")
                start = parse_dt(f"{d}T{h}") if d else None

            end = parse_dt(promo.findtext("PromotionEndDateTime"))
            if end is None:
                d = promo.findtext("PromotionEndDate", "")
                h = promo.findtext("PromotionEndHour", "23:59:00")
                end = parse_dt(f"{d}T{h}") if d else None

            if not start or not end or not (start <= today <= end):
                continue

            # --- metadata ---
            pid      = promo.findtext("PromotionID") or promo.findtext("PromotionId", "")
            desc     = promo.findtext("PromotionDescription", "")
            end_date = (promo.findtext("PromotionEndDateTime")
                        or promo.findtext("PromotionEndDate") or "")[:10]

            # --- club check: ClubID (Osher Ad) or Clubs (Rami Levy) ---
            club_id = promo.findtext("ClubID", "0")
            clubs   = (promo.findtext("Clubs") or "").strip()
            club    = (club_id not in ("0", "")) or bool(clubs)

            # --- promotion-level discount (Rami Levy puts it here, not per-item) ---
            promo_dp  = promo.findtext("DiscountedPrice", "")
            promo_dr  = promo.findtext("DiscountRate", "")
            promo_qty = promo.findtext("MinQty")

            # --- items: <PromotionItem> (Osher Ad) or <Item> (Rami Levy) ---
            items = promo.findall(".//PromotionItem") or promo.findall(".//Item")

            for pi in items:
                code = pi.findtext("ItemCode")
                if not code:
                    continue
                # item-level discount takes priority; fall back to promotion-level
                raw_dp = pi.findtext("DiscountedPrice") or promo_dp
                raw_dr = pi.findtext("DiscountRate")    or promo_dr
                entry  = {
                    "id":              pid,
                    "description":     desc,
                    "endDate":         end_date,
                    "clubOnly":        club,
                    "minQty":          pi.findtext("MinQty") or promo_qty,
                    "discountedPrice": round(float(raw_dp), 2) if raw_dp else None,
                    "discountRate":    round(float(raw_dr), 2) if raw_dr else None,
                }
                if code not in promo_map or entry["discountedPrice"] is not None:
                    promo_map[code] = entry

        return promo_map

    # --- stores cache: download Stores XML once per chain, reuse on next runs ---
    def _ensure_stores_cache(self, csrf: str) -> list:
        """
        Downloads the Stores XML for this chain once and caches it to
        data/stores_<chainid>.json. On subsequent runs, reads from cache.
        Thread-safe: per-chain lock prevents duplicate downloads when stores
        of the same chain run in parallel.
        To force refresh: delete the cache file.
        """
        cache_path = DATA_DIR / f"stores_{self.chain_id}.json"
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8")).get("stores", [])

        with _chain_lock(self.chain_id):
            # Double-check after acquiring lock (another thread may have downloaded it)
            if cache_path.exists():
                return json.loads(cache_path.read_text(encoding="utf-8")).get("stores", [])

            self._p(f"  [stores] First run for chain {self.chain_id} — downloading store list...")
            r = self.s.post(
                f"{self.base}/file/json/dir",
                data={"path": "/", "iDisplayLength": 50, "iDisplayStart": 0,
                      "sSearch": f"Stores{self.chain_id}", "sEcho": 1, "csrftoken": csrf},
                timeout=30, verify=self._verify,
            )
            r.raise_for_status()
            candidates = [f for f in r.json().get("aaData", [])
                          if f.get("fname", "").startswith(f"Stores{self.chain_id}")]
            if not candidates:
                self._p(f"  WARNING: no Stores file found for chain {self.chain_id}")
                return []

            latest = sorted(candidates, key=lambda x: x["time"])[-1]
            self._p(f"  [stores] {latest['fname']}  ({latest['size']//1024} KB)")
            raw = self._download(latest["fname"])
            root = ET.fromstring(decode_xml_bytes(raw))

            stores = []
            for store in root.findall(".//Store"):
                stores.append({
                    "storeId": (store.findtext("StoreId") or store.findtext("StoreID") or "").strip(),
                    "name":    store.findtext("StoreName", "").strip(),
                    "address": store.findtext("Address", "").strip(),
                    "city":    store.findtext("City", "").strip(),
                })

            cache_path.write_text(
                json.dumps({"chainId": self.chain_id,
                            "fetchedAt": datetime.now().isoformat(timespec="seconds"),
                            "stores": stores},
                           ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self._p(f"  [stores] Cached {len(stores)} stores → {cache_path.name}")
            return stores

    # --- main fetch ---
    def fetch(self, log: Optional[list] = None) -> Optional[dict]:
        """
        Fetch prices and promos for this store.
        If `log` is provided, all output lines are appended to it instead of
        being printed immediately (enables clean parallel output in main).
        """
        if log is not None:
            self._log = log

        self.login()
        csrf = self._get_csrf(f"{self.base}/file")

        # Ensure store list is cached (downloads Stores XML only on first run per chain)
        if self.chain_id:
            self._ensure_stores_cache(csrf)

        # --- find latest Price and Promo files for this store ---
        latest_price = self._find_latest("Price", csrf)
        # Use the exact promo kind from promo_prefix (PromoFull vs Promo).
        # Searching for just "Promo" would also match delta files (e.g. Promo7290058140886-044-...)
        # which are updated more frequently and thus always sort as newest — hiding the PromoFull.
        promo_kind = "PromoFull" if self.promo_prefix.startswith("PromoFull") else "Promo"
        latest_promo = self._find_latest(promo_kind, csrf)

        if not latest_price:
            self._p(f"  WARNING: no Price file found for store tag {self.store_tag}")
            return None

        self._p(f"  Price file : {latest_price['fname']}  ({latest_price['size']//1024} KB)")

        # --- prices ---
        price_xml  = ET.fromstring(decode_xml_bytes(self._get_or_download(latest_price["fname"])))
        all_items  = price_xml.findall(".//Item")
        fresh_items = [i for i in all_items
                       if FRESH_RE.search(i.findtext("ItemName") or "")]

        self._p(f"  Total items: {len(all_items)}  Fresh: {len(fresh_items)}")

        # --- promos ---
        promo_map: dict = {}
        if not latest_promo:
            self._p(f"  WARNING: no Promo file found for store tag {self.store_tag}")
        else:
            self._p(f"  Promo file : {latest_promo['fname']}  ({latest_promo['size']//1024} KB)")
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
        self._p(f"  Promos on fresh products: {promo_count}")

        return {
            "sourceFile":    latest_price["fname"],
            "promoFile":     latest_promo["fname"] if latest_promo else None,
            "totalItems":    len(all_items),
            "freshCount":    len(fresh_items),
            "promoRawCount": len(promo_map),
            "promoCount":    promo_count,
            "products":      products,
        }


# ---------------------------------------------------------------------------
# Shufersal fetcher  (placeholder — Azure blob portal)
# ---------------------------------------------------------------------------
class ShufersalFetcher:
    def __init__(self, store_id: str, **_):
        self.store_id = store_id

    def fetch(self, log: Optional[list] = None) -> dict:
        raise NotImplementedError(
            "Shufersal fetcher not yet implemented. "
            "Add store_id to stores.csv and implement this class."
        )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
FETCHERS = {
    "publishedprices": lambda row, **kw: PublishedPricesFetcher(
        portal_url   = row["portal_url"],
        username     = row["משתמש"],
        password     = row["סיסמא"],
        price_prefix = row["price_prefix"],
        promo_prefix = row.get("promo_prefix", ""),
        **kw,
    ),
    "shufersal": lambda row, **kw: ShufersalFetcher(store_id=row.get("store_id", "")),
}


# ---------------------------------------------------------------------------
# Phase 0: ensure all chain store-caches are current, update stores.csv
# ---------------------------------------------------------------------------
_HADERA_RE    = re.compile(r'חדר', re.UNICODE)
_HADERA_CODES = {"6500", "6501", "6502"}
_HADERA_ZIP   = re.compile(r'^38[0-3]\d{4}$')

# Known price/promo prefixes for Hadera stores.
# Key: (chain_id, store_id) → (price_prefix, promo_prefix)
_KNOWN_PREFIXES: dict[tuple, tuple] = {
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

_CSV_FIELDNAMES = [
    "רשת", "chain_id", "סניף", "store_id", "subchain_id",
    "עיר", "כתובת", "zip",
    "משתמש", "סיסמא", "סוג_פיד", "portal_url",
    "price_prefix", "promo_prefix",
]


def _is_hadera(st: dict) -> bool:
    city = st.get("city", "").strip()
    if city in _HADERA_CODES or _HADERA_RE.search(city):
        return True
    if _HADERA_RE.search(st.get("name", "")) or _HADERA_RE.search(st.get("address", "")):
        return True
    if _HADERA_ZIP.match(st.get("zip", "")):
        return True
    return False


def _download_stores_for_chain(chain_id: str, rep_row: dict) -> list[dict]:
    """Login to chain portal, download Stores XML, cache to data/stores_<chain_id>.json.
    Returns list of store dicts (empty on failure)."""
    base  = rep_row["portal_url"].rstrip("/")
    name  = rep_row["רשת"]
    s = requests.Session()
    s.headers.update({"User-Agent": "pricetop/1.0"})
    try:
        r = s.get(f"{base}/login", timeout=30, verify=False)
        m = re.search(r'csrftoken"\s+content="([^"]+)"', r.text)
        csrf = m.group(1) if m else ""
        s.post(f"{base}/login/user",
               data={"username": rep_row["משתמש"], "password": rep_row["סיסמא"],
                     "csrftoken": csrf, "r": ""},
               timeout=30, verify=False)
        r2 = s.get(f"{base}/file", timeout=30, verify=False)
        m2 = re.search(r'csrftoken"\s+content="([^"]+)"', r2.text)
        csrf2 = m2.group(1) if m2 else ""

        r3 = s.post(f"{base}/file/json/dir",
                    data={"path": "/", "iDisplayLength": 50, "iDisplayStart": 0,
                          "sSearch": f"Stores{chain_id}", "sEcho": 1, "csrftoken": csrf2},
                    timeout=30, verify=False)
        candidates = [f for f in r3.json().get("aaData", [])
                      if f.get("fname", "").startswith(f"Stores{chain_id}")]
        if not candidates:
            print(f"  [Phase 0] WARNING: no Stores file for chain {chain_id} ({name})", file=sys.stderr)
            return []

        latest = sorted(candidates, key=lambda x: x["time"])[-1]
        raw = s.get(f"{base}/file/d/{latest['fname']}", timeout=60, verify=False).content
        root = ET.fromstring(decode_xml_bytes(raw))

        stores = []
        for st in root.findall(".//Store"):
            stores.append({
                "storeId":  (st.findtext("StoreId") or st.findtext("StoreID") or "").strip(),
                "name":     st.findtext("StoreName", "").strip(),
                "address":  st.findtext("Address", "").strip(),
                "city":     st.findtext("City", "").strip(),
                "zip":      (st.findtext("ZipCode") or st.findtext("ZIPCode") or "").strip(),
            })

        cache_path = DATA_DIR / f"stores_{chain_id}.json"
        cache_path.write_text(
            json.dumps({"chainId": chain_id, "chainName": name,
                        "fetchedAt": datetime.now().isoformat(timespec="seconds"),
                        "stores": stores},
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"  [Phase 0] {name}: downloaded {len(stores)} stores → {cache_path.name}")
        return stores
    except Exception as exc:
        print(f"  [Phase 0] WARNING: {name} — {exc}", file=sys.stderr)
        return []


def _phase0_ensure_stores(all_rows: list[dict]) -> None:
    """Phase 0: for every chain in stores.csv, ensure data/stores_<chain_id>.json exists.
    Downloads any missing caches in parallel, then appends genuinely new stores to stores.csv
    (with empty price_prefix for non-Hadera, known prefix for Hadera)."""
    # Group by chain_id; keep first row as representative for credentials
    chains: dict[str, dict] = {}
    for row in all_rows:
        cid = row.get("chain_id", "").strip()
        if cid and cid not in chains:
            chains[cid] = row

    missing = {cid: rep for cid, rep in chains.items()
               if not (DATA_DIR / f"stores_{cid}.json").exists()}
    if not missing:
        return   # All caches present — nothing to do

    print(f"\n[Phase 0] {len(missing)} chain cache(s) missing — downloading...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_download_stores_for_chain, cid, rep): cid
                   for cid, rep in missing.items()}
        downloaded: dict[str, list] = {}
        for fut in futures:
            cid = futures[fut]
            downloaded[cid] = fut.result()

    # Reconcile: find stores not yet in stores.csv
    existing_ids = {(r["chain_id"], r["store_id"]) for r in all_rows}
    new_rows: list[dict] = []
    for cid, stores in downloaded.items():
        rep = missing[cid]
        for st in stores:
            sid = st["storeId"]
            if not sid or (cid, sid) in existing_ids:
                continue
            hadera = _is_hadera(st)
            pp, pp2 = "", ""
            if hadera:
                pp, pp2 = _KNOWN_PREFIXES.get((cid, sid), ("", ""))
            new_rows.append({
                "רשת":         rep["רשת"],
                "chain_id":    cid,
                "סניף":        st["name"],
                "store_id":    sid,
                "subchain_id": rep.get("subchain_id", ""),
                "עיר":         st.get("city", ""),
                "כתובת":       st.get("address", ""),
                "zip":         st.get("zip", ""),
                "משתמש":       rep["משתמש"],
                "סיסמא":       rep["סיסמא"],
                "סוג_פיד":     rep["סוג_פיד"],
                "portal_url":  rep["portal_url"],
                "price_prefix": pp,
                "promo_prefix": pp2,
            })

    if new_rows:
        with open(STORES_CSV, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_CSV_FIELDNAMES)
            writer.writerows(new_rows)
        print(f"[Phase 0] Appended {len(new_rows)} new store(s) to stores.csv")
    else:
        print("[Phase 0] Done — no new stores found")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if not STORES_CSV.exists():
        print(f"ERROR: {STORES_CSV} not found", file=sys.stderr)
        sys.exit(1)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(STORES_CSV, encoding="utf-8-sig") as f:
        all_rows = list(csv.DictReader(f))

    # ── Phase 0: Ensure all chain store-caches exist; update stores.csv ────
    _phase0_ensure_stores(all_rows)

    # Re-read stores.csv in case Phase 0 appended new rows
    with open(STORES_CSV, encoding="utf-8-sig") as f:
        all_rows = list(csv.DictReader(f))

    # Filter: only rows with a price_prefix (stores to actually fetch).
    # Rows without price_prefix are recorded in stores.csv for reference but not fetched.
    rows = [r for r in all_rows if r.get("price_prefix", "").strip()]

    # ── Phase 1: Login once per chain ──────────────────────────────────────
    # For publishedprices chains, login once per (portal_url, username) pair
    # and share the resulting cookies across all stores of that chain.
    # This reduces N-store logins to M-chain logins.
    chain_cookies: dict[tuple, dict] = {}
    for row in rows:
        if row["סוג_פיד"] != "publishedprices":
            continue
        key = (row["portal_url"].rstrip("/"), row["משתמש"])
        if key not in chain_cookies:
            print(f"[login] {row['רשת']} ({row['משתמש']})...", end=" ", flush=True)
            tmp = PublishedPricesFetcher(
                portal_url   = row["portal_url"],
                username     = row["משתמש"],
                password     = row["סיסמא"],
                price_prefix = row["price_prefix"],
                promo_prefix = row.get("promo_prefix", ""),
            )
            tmp.login()
            chain_cookies[key] = dict(tmp.s.cookies)
            print("OK")

    # ── Phase 2: Fetch all stores in parallel ──────────────────────────────
    def _fetch_row(row: dict) -> tuple:
        """Worker: fetch one store. Returns (result | None, log_lines, error | None)."""
        chain     = row["רשת"]
        store     = row["סניף"]
        feed_type = row["סוג_פיד"]
        log: list[str] = [f"\n=== {chain} / {store} ({feed_type}) ==="]

        builder = FETCHERS.get(feed_type)
        if not builder:
            msg = f"Unknown feed type: {feed_type}"
            log.append(f"  WARNING: {msg}")
            return None, log, {"chain": chain, "store": store, "error": msg}

        try:
            key     = (row.get("portal_url", "").rstrip("/"), row.get("משתמש", ""))
            cookies = chain_cookies.get(key, {})
            fetcher = builder(row, prefetched_cookies=cookies)
            data    = fetcher.fetch(log=log)
            if data is None:
                msg = "no price file found (warning only)"
                log.append(f"  WARNING: {msg}")
                return None, log, {"chain": chain, "store": store, "error": msg}
            result = {
                "chain":     chain,
                "store":     store,
                "feedType":  feed_type,
                "fetchTime": datetime.now().strftime("%d/%m/%Y %H:%M"),
                **data,
            }
            return result, log, None
        except Exception as exc:
            msg = str(exc)
            log.append(f"  ERROR: {msg}")
            return None, log, {"chain": chain, "store": store, "error": msg}

    print(f"\nFetching {len(rows)} store(s) — up to {MAX_WORKERS} in parallel...")
    t0 = datetime.now()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(_fetch_row, row) for row in rows]
    # futures complete in background; we collect results in original CSV order below

    results = []
    errors  = []
    for future in futures:
        result, log, error = future.result()
        for line in log:
            print(line)
        if result:
            results.append(result)
        if error:
            errors.append(error)

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\nCompleted in {elapsed:.1f}s")

    output = {
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
        "storeCount":  len(results),
        "stores":      results,
        **({"errors": errors} if errors else {}),
    }

    output_json = json.dumps(output, ensure_ascii=False, indent=2)
    OUTPUT_FILE.write_text(output_json, encoding="utf-8")
    print(f"Saved {len(results)} store(s) → {OUTPUT_FILE}")
    if errors:
        print(f"WARNING: {len(errors)} store(s) had issues — check errors[] in JSON", file=sys.stderr)

    # --- POST to Make.com webhook ---
    WEBHOOK_URL = "https://hook.eu1.make.com/oi6u7w14igpl7otyxvisryajg1x4dqt4"
    try:
        resp = requests.post(
            WEBHOOK_URL,
            data=output_json.encode("utf-8"),
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        print(f"Webhook → {resp.status_code} {resp.text[:120]}")
    except Exception as exc:
        print(f"WARNING: webhook failed — {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
