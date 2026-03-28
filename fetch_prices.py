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
    def _find_latest(self, kind: str, csrf: str) -> Optional[dict]:
        """Find latest file whose name starts with <kind> and contains -<storenum>- (e.g. -014-)."""
        r = self.s.post(
            f"{self.base}/file/json/dir",
            data={"path": "/", "iDisplayLength": 200, "iDisplayStart": 0,
                  "sSearch": self.store_tag, "sEcho": 1, "csrftoken": csrf},
            timeout=30, verify=self._verify,
        )
        r.raise_for_status()
        files = r.json().get("aaData", [])
        # startswith(kind) covers both "PriceFull" and "Price", "PromoFull" and "Promo"
        # store_tag+"-" ensures -044- appears as a segment (not a substring of another number)
        matching = [f for f in files
                    if f.get("fname", "").startswith(kind)
                    and (self.store_tag + "-") in f.get("fname", "")]
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
        latest_promo = self._find_latest("Promo", csrf)

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
# Main
# ---------------------------------------------------------------------------
def main():
    if not STORES_CSV.exists():
        print(f"ERROR: {STORES_CSV} not found", file=sys.stderr)
        sys.exit(1)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(STORES_CSV, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

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
