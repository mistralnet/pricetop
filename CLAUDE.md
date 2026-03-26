# pricetop — תיעוד פרויקט

## מה הפרויקט עושה
מושך מחירי מוצרי בשר טרי מרשתות סופרמרקט ישראליות דרך פידי XML של **חוק שקיפות המחירים (2015)**.
שומר תוצאות ל-`data/prices.json` ומריץ אוטומטית דרך GitHub Actions.

---

## פורטל אושר עד — publishedprices

### פרטי התחברות
- **URL:** `https://url.publishedprices.co.il`
- **משתמש:** `osherad`
- **סיסמא:** (ריק — אין סיסמא)
- **סוג פורטל:** Cerberus FTP Web Client

### תהליך התחברות (Cerberus login flow)
1. `GET /login` — מחלץ CSRF מ-`<meta name="csrftoken" content="...">`
2. `POST /login/user` — שולח `{username, password, csrftoken, r}`
3. Cookie `cftpSID` נשמר בסשן — חובה לעבוד עם `-SessionVariable` / `requests.Session()`
4. `GET /file` — מחלץ CSRF נפרד לפעולות קבצים
5. `POST /file/json/dir` — רשימת קבצים (DataTables v1.x params)

### רשימת קבצים — DataTables params
```
POST /file/json/dir
path=/  iDisplayLength=200  iDisplayStart=0  sSearch=<prefix>  sEcho=1  csrftoken=<csrf>
```
תגובה: `{ "aaData": [ { "fname": "...", "size": 12345, "time": "..." } ] }`

### הורדת קובץ
```
GET /file/d/<fname>
```

---

## רשת אושר עד — מזהים
- **Chain ID:** `7290103152017`
- **SubChain ID:** `001`

### סניפים בחדרה
| StoreID | שם | כתובת | קוד עיר |
|---------|-----|--------|---------|
| `014` | חדרה | המסגר 22, אזה"ת הצפוני | `6500` |

> **הערה:** רק סניף אחד של אושר עד בחדרה. City code של חדרה הוא `6500` (לא שם עיר בטקסט).

---

## פורמט קבצי ה-XML

### סוגי קבצים
| prefix | תוכן |
|--------|------|
| `PriceFull7290103152017-001-014` | מחירי כל המוצרים בסניף 014 |
| `PromoFull7290103152017-001-014` | מבצעים פעילים בסניף 014 |
| `Stores7290103152017-000` | רשימת כל הסניפים של הרשת |

### Encoding
- **קבצי מחירים ומבצעים (.gz):** gzip → UTF-16 LE עם BOM (`0xFF 0xFE`)
- **קובץ Stores (.xml):** XML רגיל, ללא gzip, UTF-16 LE
- **זיהוי BOM:**
  - `0xFF 0xFE` → UTF-16 LE
  - `0xEF 0xBB 0xBF` → UTF-8 BOM
  - אחר → UTF-8 רגיל

### מבנה XML — Price
```xml
<Root>
  <Items>
    <Item>
      <ItemCode>...</ItemCode>
      <ItemName>...</ItemName>
      <ItemPrice>...</ItemPrice>
      <UnitOfMeasurePrice>...</UnitOfMeasurePrice>
      <UnitOfMeasure>...</UnitOfMeasure>
      <Quantity>...</Quantity>
      <PriceUpdateDate>...</PriceUpdateDate>
      <ManufacturerName>...</ManufacturerName>
    </Item>
  </Items>
</Root>
```

### מבנה XML — Promo
```xml
<Root>
  <Promotions>
    <Promotion>
      <PromotionID>...</PromotionID>
      <PromotionDescription>...</PromotionDescription>
      <PromotionStartDateTime>2026-01-01T00:00:00.000</PromotionStartDateTime>
      <PromotionEndDateTime>2026-12-31T23:59:00.000</PromotionEndDateTime>
      <ClubID>0</ClubID>
      <PromotionItems>
        <PromotionItem>
          <ItemCode>...</ItemCode>
          <MinQty>...</MinQty>
          <DiscountedPrice>...</DiscountedPrice>
          <DiscountRate>...</DiscountRate>
        </PromotionItem>
      </PromotionItems>
    </Promotion>
  </Promotions>
</Root>
```

### מבנה XML — Stores
```xml
<Root>
  <SubChains>
    <SubChain>
      <SubChainID>001</SubChainID>
      <Stores>
        <Store>
          <StoreID>014</StoreID>
          <StoreName>חדרה</StoreName>
          <Address>המסגר 22 אזה"ת הצפוני</Address>
          <City>6500</City>
          <ZIPCode>3850169</ZIPCode>
        </Store>
      </Stores>
    </SubChain>
  </SubChains>
</Root>
```

---

## סינון מוצרי בשר טרי
- regex: `(?<!\S)טרי(?!\S)` — word boundary לעברית (מונע hitMatch כמו "אטריות")
- מספר מוצרים טריים טיפוסי: **~116 מתוך ~6900**
- מוצרים עם מבצע פעיל: **~14**

---

## SSL
- `publishedprices.co.il` — אישור SSL שאינו מוכר על ידי Python ב-Windows
- **פתרון:** `verify=False` + `urllib3.disable_warnings()` — מוגדר קבוע ב-`PublishedPricesFetcher`
- **GitHub Actions (Ubuntu):** אין בעיה, SSL עובד ישירות

---

## קבצי הפרויקט

| קובץ | תפקיד |
|------|--------|
| `fetch_prices.py` | סקריפט ראשי — קורא CSV, מושך מחירים, שומר JSON |
| `stores.csv` | רשימת סניפים + פרטי התחברות |
| `data/prices.json` | פלט — מחירים + מבצעים |
| `.github/workflows/fetch-prices.yml` | GitHub Actions — טריגר ידני (`workflow_dispatch`) |
| `requirements.txt` | `requests>=2.31.0` |
| `osherad-fresh.ps1` | סקריפט PowerShell ישן (גיבוי — superseded על ידי Python) |
| `_get_stores.py` | כלי עזר — הורדת רשימת סניפים מהפורטל |

### stores.csv — עמודות
```
רשת, סניף, משתמש, סיסמא, סוג_פיד, portal_url, price_prefix, promo_prefix
```

---

## GitHub
- **Repo:** `https://github.com/mistralnet/pricetop`
- **Branch:** `main`
- **Actions:** workflow_dispatch בלבד (לא scheduled)
- **Artifact:** `prices-<run_id>` — מכיל `data/prices.json`
- **Auto-commit:** אחרי כל ריצה מוצלחת, מעדכן `data/prices.json` ב-repo

---

## מה עובד
- [x] Login לפורטל Cerberus של אושר עד
- [x] איתור קובץ מחירים עדכני לפי prefix
- [x] איתור קובץ מבצעים עדכני לפי prefix
- [x] הורדה + פתיחת gzip + decode UTF-16 LE
- [x] סינון מוצרי בשר טרי
- [x] מיזוג מבצעים פעילים לפי ItemCode
- [x] שמירה ל-JSON מאוחד
- [x] GitHub Actions (workflow_dispatch)
- [x] הורדת קובץ Stores לזיהוי סניפים

## מה לא עובד / לא מומש
- [ ] Shufersal fetcher — מחלקה `ShufersalFetcher` קיימת אבל `NotImplementedError`
- [ ] רמי לוי — הדומיין `prices.rframi.co.il` לא זמין (DNS failure)
- [ ] סניפים נוספים של אושר עד — רק סניף 014 מוגדר ב-stores.csv
- [ ] ממשק HTML לתצוגת נתונים (index.html ישן עובד רק עם שופרסל)
- [ ] cache check — הוסר; כל ריצה מורידה מחדש

---

## הרצה מקומית
```bash
cd C:\Users\mistral\Downloads\pricetop
python fetch_prices.py
```
פלט צפוי:
```
=== אושר עד / חדרה - המסגר 22 (publishedprices) ===
  [DEBUG] N files visible in portal:
    PriceFull7290103152017-001-014-YYYYMMDD-HHMMSS.gz  (5XX KB)
    PromoFull7290103152017-001-014-YYYYMMDD-HHMMSS.gz  (1XX KB)
  Price file : PriceFull...gz  (5XX KB)
  Total items: ~6900  Fresh: ~116
  Promo file : PromoFull...gz  (1XX KB)
  Promos on fresh products: ~14
Saved 1 store(s) → data\prices.json
```
