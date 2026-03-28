# pricetop — תיעוד פרויקט

## מה הפרויקט עושה
מושך מחירי מוצרים **טריים** (בשר, עוף, דגים, ירקות, פירות) מסניפי סופרמרקט בחדרה,
דרך פידי XML של **חוק שקיפות המחירים (2015)**.
שומר תוצאות ל-`data/prices.json`, שולח ל-Make.com webhook, ומריץ ידנית דרך GitHub Actions.

---

## פורטל publishedprices — כללי
כל הרשתות שמשתמשות בפורטל זה (Cerberus FTP Web Client):
- **URL:** `https://url.publishedprices.co.il`
- **תהליך לוגין זהה לכולן** — רק שם המשתמש שונה

### תהליך התחברות (Cerberus login flow)
1. `GET /login` — מחלץ CSRF מ-`<meta name="csrftoken" content="...">`
2. `POST /login/user` — שולח `{username, password, csrftoken, r}`
3. Cookie `cftpSID` נשמר בסשן — **חובה** לעבוד עם `requests.Session()` לאורך כל הסשן
4. `GET /file` — מחלץ CSRF **נפרד** לפעולות קבצים (CSRF שני!)
5. `POST /file/json/dir` — רשימת קבצים (DataTables v1.x params)

> ⚠️ **קריטי:** ה-CSRF לפעולות קבצים שונה מה-CSRF ללוגין. חייבים לקרוא `/file` ולחלץ אותו בנפרד.

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

## רשתות מוגדרות

### אושר עד
- **משתמש פורטל:** `osherad`
- **סיסמא:** ריק — אין סיסמא
- **Chain ID:** `7290103152017`
- **SubChain ID:** `001`

#### סניפים בחדרה
| StoreID | שם | כתובת | קוד עיר |
|---------|-----|--------|---------|
| `014` | חדרה | המסגר 22, אזה"ת הצפוני | `6500` |

> **הערה:** City code של חדרה הוא `6500` (מספר, לא שם עיר). רק סניף אחד של אושר עד בחדרה.

#### prefixes לסניף 014
| prefix | תוכן |
|--------|------|
| `PriceFull7290103152017-001-014` | מחירי כל המוצרים |
| `PromoFull7290103152017-001-014` | מבצעים פעילים |
| `Stores7290103152017-000` | רשימת כל סניפי הרשת |

---

### רמי לוי *(בהכנה)*
- **משתמש פורטל:** `RamiLevi`
- **סיסמא:** ריק — אין סיסמא
- **Chain ID:** עדיין לא ידוע — לבדוק בקובץ Stores לאחר לוגין
- **סניפים בחדרה:** עדיין לא מופו
- **סטטוס:** לא מומש. `ShufersalFetcher` קיים בקוד כ-`NotImplementedError`

> ⚠️ הדומיין הישן `prices.rframi.co.il` כבר לא פעיל (DNS failure). הפורטל הנכון הוא `url.publishedprices.co.il` עם משתמש `RamiLevi`.

---

## פורמט קבצי ה-XML

### מוסכמת שמות קבצים
```
<Type><ChainID>-<SubChainID>-<StoreID>-<YYYYMMDD>-<HHMMSS>.gz
```
דוגמה: `PriceFull7290103152017-001-014-20260325-140501.gz`

> **חיפוש קובץ עדכני:** מסננים לפי prefix, ממיינים לפי `time` בתגובת ה-API, לוקחים האחרון.

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

## סינון מוצרים טריים
- **regex:** `(?<!\S)טרי(?!\S)` — word boundary לעברית
- מונע false-positives כמו "אטריות", "פטריות"
- מספר מוצרים טריים טיפוסי: **~116 מתוך ~6900** (אושר עד 014)
- מוצרים עם מבצע פעיל: **~14**
- מבצע פעיל = `PromotionStartDateTime <= now <= PromotionEndDateTime`
- ClubID=0 = מבצע לכולם; ClubID!=0 = מועדון בלבד (לשקול סינון)

---

## פלט — data/prices.json
```json
{
  "generatedAt": "2026-03-25T14:05:01",
  "storeCount": 1,
  "stores": [
    {
      "chain": "אושר עד",
      "store": "חדרה - המסגר 22",
      "feedType": "publishedprices",
      "fetchTime": "25/03/2026 14:05",
      "sourceFile": "PriceFull...gz",
      "promoFile": "PromoFull...gz",
      "products": [
        {
          "code": "...",
          "name": "...",
          "price": 12.90,
          "unitPrice": 12.90,
          "unit": "ק\"ג",
          "updated": "2026-03-25",
          "promo": {
            "id": "...",
            "description": "...",
            "discountedPrice": 9.90,
            "minQty": 1
          }
        }
      ]
    }
  ]
}
```
> `promo` יהיה `null` אם אין מבצע פעיל למוצר.

---

## Make.com Webhook
- **URL:** `https://hook.eu1.make.com/oi6u7w14igpl7otyxvisryajg1x4dqt4`
- **Method:** POST
- **Content-Type:** `application/json`
- נשלח בסוף כל ריצה מוצלחת עם תוכן `data/prices.json` המלא
- כשלון webhook — warning בלבד, לא קורס את הסקריפט

---

## SSL
- `publishedprices.co.il` — אישור SSL שאינו מוכר על ידי Python ב-Windows
- **פתרון:** `verify=False` + `urllib3.disable_warnings()` — מוגדר קבוע ב-`PublishedPricesFetcher`
- **GitHub Actions (Ubuntu):** אין בעיה, SSL עובד ישירות — אין צורך בסביבת עבודה מיוחדת

---

## קבצי הפרויקט

| קובץ | תפקיד |
|------|--------|
| `fetch_prices.py` | סקריפט ראשי — קורא CSV, מושך מחירים, שומר JSON, שולח webhook |
| `stores.csv` | רשימת סניפים + פרטי התחברות (UTF-8 עם BOM) |
| `data/prices.json` | פלט — מחירים + מבצעים |
| `.github/workflows/fetch-prices.yml` | GitHub Actions — טריגר ידני בלבד (`workflow_dispatch`) |
| `requirements.txt` | `requests>=2.31.0` |
| `osherad-fresh.ps1` | סקריפט PowerShell ישן (גיבוי — superseded על ידי Python) |
| `_get_stores.py` | כלי עזר — הורדת רשימת סניפים מהפורטל |

### stores.csv — עמודות
```
רשת, סניף, משתמש, סיסמא, סוג_פיד, portal_url, price_prefix, promo_prefix
```

> **הוספת רשת חדשה:** מוסיפים שורה ב-stores.csv. `fetch_prices.py` יטפל בה אוטומטית.

---

## GitHub Actions
- **Repo:** `https://github.com/mistralnet/pricetop`
- **Branch:** `main`
- **Actions:** `workflow_dispatch` בלבד — מופעל ידנית מ-GitHub UI
- **Artifact:** `prices-<run_id>` — מכיל `data/prices.json`
- **Auto-commit:** אחרי כל ריצה מוצלחת, מעדכן `data/prices.json` ב-repo
- **env בריצה:** `PYTHONIOENCODING=utf-8` (חובה לעברית ב-Ubuntu)

---

## הרצה מקומית (Windows)
```powershell
cd C:\Users\mistral\Downloads\pricetop
python fetch_prices.py
```

פלט צפוי:
```
=== אושר עד / חדרה - המסגר 22 (publishedprices) ===
  Price file : PriceFull7290103152017-001-014-YYYYMMDD-HHMMSS.gz  (5XX KB)
  Total items: ~6900  Fresh: ~116
  Promo file : PromoFull7290103152017-001-014-YYYYMMDD-HHMMSS.gz  (1XX KB)
  Promos on fresh products: ~14
Saved 1 store(s) → data\prices.json
Webhook → 200 Accepted
```

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
- [x] שליחה ל-Make.com webhook בסוף ריצה

## מה לא עובד / לא מומש
- [ ] רמי לוי — לוגין מוגדר (`RamiLevi`) אבל טרם מופה Chain ID וסניפים בחדרה
- [ ] שופרסל — מחלקה `ShufersalFetcher` קיימת אבל `NotImplementedError`
- [ ] סניפים נוספים של אושר עד — רק סניף 014 מוגדר ב-stores.csv
- [ ] ממשק HTML לתצוגת נתונים
- [ ] cache check — הוסר; כל ריצה מורידה מחדש
- [ ] סינון מבצעי מועדון (ClubID != 0)
