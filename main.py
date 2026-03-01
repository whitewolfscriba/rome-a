import os
import re
import json
import time
import hashlib
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from dateutil.parser import isoparse

# -----------------------------
# Config / Defaults
# -----------------------------
PREFS_PATH = Path("config/preferences.json")
KW_PATH = Path("config/keywords.json")
STATE_DB = Path("storage/listings.db")

DEFAULT_PREFS = {
    "max_price_default": 900,
    "max_price_hard": 1000,
    "max_over_900_per_run": 4,
    "min_send": 10,
    "max_send": 20,
    "max_age_days": 14,
    "timezone": "Europe/Rome",
    "run_hours_local": [9, 21],
}

DEFAULT_KW = {
    "include_types": ["monolocale", "bilocale", "appartamento", "loft", "casa", "miniappartamento"],
    "exclude_room_words": ["stanza", "camera", "posto letto", "room", "single room"],
    "agency_blacklist": ["agenzia", "immobiliare", "provvigione", "mediazione", "commissione", "tecnocasa", "tempocasa", "frimm"],
    "too_rigid_blacklist": ["solo contratto a tempo indeterminato", "indeterminato obbligatorio"],
    "furnished_positive": ["arredato", "ammobiliato", "completamente arredato", "cucina attrezzata"],
    "condition_positive": ["ristrutturato", "ottimo stato", "buone condizioni", "luminoso"],
    "balcony_positive": ["balcone", "terrazzo"],
}

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}

# -----------------------------
# Helpers
# -----------------------------
def load_json(path: Path, fallback: dict) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback

def norm(s: str) -> str:
    return (s or "").strip()

def norm_lower(s: str) -> str:
    return norm(s).lower()

def contains_any(text: str, words: list[str]) -> bool:
    t = norm_lower(text)
    return any(w.lower() in t for w in words)

def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def split_urls(raw: str) -> list[str]:
    if not raw:
        return []
    raw = raw.replace(",", "\n")
    return [u.strip() for u in raw.splitlines() if u.strip()]

def parse_price_eur(text: str) -> int | None:
    """
    Prova a trovare un prezzo in euro da testo: "900 €", "1.000€/mese", ecc.
    """
    t = norm(text)
    if not t:
        return None
    # prende numeri tipo 900 / 1.000 / 1000
    m = re.search(r"(\d{1,3}(?:\.\d{3})+|\d{3,4})\s*€", t)
    if not m:
        m = re.search(r"€\s*(\d{1,3}(?:\.\d{3})+|\d{3,4})", t)
    if not m:
        return None
    n = m.group(1).replace(".", "")
    try:
        return int(n)
    except Exception:
        return None

def is_recent_enough(published_iso: str | None, max_age_days: int) -> tuple[bool, bool]:
    """
    returns (keep, known)
    - keep True se data ignota O <= max_age_days
    - known True se la data è stata parse-ata
    """
    if not published_iso:
        return True, False
    try:
        dt = isoparse(published_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - dt).days
        return (age_days <= max_age_days), True
    except Exception:
        return True, False

def should_run_now(prefs: dict) -> bool:
    tz = ZoneInfo(prefs["timezone"])
    now_local = datetime.now(tz)
    # finestra 10 minuti per evitare doppi run
    return (now_local.hour in prefs["run_hours_local"]) and (now_local.minute < 10)

# -----------------------------
# DB (SQLite)
# -----------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
  id_hash TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  title TEXT,
  price_eur INTEGER,
  zone TEXT,
  source TEXT,
  published_at TEXT,
  published_at_known INTEGER DEFAULT 0,
  score INTEGER DEFAULT 0,
  sent_count INTEGER DEFAULT 0,
  last_sent_at TEXT
);
"""

def db_init():
    STATE_DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(STATE_DB) as con:
        con.execute(SCHEMA)
        con.commit()

def db_upsert(item: dict):
    with sqlite3.connect(STATE_DB) as con:
        con.execute("""
        INSERT INTO listings (id_hash, url, title, price_eur, zone, source, published_at, published_at_known, score, sent_count, last_sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT sent_count FROM listings WHERE id_hash=?), 0),
                    (SELECT last_sent_at FROM listings WHERE id_hash=?))
        ON CONFLICT(id_hash) DO UPDATE SET
          url=excluded.url,
          title=excluded.title,
          price_eur=excluded.price_eur,
          zone=excluded.zone,
          source=excluded.source,
          published_at=excluded.published_at,
          published_at_known=excluded.published_at_known,
          score=excluded.score
        """, (
            item["id_hash"], item["url"], item.get("title"), item.get("price_eur"),
            item.get("zone"), item.get("source"), item.get("published_at"),
            1 if item.get("published_at_known") else 0,
            item.get("score", 0),
            item["id_hash"], item["id_hash"]
        ))
        con.commit()

def db_fetch_candidates(limit: int = 500) -> list[dict]:
    with sqlite3.connect(STATE_DB) as con:
        cur = con.execute("""
        SELECT id_hash, url, title, price_eur, zone, source, published_at, published_at_known, score, sent_count, last_sent_at
        FROM listings
        ORDER BY score DESC
        LIMIT ?
        """, (limit,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

def db_sent_count(id_hash: str) -> int:
    with sqlite3.connect(STATE_DB) as con:
        cur = con.execute("SELECT sent_count FROM listings WHERE id_hash=?", (id_hash,))
        row = cur.fetchone()
        return int(row[0]) if row else 0

def db_mark_sent(id_hash: str):
    now_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(STATE_DB) as con:
        con.execute("""
        UPDATE listings
        SET sent_count = sent_count + 1,
            last_sent_at = ?
        WHERE id_hash = ?
        """, (now_iso, id_hash))
        con.commit()

# -----------------------------
# Ranking / formatting
# -----------------------------
def score_item(item: dict, prefs: dict, kw: dict) -> int:
    title = item.get("title", "")
    desc = item.get("description", "")
    zone = item.get("zone") or ""
    price = item.get("price_eur")
    text = f"{title}\n{desc}"

    score = 0

    # prezzo
    if isinstance(price, int):
        if price <= prefs["max_price_default"]:
            score += 25
        elif price <= prefs["max_price_hard"]:
            score += 10
        else:
            score -= 60
    else:
        score -= 4  # prezzo non trovato: leggermente meno priorità

    # arredato / condizioni
    if contains_any(text, kw["furnished_positive"]):
        score += 12
    if contains_any(text, kw["condition_positive"]):
        score += 6
    if contains_any(text, kw["balcony_positive"]):
        score += 4

    # agenzie / rigidità
    if contains_any(text, kw["agency_blacklist"]):
        score -= 80
    if contains_any(text, kw["too_rigid_blacklist"]):
        score -= 40

    # data pubblicazione
    if item.get("published_at_known"):
        score += 8
    else:
        score -= 5  # data non trovata: non scarto, solo meno priorità

    # zona (semplice)
    z = zone.lower()
    if "trastevere" in z:
        score += 20
    if "marconi" in z:
        score += 16
    if any(k in z for k in ["ostiense", "testaccio", "san paolo", "garbatella", "monteverde", "portuense", "piramide"]):
        score += 10

    # qualità descrizione
    if len(norm_lower(desc)) > 200:
        score += 3

    return score

def human_age(published_iso: str | None) -> str:
    if not published_iso:
        return "Data non trovata"
    try:
        dt = isoparse(published_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        days = (datetime.now(timezone.utc) - dt).days
        return f"{dt.date()} ({days} giorni fa)"
    except Exception:
        return "Data non trovata"

def build_pros_cons(item: dict, kw: dict) -> tuple[list[str], list[str]]:
    text = (item.get("title","") + "\n" + item.get("description","")).lower()
    pros, cons = [], []

    if any(w in text for w in kw["furnished_positive"]):
        pros.append("Arredato")
    if any(w in text for w in kw["condition_positive"]):
        pros.append("Buone condizioni")
    if any(w in text for w in kw["balcony_positive"]):
        pros.append("Balcone/terrazzo")

    if any(w in text for w in kw["too_rigid_blacklist"]):
        cons.append("Richiesta troppo rigida")
    if any(w in text for w in kw["agency_blacklist"]):
        cons.append("Possibile agenzia")
    if not item.get("published_at_known"):
        cons.append("Data non trovata")

    return pros[:3], cons[:3]

def format_card(item: dict, seen: bool) -> str:
    title = item.get("title") or "(senza titolo)"
    zone = item.get("zone") or "Zona non indicata"
    price = item.get("price_eur")
    price_txt = f"€{price}" if isinstance(price, int) else "Prezzo n/d"

    head = f"{'VISTO • ' if seen else ''}{title} — {price_txt}\n📍 {zone}"
    pub = f"🗓 Pubblicato: {human_age(item.get('published_at'))}"

    filters = []
    filters.append("data ok" if item.get("published_at_known") else "data non trovata (priorità -)")
    if isinstance(price, int):
        filters.append("<=1000" if price <= 1000 else ">1000")
    filt = "🔎 Filtri: " + " • ".join(filters)

    pros = item.get("pros") or []
    cons = item.get("cons") or []

    pro_line = "✅ Pro: " + (" | ".join(pros) if pros else "—")
    con_line = "⚠️ Contro: " + (" | ".join(cons) if cons else "—")

    link = f"🔗 {item.get('url')}"
    return "\n".join([head, pub, filt, pro_line, con_line, link])

# -----------------------------
# Notify (Telegram + Email)
# -----------------------------
def send_telegram(token: str, chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": False
    }, timeout=30)
    r.raise_for_status()

def send_email(gmail_user: str, gmail_app_pass: str, to_email: str, subject: str, body: str):
    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = gmail_user
    msg["To"] = to_email

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_app_pass)
        server.sendmail(gmail_user, [to_email], msg.as_string())

# -----------------------------
# Scrapers (multi-sito, leggeri)
# -----------------------------
def fetch_html(url: str) -> str:
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return r.text

def scrape_subito(list_url: str, max_items: int = 120) -> list[dict]:
    html = fetch_html(list_url)
    soup = BeautifulSoup(html, "html.parser")

    out = []
    # Subito: prendiamo link che puntano ad annunci (euristica)
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if "subito.it" not in href:
            continue
        # evita link inutili
        if "/annunci-" in href and "/dettaglio/" in href:
            pass
        title = a.get_text(" ", strip=True) or ""
        if len(title) < 8:
            continue
        url = href.split("?")[0]
        out.append({
            "id_hash": sha(url),
            "url": url,
            "title": title,
            "price_eur": parse_price_eur(title),
            "zone": "Roma",  # spesso in pagina lista non è stabile: fallback
            "published_at": None,
            "published_at_known": False,
            "source": "subito",
            "description": ""
        })
        if len(out) >= max_items:
            break

    # dedup interno
    uniq = {}
    for it in out:
        uniq[it["id_hash"]] = it
    return list(uniq.values())

def scrape_immobiliare(list_url: str, max_items: int = 120) -> list[dict]:
    html = fetch_html(list_url)
    soup = BeautifulSoup(html, "html.parser")

    out = []
    # Immobiliare: nelle liste troviamo link /annunci/ID
    for a in soup.select('a[href^="/annunci/"], a[href*="/annunci/"]'):
        href = a.get("href") or ""
        if "/annunci/" not in href:
            continue
        if href.startswith("/"):
            url = "https://www.immobiliare.it" + href
        else:
            url = href
        title = a.get_text(" ", strip=True) or ""
        if len(title) < 6:
            title = "Annuncio Immobiliare"
        out.append({
            "id_hash": sha(url),
            "url": url.split("?")[0],
            "title": title,
            "price_eur": None,  # lo tenteremo dalla pagina dettaglio se la apriamo
            "zone": "Roma",
            "published_at": None,
            "published_at_known": False,
            "source": "immobiliare",
            "description": ""
        })
        if len(out) >= max_items:
            break

    # dedup
    uniq = {}
    for it in out:
        uniq[it["id_hash"]] = it
    return list(uniq.values())

def scrape_bakeca(list_url: str, max_items: int = 120) -> list[dict]:
    html = fetch_html(list_url)
    soup = BeautifulSoup(html, "html.parser")
    out = []

    # Bakeca: spesso ha link /dettaglio/offro-casa/...
    for a in soup.select('a[href*="/dettaglio/offro-casa/"]'):
        href = a.get("href") or ""
        if not href:
            continue
        url = href if href.startswith("http") else "https://roma.bakeca.it" + href
        title = a.get_text(" ", strip=True) or "Annuncio Bakeca"

        # prova a pescare un prezzo dal testo vicino
        price = parse_price_eur(title)

        out.append({
            "id_hash": sha(url),
            "url": url.split("?")[0],
            "title": title,
            "price_eur": price,
            "zone": "Roma",
            "published_at": None,
            "published_at_known": False,
            "source": "bakeca",
            "description": ""
        })
        if len(out) >= max_items:
            break

    uniq = {}
    for it in out:
        uniq[it["id_hash"]] = it
    return list(uniq.values())

def scrape_idealista(list_url: str, max_items: int = 120) -> list[dict]:
    # Idealista può essere più "anti-scrape": se fallisce, non blocchiamo il bot.
    try:
        html = fetch_html(list_url)
    except Exception:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out = []

    # euristica: link che contiene "/immobile/" o simili
    for a in soup.select('a[href]'):
        href = a.get("href") or ""
        if "idealista.it" not in href and not href.startswith("/"):
            continue
        if "immobile" not in href:
            continue
        url = href if href.startswith("http") else "https://www.idealista.it" + href
        title = a.get_text(" ", strip=True) or "Annuncio Idealista"
        out.append({
            "id_hash": sha(url),
            "url": url.split("?")[0],
            "title": title,
            "price_eur": parse_price_eur(title),
            "zone": "Roma",
            "published_at": None,
            "published_at_known": False,
            "source": "idealista",
            "description": ""
        })
        if len(out) >= max_items:
            break

    uniq = {}
    for it in out:
        uniq[it["id_hash"]] = it
    return list(uniq.values())

def scrape_by_domain(url: str) -> list[dict]:
    u = url.lower()
    if "subito.it" in u:
        return scrape_subito(url)
    if "immobiliare.it" in u:
        return scrape_immobiliare(url)
    if "bakeca.it" in u:
        return scrape_bakeca(url)
    if "idealista.it" in u:
        return scrape_idealista(url)
    return []

# -----------------------------
# Main pipeline
# -----------------------------
def main():
    prefs = load_json(PREFS_PATH, DEFAULT_PREFS)
    kw = load_json(KW_PATH, DEFAULT_KW)

    db_init()

    # 1) URL list (multi-sito)
    # Metti tutti gli URL nello stesso secret: QUERY_URLS (consigliato).
    # Se hai già SUBITO_QUERY_URL, lo prendiamo comunque come fallback.
    raw_urls = os.getenv("QUERY_URLS", "").strip()
    if not raw_urls:
        raw_urls = os.getenv("SUBITO_QUERY_URL", "").strip()

    urls = split_urls(raw_urls)
    if not urls:
        print("No URLs provided. Set QUERY_URLS (or SUBITO_QUERY_URL). Exit.")
        return

    all_items: list[dict] = []

    # 2) Scrape
    for u in urls:
        try:
            items = scrape_by_domain(u)
            all_items.extend(items)
            time.sleep(0.4)  # piccolo delay, più "gentile"
        except Exception as e:
            print(f"Scrape failed for {u}: {e}")

    print(f"Scraped items: {len(all_items)}")

    # 3) Filtri + scoring + DB
    kept = 0
    for it in all_items:
        text = (it.get("title","") + "\n" + it.get("description",""))

        # NO stanze
        if contains_any(text, kw["exclude_room_words"]):
            continue

        # deve essere tipo casa/appartamento (euristica)
        if not contains_any(text, kw["include_types"]):
            continue

        # data: se c'è e > 14 giorni, scarta; se non c'è, tieni ma penalizza (ranking)
        keep, known = is_recent_enough(it.get("published_at"), prefs["max_age_days"])
        it["published_at_known"] = known
        if known and not keep:
            continue

        # prezzo: se noto e > 1000, scarta
        price = it.get("price_eur")
        if isinstance(price, int) and price > prefs["max_price_hard"]:
            continue

        it["score"] = score_item(it, prefs, kw)
        pros, cons = build_pros_cons(it, kw)
        it["pros"] = pros
        it["cons"] = cons

        db_upsert(it)
        kept += 1

    print(f"Kept items after filters: {kept}")

    # 4) Selezione invio: <=900 prima, poi (901-1000) max 4, poi riempimento
    candidates = db_fetch_candidates(limit=600)

    under_eq_900 = [c for c in candidates if isinstance(c.get("price_eur"), int) and c["price_eur"] <= prefs["max_price_default"]]
    up_to_1000 = [c for c in candidates if isinstance(c.get("price_eur"), int) and prefs["max_price_default"] < c["price_eur"] <= prefs["max_price_hard"]]
    unknown_price = [c for c in candidates if c.get("price_eur") is None]

    selected = []
    selected_ids = set()

    def pick(lst, target_len):
        nonlocal selected, selected_ids
        for x in lst:
            if x["id_hash"] in selected_ids:
                continue
            selected.append(x)
            selected_ids.add(x["id_hash"])
            if len(selected) >= target_len:
                break

    pick(under_eq_900, prefs["max_send"])

    if len(selected) < prefs["max_send"]:
        target = min(prefs["max_send"], len(selected) + prefs["max_over_900_per_run"])
        pick(up_to_1000, target)

    if len(selected) < prefs["min_send"]:
        pick(unknown_price, prefs["max_send"])

    if len(selected) < prefs["min_send"]:
        pick(candidates, prefs["max_send"])

    selected = selected[:prefs["max_send"]]

    # 5) Format + send
    cards = []
    for it in selected:
        seen = db_sent_count(it["id_hash"]) >= 1
        # "VISTO" dal secondo invio in poi
        cards.append(format_card(it, seen))

    message = "🏠 Annunci selezionati (09/21)\n\n" + "\n\n---\n\n".join(cards)

    tg_token = os.getenv("TELEGRAM_TOKEN", "")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID", "")
    if tg_token and tg_chat:
        send_telegram(tg_token, tg_chat, message)

    email_user = os.getenv("EMAIL_USER", "")
    email_pass = os.getenv("EMAIL_PASS", "")
    email_to = os.getenv("EMAIL_TO", "")
    if email_user and email_pass and email_to:
        send_email(email_user, email_pass, email_to, "Annunci casa Roma (09/21)", message)

    # 6) mark sent
    for it in selected:
        db_mark_sent(it["id_hash"])

    print(f"Sent {len(selected)} listings.")

if __name__ == "__main__":
    main()
