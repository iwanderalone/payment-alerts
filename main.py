import email
import html
import imaplib
import json
import logging
import os
import pathlib
import re
import socket
import time
from datetime import datetime, timedelta
from email.header import decode_header
from email.utils import parseaddr
from logging.handlers import TimedRotatingFileHandler

import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# --- SETUP PATHS & ENV ---
script_dir = pathlib.Path(__file__).parent.absolute()
env_path = script_dir / ".env"
load_dotenv(env_path)

PROCESSED_FILE = script_dir / "processed_emails.json"
PROCESSED_RETENTION_DAYS = int(os.getenv("PROCESSED_RETENTION_DAYS", "14"))

# --- CONFIG ---
IMAP_SERVER = os.getenv("IMAP_SERVER", "")
IMAP_PORT = int(os.getenv("IMAP_PORT", "993"))
IMAP_TIMEOUT = int(os.getenv("IMAP_TIMEOUT", "30"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "3600")) 
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
MAX_BODY_CHARS = int(os.getenv("MAX_BODY_CHARS", "800"))

EMAILS = [x.strip() for x in os.getenv("EMAILS", "").split(",") if x.strip()]
PASSWORDS = [x.strip() for x in os.getenv("PASSWORDS", "").split(",") if x.strip()]
COMPANY_NAMES = [x.strip() for x in os.getenv("COMPANY_NAMES", "").split(",") if x.strip()]

_raw_chat_ids = [x.strip() for x in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if x.strip()]
_raw_tags = [x.strip() for x in os.getenv("TELEGRAM_TAGS", "").split(",")]
_raw_allowed_senders = [x.strip() for x in os.getenv("ALLOWED_SENDERS", "").split(",")]
_raw_company_keywords = [x.strip() for x in os.getenv("COMPANY_KEYWORDS", "").split(",")]

START_DATE_ENV = os.getenv("START_DATE", "")

# Global search phrases (RU/EN)
GLOBAL_ALERT_PHRASES = [
    "пора пополнить баланс", "доступ к сервисам ограничен", "истекает",
    "закончатся средства", "пополните баланс", "услуга приостановлена",
    "доступ заблокирован", "срок действия истекает", "оплатите",
    "задолженность", "не удалось списать", "требуется оплата",
    "payment due", "service suspended", "account suspended", "expires",
    "expiring soon", "low balance", "top up your balance", "billing issue",
    "invoice overdue", "subscription expired", "credit card was declined"
]

# --- LOGGING ---
log_file = script_dir / "payment_bot.log"
handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7, encoding="utf-8")
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger = logging.getLogger("payment_monitor")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler())

# --- HELPERS ---
def resolve_since_date() -> str:
    """ Formats date for IMAP: DD-Mon-YYYY """
    if START_DATE_ENV:
        try:
            dt = datetime.strptime(START_DATE_ENV, "%Y-%m-%d")
            return dt.strftime("%d-%b-%Y")
        except ValueError:
            logger.warning(f"Invalid START_DATE format: {START_DATE_ENV}. Use YYYY-MM-DD. Falling back to 24h.")
    return (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")

def clean_html_content(raw_html: str) -> str:
    try:
        soup = BeautifulSoup(raw_html, "lxml")
        for element in soup(["script", "style", "head", "title", "meta", "[document]"]):
            element.decompose()
        text = soup.get_text(separator=' ')
        text = html.unescape(text)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"BS4 failed, using regex fallback: {e}")
        text = re.sub(r"<(style|script)[^>]*>.*?</\1>", " ", raw_html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        return " ".join(text.split())

def extract_clean_text(msg) -> str:
    text_parts = []
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdisp = str(part.get("Content-Disposition", ""))
            if "attachment" in cdisp: continue
            
            if ctype == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    text_parts.append(payload.decode(part.get_content_charset() or "utf-8", errors="replace"))
            elif ctype == "text/html" and not text_parts:
                payload = part.get_payload(decode=True)
                if payload:
                    text_parts.append(clean_html_content(payload.decode(part.get_content_charset() or "utf-8", errors="replace")))
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            content = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
            if msg.get_content_type() == "text/html":
                content = clean_html_content(content)
            text_parts.append(content)

    full_text = "\n".join(text_parts).strip()
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    if len(full_text) > MAX_BODY_CHARS:
        full_text = full_text[:MAX_BODY_CHARS].rstrip() + "..."
    return full_text or "(empty body)"

def decode_header_value(value: str) -> str:
    if not value: return ""
    decoded = []
    for val, enc in decode_header(value):
        if isinstance(val, bytes):
            decoded.append(val.decode(enc or "utf-8", errors="replace"))
        else:
            decoded.append(str(val))
    return "".join(decoded)

# --- TRACKING ---
def load_processed_ids() -> dict:
    if not PROCESSED_FILE.exists(): return {}
    try:
        with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            cutoff = (datetime.now() - timedelta(days=PROCESSED_RETENTION_DAYS)).isoformat()
            return {k: v for k, v in data.items() if v > cutoff}
    except: return {}

def save_processed_ids(data: dict):
    with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

# --- CORE LOGIC ---
def check_mail():
    processed = load_processed_ids()
    since_date = resolve_since_date()
    
    # Rebuild configs to ensure we use current env
    for i, email_addr in enumerate(EMAILS):
        try:
            # Build current company config
            dest = {"chat_id": _raw_chat_ids[i], "topic_id": None}
            if ":" in dest["chat_id"]:
                cid, tid = dest["chat_id"].split(":", 1)
                dest = {"chat_id": cid.strip(), "topic_id": int(tid.strip())}
            
            tags = [f"@{u}" for u in _raw_tags[i].split("+") if u.strip()] if i < len(_raw_tags) else []
            senders = [s.lower() for s in _raw_allowed_senders[i].split("+") if s.strip()] if i < len(_raw_allowed_senders) else []
            extra_k = [k.lower() for k in _raw_company_keywords[i].split("+") if k.strip()] if i < len(_raw_company_keywords) else []
            keywords = list(set(GLOBAL_ALERT_PHRASES + extra_k))
            
            company_name = COMPANY_NAMES[i] if i < len(COMPANY_NAMES) else email_addr
            password = PASSWORDS[i]

            logger.info(f"Checking {company_name} ({email_addr})...")
            
            mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
            mail.login(email_addr, password)
            mail.select("INBOX", readonly=True)

            # FIX: Use tuple arguments for SEARCH to avoid syntax errors
            status, messages = mail.search(None, 'SINCE', since_date)
            
            if status == "OK" and messages[0]:
                for num in messages[0].split():
                    status, data = mail.fetch(num, "(RFC822)")
                    if status != "OK": continue

                    msg = email.message_from_bytes(data[0][1])
                    msg_id = msg.get("Message-ID", f"{email_addr}-{num.decode()}")
                    if msg_id in processed: continue

                    subject = decode_header_value(msg.get("Subject"))
                    from_raw = decode_header_value(msg.get("From"))
                    _, sender_email = parseaddr(from_raw.lower())

                    if senders and not any(s in sender_email for s in senders):
                        processed[msg_id] = datetime.now().isoformat()
                        continue

                    body = extract_clean_text(msg)
                    haystack = f"{subject} {body}".lower()
                    
                    if any(k.lower() in haystack for k in keywords):
                        send_alert(company_name, from_raw, subject, body, dest, tags)
                    
                    processed[msg_id] = datetime.now().isoformat()

            mail.logout()
            save_processed_ids(processed)
            time.sleep(1)

        except Exception as e:
            logger.error(f"Error checking {email_addr}: {e}")

def send_alert(name, sender, subject, body, dest, tags):
    tag_line = " ".join(tags)
    msg_text = (
        f"💳 *Billing Alert*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏢 *Company:* {name}\n"
        f"📧 *From:* `{sender}`\n"
        f"📌 *Subject:* {subject}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 *Email Content:*\n{body}\n"
    )
    if tag_line: msg_text += f"\n🔔 {tag_line}"

    payload = {
        "chat_id": dest["chat_id"],
        "text": msg_text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    if dest["topic_id"]: payload["message_thread_id"] = dest["topic_id"]

    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload, timeout=15)
        r.raise_for_status()
        logger.info(f"Alert sent for {name}")
    except Exception as e:
        logger.error(f"Telegram failed: {e}")

if __name__ == "__main__":
    logger.info(f"Bot started. Polling interval: {CHECK_INTERVAL}s")
    while True:
        check_mail()
        logger.info("Cycle complete. Waiting...")
        time.sleep(CHECK_INTERVAL)