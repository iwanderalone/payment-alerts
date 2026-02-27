import imaplib
import email
from email.header import decode_header
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pathlib
import socket

# --- SETUP PATHS & ENV ---
script_dir = pathlib.Path(__file__).parent.absolute()
env_path = script_dir / ".env"
load_dotenv(env_path)

# Processed emails tracking (JSON for richer data + auto-cleanup)
PROCESSED_FILE = script_dir / "processed_emails.json"
PROCESSED_RETENTION_DAYS = 7  # Auto-delete entries older than this

# --- CONFIG ---
IMAP_SERVER = os.getenv("IMAP_SERVER")
IMAP_PORT = int(os.getenv("IMAP_PORT", "993"))
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

EMAILS = os.getenv("EMAILS", "").split(",")
PASSWORDS = os.getenv("PASSWORDS", "").split(",")
COMPANY_NAMES = os.getenv("COMPANY_NAMES", "").split(",")
# Per-company Telegram destinations
# Format in .env: "chatid:topicid" or just "chatid" (no topic = General)
# Example: TELEGRAM_CHAT_IDS=-1001234567890:15,-1009876543210:3,-1005551234567
_raw_chat_ids = os.getenv("TELEGRAM_CHAT_IDS", "").split(",")
TELEGRAM_DESTINATIONS = []
for raw in _raw_chat_ids:
    raw = raw.strip()
    if ":" in raw:
        chat_id, topic_id = raw.split(":", 1)
        TELEGRAM_DESTINATIONS.append({"chat_id": chat_id.strip(), "topic_id": int(topic_id.strip())})
    else:
        TELEGRAM_DESTINATIONS.append({"chat_id": raw, "topic_id": None})

# Per-company tags: "user1+user2" per company, split into lists
# Result: [["wanderalone"], ["wanderalone", "ruptly_admin"], ["nomad_it"]]
_raw_tags = os.getenv("TELEGRAM_TAGS", "").split(",")
TELEGRAM_TAGS = [t.split("+") for t in _raw_tags]

# Build a list of company config dicts for easy access
COMPANIES = []
for i in range(len(EMAILS)):
    dest = TELEGRAM_DESTINATIONS[i] if i < len(TELEGRAM_DESTINATIONS) else {"chat_id": "", "topic_id": None}
    COMPANIES.append({
        "email": EMAILS[i].strip(),
        "password": PASSWORDS[i].strip(),
        "name": COMPANY_NAMES[i].strip(),
        "chat_id": dest["chat_id"],
        "topic_id": dest["topic_id"],
        "tags": [f"@{u.strip()}" for u in TELEGRAM_TAGS[i]] if i < len(TELEGRAM_TAGS) else [],
    })

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "120"))  # seconds between checks
IMAP_TIMEOUT = int(os.getenv("IMAP_TIMEOUT", "30"))  # socket timeout for IMAP

# ============================================================
# TARGET PHRASES — Categorized Alerts
# ============================================================
# Each category has:
#   - "emoji": icon for the Telegram message
#   - "label": human-readable category name
#   - "phrases": list of lowercase trigger phrases to match in subject
#
# HOW TO ADD MORE:
#   1. To add a phrase to an existing category, just append to its "phrases" list.
#   2. To create a new category, add a new dict to ALERT_CATEGORIES.
#   3. An email can match multiple categories — each sends its own alert.
#
ALERT_CATEGORIES = [
    {
        "emoji": "💸",
        "label": "Payment Due",
        "phrases": [
            "закончатся средства",
            "пополните баланс",
            "к сервисам ограничен",
            # Add more payment-related phrases here:
            # "задолженность",
            # "оплатите счёт",
        ],
    },
    {
        "emoji": "🔴",
        "label": "Service Suspended",
        "phrases": [
            "услуга приостановлена",
            "доступ заблокирован",
            # Add more suspension phrases here:
        ],
    },
    {
        "emoji": "⚠️",
        "label": "Expiring Soon",
        "phrases": [
            "срок действия истекает",
            "домен истекает",
            "сертификат истекает",
            "пора пополнить баланс",
            # Add more expiry phrases here:
        ],
    },
]

# --- LOGGING ---
log_file = script_dir / "payment_bot.log"
handler = TimedRotatingFileHandler(
    log_file, when="midnight", interval=1, backupCount=7, encoding="utf-8"
)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger = logging.getLogger("payment_monitor")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Also log to console so you can see output when running interactively
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console)


# --- PROCESSED EMAIL TRACKING (with auto-cleanup) ---

def load_processed_ids() -> dict:
    """Returns {msg_id: iso_timestamp} dict. Auto-cleans old entries."""
    if not PROCESSED_FILE.exists():
        return {}
    try:
        with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, ValueError):
        logger.warning("Corrupted processed file, resetting.")
        return {}

    # Prune entries older than retention period
    cutoff = (datetime.now() - timedelta(days=PROCESSED_RETENTION_DAYS)).isoformat()
    pruned = {k: v for k, v in data.items() if v > cutoff}
    if len(pruned) < len(data):
        logger.info(f"Pruned {len(data) - len(pruned)} old processed entries.")
        _save_processed_ids(pruned)
    return pruned


def _save_processed_ids(data: dict):
    tmp = PROCESSED_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    tmp.replace(PROCESSED_FILE)  # atomic on most OS


def mark_processed(processed: dict, msg_id: str):
    processed[msg_id] = datetime.now().isoformat()
    _save_processed_ids(processed)


# --- TELEGRAM ---

def send_telegram_alert(company: dict, subject: str, from_addr: str, category: dict, retries: int = 3):
    """Send alert to the company's specific chat, tagging the right people."""
    tag_line = " ".join(company["tags"])
    message_text = (
        f"{category['emoji']} {category['label']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏢  {company['name']}\n"
        f"📧  {from_addr}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📋  {subject}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{tag_line}"
    )
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": company["chat_id"], "text": message_text}
    
    # Send to specific topic/thread if configured
    if company.get("topic_id") is not None:
        payload["message_thread_id"] = company["topic_id"]

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, data=payload, timeout=15)
            if resp.status_code == 429:
                # Telegram rate limit — respect Retry-After header
                retry_after = int(resp.headers.get("Retry-After", 5))
                logger.warning(f"Telegram rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            logger.info(f"Telegram alert sent: [{category['label']}] {company['name']} → chat {company['chat_id']}")
            return True
        except requests.RequestException as e:
            wait = 2 ** attempt
            logger.error(f"Telegram send failed (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(wait)

    logger.error(f"Telegram alert FAILED after {retries} attempts for {company['name']}")
    return False


# --- IMAP HELPERS ---

def connect_imap(email_addr: str, password: str) -> imaplib.IMAP4_SSL:
    """Connect with explicit timeout and error handling."""
    socket.setdefaulttimeout(IMAP_TIMEOUT)
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    mail.login(email_addr, password)
    return mail


def safe_logout(mail):
    try:
        mail.logout()
    except Exception:
        pass


def decode_subject(msg) -> str:
    """Safely decode email subject."""
    raw = msg.get("Subject", "")
    if not raw:
        return ""
    try:
        parts = decode_header(raw)
        decoded = []
        for part, enc in parts:
            if isinstance(part, bytes):
                decoded.append(part.decode(enc or "utf-8", errors="replace"))
            else:
                decoded.append(part)
        return " ".join(decoded)
    except Exception as e:
        logger.warning(f"Subject decode error: {e}")
        return str(raw)


def decode_from(msg) -> str:
    """Extract and decode the From address."""
    raw = msg.get("From", "")
    if not raw:
        return "Unknown"
    try:
        parts = decode_header(raw)
        decoded = []
        for part, enc in parts:
            if isinstance(part, bytes):
                decoded.append(part.decode(enc or "utf-8", errors="replace"))
            else:
                decoded.append(part)
        return " ".join(decoded)
    except Exception:
        return str(raw)


def get_message_id(msg, email_addr: str, num: bytes) -> str:
    """Get a stable unique ID for deduplication."""
    msg_id = msg.get("Message-ID", "")
    if not msg_id:
        # Fallback: use date + from + subject hash for stability
        msg_id = f"{email_addr}-{num.decode()}-{msg.get('Date', '')}"
    return msg_id.strip().replace("<", "").replace(">", "")


def match_categories(subject: str) -> list[dict]:
    """Return all categories whose phrases match the subject. Case-insensitive."""
    subject_lower = subject.lower()
    return [cat for cat in ALERT_CATEGORIES if any(p.lower() in subject_lower for p in cat["phrases"])]


# --- MAIN LOGIC ---

def check_mail():
    processed = load_processed_ids()
    since_date = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")

    for company in COMPANIES:
        mail = None
        try:
            logger.info(f"Checking {company['name']} ({company['email']})...")
            mail = connect_imap(company["email"], company["password"])
            mail.select("inbox")

            status, messages = mail.search(None, f'(SINCE "{since_date}")')
            if status != "OK" or not messages[0]:
                logger.info(f"  No recent messages for {company['name']}.")
                safe_logout(mail)
                continue

            msg_nums = messages[0].split()
            logger.info(f"  Found {len(msg_nums)} message(s) to scan.")

            for num in msg_nums:
                try:
                    status, data = mail.fetch(num, "(RFC822)")
                    if status != "OK":
                        continue

                    msg = email.message_from_bytes(data[0][1])
                    msg_id = get_message_id(msg, company["email"], num)

                    if msg_id in processed:
                        continue

                    subject = decode_subject(msg)
                    from_addr = decode_from(msg)
                    matched = match_categories(subject)

                    if matched:
                        for cat in matched:
                            logger.info(f"  Trigger [{cat['label']}]: {subject} (from: {from_addr})")
                            send_telegram_alert(company, subject, from_addr, cat)
                            time.sleep(0.5)

                        mark_processed(processed, msg_id)

                except Exception as e:
                    logger.error(f"  Error processing message {num}: {e}")

            safe_logout(mail)

        except (imaplib.IMAP4.error, socket.timeout, OSError) as e:
            logger.error(f"IMAP connection error for {company['email']}: {e}")
            safe_logout(mail)
        except Exception as e:
            logger.error(f"Unexpected error for {company['email']}: {e}")
            safe_logout(mail)


def run():
    logger.info("=" * 50)
    logger.info("Starting Payment Monitor Bot")
    logger.info(f"  Monitoring {len(COMPANIES)} mailbox(es)")
    logger.info(f"  Check interval: {CHECK_INTERVAL}s")
    logger.info(f"  Alert categories: {[c['label'] for c in ALERT_CATEGORIES]}")
    for c in COMPANIES:
        topic = f", topic={c['topic_id']}" if c.get('topic_id') is not None else ""
        logger.info(f"    {c['name']}: chat={c['chat_id']}{topic}, tags={c['tags']}")
    logger.info("=" * 50)

    consecutive_failures = 0
    max_backoff = 300  # 5 min max wait on repeated failures

    while True:
        try:
            check_mail()
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            backoff = min(CHECK_INTERVAL * consecutive_failures, max_backoff)
            logger.error(f"Main loop error (failure #{consecutive_failures}): {e}")
            logger.info(f"Backing off for {backoff}s before retry...")
            time.sleep(backoff)
            continue

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")