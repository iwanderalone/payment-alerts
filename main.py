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

# --- SETUP PATHS & ENV ---
script_dir = pathlib.Path(__file__).parent.absolute()
env_path = script_dir / ".env"
load_dotenv(env_path)

# Processed emails tracking (JSON for richer data + auto-cleanup)
PROCESSED_FILE = script_dir / "processed_emails.json"
PROCESSED_RETENTION_DAYS = int(os.getenv("PROCESSED_RETENTION_DAYS", "14"))

# --- CONFIG ---
IMAP_SERVER = os.getenv("IMAP_SERVER", "")
IMAP_PORT = int(os.getenv("IMAP_PORT", "993"))
IMAP_TIMEOUT = int(os.getenv("IMAP_TIMEOUT", "30"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))  # avoid checking too often
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
MAX_BODY_CHARS = int(os.getenv("MAX_BODY_CHARS", "700"))

EMAILS = [x.strip() for x in os.getenv("EMAILS", "").split(",") if x.strip()]
PASSWORDS = [x.strip() for x in os.getenv("PASSWORDS", "").split(",") if x.strip()]
COMPANY_NAMES = [x.strip() for x in os.getenv("COMPANY_NAMES", "").split(",") if x.strip()]

# Telegram destinations per company.
# TELEGRAM_CHAT_IDS accepts entries like:
#   -10012345                -> normal chat/channel post
#   -10012345:15             -> specific forum topic/thread
_raw_chat_ids = [x.strip() for x in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if x.strip()]

# User tags per company, each company uses + separator:
# TELEGRAM_TAGS=admin1+admin2,finops,manager
_raw_tags = [x.strip() for x in os.getenv("TELEGRAM_TAGS", "").split(",")]

# Allowed senders per company, each company uses + separator:
# ALLOWED_SENDERS=billing@service.ru+no-reply@service.ru,alerts@stripe.com,
_raw_allowed_senders = [x.strip() for x in os.getenv("ALLOWED_SENDERS", "").split(",")]

# Optional custom keywords per company, + separator
# COMPANY_KEYWORDS=invoice+оплатите,balance+истекает,
_raw_company_keywords = [x.strip() for x in os.getenv("COMPANY_KEYWORDS", "").split(",")]

START_DATE = os.getenv("START_DATE", "")  # format: YYYY-MM-DD

GLOBAL_ALERT_PHRASES = [
    # Russian
    "пора пополнить баланс",
    "доступ к сервисам ограничен",
    "истекает",
    "закончатся средства",
    "пополните баланс",
    "услуга приостановлена",
    "доступ заблокирован",
    "срок действия истекает",
    "оплатите",
    "задолженность",
    "не удалось списать",
    "требуется оплата",
    # English
    "payment due",
    "service suspended",
    "account suspended",
    "expires",
    "expiring soon",
    "low balance",
    "top up your balance",
    "billing issue",
    "invoice overdue",
    "subscription expired",
    "credit card was declined",
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

console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console)


def parse_chat_destination(raw: str) -> dict:
    if ":" in raw:
        chat_id, topic_id = raw.split(":", 1)
        return {"chat_id": chat_id.strip(), "topic_id": int(topic_id.strip())}
    return {"chat_id": raw, "topic_id": None}


def parse_plus_list(raw: str) -> list[str]:
    return [x.strip() for x in raw.split("+") if x.strip()]


def build_company_configs() -> list[dict]:
    companies = []
    for i, email_addr in enumerate(EMAILS):
        destination = parse_chat_destination(_raw_chat_ids[i]) if i < len(_raw_chat_ids) else {"chat_id": "", "topic_id": None}
        tags = [f"@{u}" for u in parse_plus_list(_raw_tags[i])] if i < len(_raw_tags) else []
        allowed_senders = [s.lower() for s in parse_plus_list(_raw_allowed_senders[i])] if i < len(_raw_allowed_senders) else []
        extra_keywords = [k.lower() for k in parse_plus_list(_raw_company_keywords[i])] if i < len(_raw_company_keywords) else []

        companies.append(
            {
                "email": email_addr,
                "password": PASSWORDS[i] if i < len(PASSWORDS) else "",
                "name": COMPANY_NAMES[i] if i < len(COMPANY_NAMES) else email_addr,
                "chat_id": destination["chat_id"],
                "topic_id": destination["topic_id"],
                "tags": tags,
                "allowed_senders": allowed_senders,
                "keywords": list(dict.fromkeys([*GLOBAL_ALERT_PHRASES, *extra_keywords])),
            }
        )
    return companies


COMPANIES = build_company_configs()


# --- PROCESSED EMAIL TRACKING ---
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

    cutoff = (datetime.now() - timedelta(days=PROCESSED_RETENTION_DAYS)).isoformat()
    pruned = {k: v for k, v in data.items() if v > cutoff}
    if len(pruned) < len(data):
        logger.info("Pruned %s old processed entries.", len(data) - len(pruned))
        _save_processed_ids(pruned)
    return pruned


def _save_processed_ids(data: dict):
    tmp = PROCESSED_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    tmp.replace(PROCESSED_FILE)


def mark_processed(processed: dict, msg_id: str):
    processed[msg_id] = datetime.now().isoformat()
    _save_processed_ids(processed)


# --- TELEGRAM ---
def send_telegram_alert(company: dict, from_addr: str, subject: str, excerpt: str, matches: list[str], retries: int = 3):
    tag_line = " ".join(company["tags"]).strip()
    match_line = ", ".join(matches[:6])

    message_text = (
        f"🚨 Billing Alert\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏢 Company: {company['name']}\n"
        f"📧 Sender: {from_addr}\n"
        f"📌 Subject: {subject or '(no subject)'}\n"
        f"🔎 Matched: {match_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 Email text:\n{excerpt}\n"
    )
    if tag_line:
        message_text += f"━━━━━━━━━━━━━━━━━━━━\n{tag_line}"

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": company["chat_id"],
        "text": message_text,
        "disable_web_page_preview": True,
    }

    if company.get("topic_id") is not None:
        payload["message_thread_id"] = company["topic_id"]

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, data=payload, timeout=20)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                logger.warning("Telegram rate limited, waiting %ss...", retry_after)
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            logger.info("Telegram alert sent: %s -> %s", company["name"], company["chat_id"])
            return True
        except requests.RequestException as e:
            wait = 2 ** attempt
            logger.error("Telegram send failed (attempt %s/%s): %s", attempt, retries, e)
            if attempt < retries:
                time.sleep(wait)

    logger.error("Telegram alert FAILED after %s attempts for %s", retries, company["name"])
    return False


# --- IMAP HELPERS ---
def connect_imap(email_addr: str, password: str) -> imaplib.IMAP4_SSL:
    socket.setdefaulttimeout(IMAP_TIMEOUT)
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    mail.login(email_addr, password)
    return mail


def safe_logout(mail):
    if not mail:
        return
    try:
        mail.logout()
    except Exception:
        pass


def decode_mime_header(raw: str) -> str:
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
    except Exception:
        return str(raw)


def decode_subject(msg) -> str:
    return decode_mime_header(msg.get("Subject", ""))


def decode_from(msg) -> str:
    return decode_mime_header(msg.get("From", "")) or "Unknown"


def extract_sender_email(from_header: str) -> str:
    _, addr = parseaddr(from_header)
    return addr.lower().strip()


def get_message_id(msg, email_addr: str, num: bytes) -> str:
    msg_id = msg.get("Message-ID", "")
    if not msg_id:
        msg_id = f"{email_addr}-{num.decode()}-{msg.get('Date', '')}"
    return msg_id.strip().replace("<", "").replace(">", "")


def html_to_text(raw_html: str) -> str:
    text = re.sub(r"<style[\\s\\S]*?</style>", " ", raw_html, flags=re.IGNORECASE)
    text = re.sub(r"<script[\\s\\S]*?</script>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return " ".join(text.split())


def extract_email_text(msg) -> str:
    chunks = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", "")).lower()
            if "attachment" in disposition:
                continue

            payload = part.get_payload(decode=True)
            if payload is None:
                continue

            charset = part.get_content_charset() or "utf-8"
            decoded = payload.decode(charset, errors="replace")
            if content_type == "text/plain":
                chunks.append(decoded)
            elif content_type == "text/html" and not chunks:
                chunks.append(html_to_text(decoded))
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            decoded = payload.decode(charset, errors="replace")
            if msg.get_content_type() == "text/html":
                decoded = html_to_text(decoded)
            chunks.append(decoded)

    normalized = "\n".join(c.strip() for c in chunks if c.strip())
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    if len(normalized) > MAX_BODY_CHARS:
        normalized = normalized[:MAX_BODY_CHARS].rstrip() + "..."
    return normalized or "(empty text body)"


def find_matches(company: dict, subject: str, body_text: str) -> list[str]:
    haystack = f"{subject}\n{body_text}".lower()
    matches = [phrase for phrase in company["keywords"] if phrase.lower() in haystack]
    return list(dict.fromkeys(matches))


def sender_allowed(company: dict, sender_email: str) -> bool:
    allowed = company.get("allowed_senders") or []
    if not allowed:
        return True
    return any(s in sender_email for s in allowed)


def resolve_since_date() -> str:
    if START_DATE:
        try:
            dt = datetime.strptime(START_DATE, "%Y-%m-%d")
            return dt.strftime("%d-%b-%Y")
        except ValueError:
            logger.warning("Invalid START_DATE format (%s). Expected YYYY-MM-DD. Falling back to 1 day.", START_DATE)
    return (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")


# --- MAIN LOGIC ---
def check_mail():
    processed = load_processed_ids()
    since_date = resolve_since_date()

    for company in COMPANIES:
        mail = None
        try:
            logger.info("Checking %s (%s)...", company["name"], company["email"])
            mail = connect_imap(company["email"], company["password"])
            mail.select("inbox")

            status, messages = mail.search(None, f'(SINCE "{since_date}")')
            if status != "OK" or not messages or not messages[0]:
                logger.info("No matching messages for %s.", company["name"])
                continue

            msg_nums = messages[0].split()
            logger.info("Found %s message(s) for scan.", len(msg_nums))

            for num in msg_nums:
                try:
                    status, data = mail.fetch(num, "(RFC822)")
                    if status != "OK" or not data or not data[0]:
                        continue

                    msg = email.message_from_bytes(data[0][1])
                    msg_id = get_message_id(msg, company["email"], num)
                    if msg_id in processed:
                        continue

                    subject = decode_subject(msg)
                    from_addr = decode_from(msg)
                    sender_email = extract_sender_email(from_addr)
                    if not sender_allowed(company, sender_email):
                        logger.info("Skipping message from non-allowed sender: %s", sender_email)
                        mark_processed(processed, msg_id)
                        continue

                    body_text = extract_email_text(msg)
                    matches = find_matches(company, subject, body_text)

                    if matches:
                        logger.info("Trigger for %s: %s", company["name"], subject)
                        send_telegram_alert(company, from_addr, subject, body_text, matches)

                    mark_processed(processed, msg_id)

                except Exception as e:
                    logger.error("Error processing message %s: %s", num, e)

        except (imaplib.IMAP4.error, socket.timeout, OSError) as e:
            logger.error("IMAP connection error for %s: %s", company["email"], e)
        except Exception as e:
            logger.error("Unexpected error for %s: %s", company["email"], e)
        finally:
            safe_logout(mail)


def validate_config() -> bool:
    if not IMAP_SERVER or not BOT_TOKEN:
        logger.error("Missing IMAP_SERVER or TELEGRAM_BOT_TOKEN in environment.")
        return False
    if not COMPANIES:
        logger.error("No companies configured. Fill EMAILS/PASSWORDS/COMPANY_NAMES/TELEGRAM_CHAT_IDS.")
        return False

    valid = True
    for company in COMPANIES:
        if not company["email"] or not company["password"] or not company["chat_id"]:
            logger.error("Invalid company config: %s", company)
            valid = False
    return valid


def run():
    logger.info("=" * 60)
    logger.info("Starting Payment Alert Bot")
    logger.info("Monitoring %s mailbox(es)", len(COMPANIES))
    logger.info("Check interval: %ss", CHECK_INTERVAL)
    logger.info("Since date for search: %s", resolve_since_date())
    for c in COMPANIES:
        topic = f", topic={c['topic_id']}" if c.get("topic_id") is not None else ""
        logger.info("  %s: chat=%s%s tags=%s allowed_senders=%s", c["name"], c["chat_id"], topic, c["tags"], c["allowed_senders"])
    logger.info("=" * 60)

    if not validate_config():
        return

    consecutive_failures = 0
    max_backoff = 900

    while True:
        try:
            check_mail()
            consecutive_failures = 0
            time.sleep(CHECK_INTERVAL)
        except Exception as e:
            consecutive_failures += 1
            backoff = min(CHECK_INTERVAL * consecutive_failures, max_backoff)
            logger.error("Main loop error (failure #%s): %s", consecutive_failures, e)
            logger.info("Backing off for %ss before retry...", backoff)
            time.sleep(backoff)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
