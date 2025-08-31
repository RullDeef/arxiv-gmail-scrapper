import base64
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

import google.oauth2.credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ---------------------------------------------------------------------------
# Domain object
# ---------------------------------------------------------------------------
class ArxivSpec:
    def __init__(self) -> None:
        self.DOI: str = ""
        self.Date: datetime = datetime.now()
        self.Title = ""
        self.Authors = ""
        self.Categories = ""
        self.Comments = ""
        self.MSCClass = ""
        self.ACMClass = ""
        self.Abstract = ""

    def to_dict(self) -> Dict[str, str]:
        return {
            "DOI": self.DOI,
            "Date": str(self.Date),
            "Title": self.Title,
            "Authors": self.Authors,
            "Categories": self.Categories,
            "Comments": self.Comments,
            "MSCClass": self.MSCClass,
            "ACMClass": self.ACMClass,
            "Abstract": self.Abstract,
        }

# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------
DB_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS arxiv_specs (
    doi          TEXT PRIMARY KEY,
    date         TEXT NOT NULL,
    title        TEXT NOT NULL,
    authors      TEXT NOT NULL,
    categories   TEXT NOT NULL,
    comments     TEXT,
    msc_class    TEXT,
    acm_class    TEXT,
    abstract     TEXT NOT NULL
) WITHOUT ROWID;
"""

INSERT_SQL = """
INSERT OR IGNORE INTO arxiv_specs
(doi, date, title, authors, categories,
 comments, msc_class, acm_class, abstract)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def _ensure_table(conn: sqlite3.Connection) -> None:
    """Create table if it does not yet exist."""
    conn.executescript(DB_SCHEMA_SQL)
    conn.commit()


def upsert_specs(db_path: Path | str, specs: Iterable[ArxivSpec]) -> int:
    """
    Idempotent upsert of ArxivSpec objects into *db_path*.
    Returns the number of *new* rows actually inserted (0 when all DOIs already exist).
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    inserted = 0
    with sqlite3.connect(db_path) as conn:
        _ensure_table(conn)
        cur = conn.executemany(
            INSERT_SQL,
            [
                (
                    spec.DOI,
                    spec.Date.isoformat(timespec="seconds"),
                    spec.Title,
                    spec.Authors,
                    spec.Categories,
                    spec.Comments,
                    spec.MSCClass,
                    spec.ACMClass,
                    spec.Abstract,
                )
                for spec in specs
            ],
        )
        inserted = cur.rowcount
        conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# OAuth helpers
# ---------------------------------------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
TOKEN_FILE = "token.json"
CREDENTIALS_FILE = "credentials.json"


def _get_credentials():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE) as f:
            creds = google.oauth2.credentials.Credentials.from_authorized_user_file(
                TOKEN_FILE, SCOPES
            )
    # refresh / re-authorize if necessary
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
    return creds


# ---------------------------------------------------------------------------
# Gmail interaction
# ---------------------------------------------------------------------------
def _get_service():
    creds = _get_credentials()
    return build("gmail", "v1", credentials=creds)


def _find_arxiv_label(service):
    results = service.users().labels().list(userId="me").execute()
    labels = results.get("labels", [])
    for lbl in labels:
        if lbl["name"].lower() == "arxiv":
            return lbl
    print("Labels:")
    for lbl in labels:
        print("-", lbl["name"])
    print('"arxiv" label required for further processing')
    return None


# ---------------------------------------------------------------------------
# Message processing
# ---------------------------------------------------------------------------
def _stream_base64_decoder(src: bytes) -> bytes:
    """Robust to corrupted data: replace invalid chunks with '???'."""
    decoded = bytearray()
    for i in range(0, len(src), 4):
        chunk = src[i : i + 4]
        if len(chunk) < 4:
            break
        try:
            decoded.extend(base64.b64decode(chunk, validate=True))
        except Exception:
            decoded.extend(b"???")
    return bytes(decoded)


def _write_message(msg: Dict) -> bytes:
    """Return the raw body bytes of a gmail message."""
    body_data = msg["payload"]["body"].get("data", "")
    raw = body_data.encode("ascii")
    return _stream_base64_decoder(raw) + b"\n"


# ---------------------------------------------------------------------------
# Parsing the e-mail body
# ---------------------------------------------------------------------------
def _iterate_message_specs(raw: bytes) -> List[ArxivSpec]:
    specs: List[ArxivSpec] = []
    message_text = raw.decode("utf-8", errors="ignore").strip()
    messages = message_text.split('-' * 78 + '\r\n' + '-' * 78 + '\r\n')

    # extract received from-to date
    #date_format = "%a %d %b %y %H:%M:%S %Z"
    article_date_format = "%a, %d %b %Y %H:%M:%S %Z"
    #received_dates = messages[1].split('received from')[1].split('to')
    #received_from = datetime.strptime(received_dates[0].strip(), date_format)
    #received_to = datetime.strptime(received_dates[1].strip(), date_format)

    messages = messages[2].split('%%%---' * 13)[0].split('-' * 78 + '\r\n')

    for message in messages:
        spec = ArxivSpec()

        parts = message.split('\\\\\r\n')
        header = parts[1]
        if len(parts) >= 3:
            spec.Abstract = parts[2].replace('\r\n', ' ').split('\\\\')[0].strip()

        header_items = header.replace('\r\n  ', ' ').split('\r\n')
        spec.DOI = header_items[0]

        for item in header_items[1:]:
            if item.startswith('Date: '):
                spec.Date = datetime.strptime(item[len('Date: '):].split('   ')[0], article_date_format)
            elif item.startswith('Title: '):
                spec.Title = item[len('Title: '):]
            elif item.startswith('Authors: '):
                spec.Authors = item[len('Authors: '):]
            elif item.startswith('Categories: '):
                spec.Categories = item[len('Categories: '):]
            elif item.startswith('Comments: '):
                spec.Comments = item[len('Comments: '):]
            elif item.startswith('MSC-class: '):
                spec.MSCClass = item[len('MSC-class: '):]
            elif item.startswith('ACM-class: '):
                spec.ACMClass = item[len('ACM-class: '):]

        specs.append(spec)

    return specs


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    service = _get_service()
    label = _find_arxiv_label(service)
    if label is None:
        return

    try:
        msgs = (
            service.users()
            .messages()
            .list(userId="me", labelIds=[label["id"]], maxResults=500)
            .execute()
            .get("messages", [])
        )
    except HttpError as error:
        print("Unable to retrieve messages:", error)
        return

    for msg_meta in msgs:
        msg_id = msg_meta["id"]
        try:
            msg = service.users().messages().get(userId="me", id=msg_id).execute()
            raw = _write_message(msg)
            specs = _iterate_message_specs(raw)
            if len(specs) == 0:
                continue

            n = upsert_specs("arxiv.db", specs)
            print(f"inserted {n} new specs")
        except Exception as e:
            print(f"failed to extract articles from message with id '{msg_id}': {e}")


if __name__ == "__main__":
    main()
