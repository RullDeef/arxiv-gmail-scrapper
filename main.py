#!/usr/bin/env python3
"""
arxiv_gmail_sync.py

Scrape the Gmail label “arxiv”, parse every daily digest, and insert each
article description into a local SQLite database.

The database is created automatically and updated idempotently
(duplicate DOIs are ignored).

Examples
--------
$ arxiv_gmail_sync.py --db arxiv.db
$ arxiv_gmail_sync.py --db arxiv.db --from 2024-06-01 --to 2024-06-30
$ arxiv_gmail_sync.py --db arxiv.db --last-week
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import google.oauth2.credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------------------------------------------------------------------------
# Domain model
# ---------------------------------------------------------------------------
class ArxivSpec:
    def __init__(self) -> None:
        self.DOI: str = ""
        self.Date: datetime = datetime.now()
        self.Title: str = ""
        self.Authors: str = ""
        self.Categories: str = ""
        self.Comments: str = ""
        self.MSCClass: str = ""
        self.ACMClass: str = ""
        self.Abstract: str = ""


# ---------------------------------------------------------------------------
# Gmail API helpers
# ---------------------------------------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
TOKEN_FILE = "token.json"
CREDENTIALS_FILE = "credentials.json"


def _get_credentials() -> google.oauth2.credentials.Credentials:
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE) as f:
            creds = google.oauth2.credentials.Credentials.from_authorized_user_file(
                TOKEN_FILE, SCOPES
            )
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
    return creds


def _find_arxiv_label(service, label_name):
    labels = service.users().labels().list(userId="me").execute().get("labels", [])
    for lbl in labels:
        if lbl["name"].lower() == label_name:
            return lbl
    print(f"No label named '{label_name}' found. Available labels:")
    for lbl in labels:
        print(" -", lbl["name"])
    return None


# ---------------------------------------------------------------------------
# Message parser
# ---------------------------------------------------------------------------
def _decode_body(msg: Dict) -> str:
    """Return the decoded plaintext body of a Gmail message."""
    body_data = msg["payload"]["body"].get("data", "")
    if not body_data:
        return ""
    raw = body_data.encode("ascii")
    decoded = base64.urlsafe_b64decode(raw + b"===")  # padding fix
    return decoded.decode("utf-8", errors="ignore")


def _parse_digest(body: str) -> List[ArxivSpec]:
    """Very small, robust parser that extracts article blocks."""
    specs: List[ArxivSpec] = []
    messages = body.strip().split('-' * 78 + '\r\n' + '-' * 78 + '\r\n')
    if len(messages) < 3:
        return []

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
                spec.Date = datetime.strptime(item[len('Date: '):].split('   ')[0], "%a, %d %b %Y %H:%M:%S %Z")
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
# Database helpers
# ---------------------------------------------------------------------------
DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS arxiv_specs (
    doi         TEXT PRIMARY KEY,
    date        TEXT NOT NULL,
    title       TEXT NOT NULL,
    authors     TEXT NOT NULL,
    categories  TEXT NOT NULL,
    comments    TEXT,
    msc_class   TEXT,
    acm_class   TEXT,
    abstract    TEXT NOT NULL
) WITHOUT ROWID;
"""

INSERT_SQL = """
INSERT OR IGNORE INTO arxiv_specs
(doi, date, title, authors, categories,
 comments, msc_class, acm_class, abstract)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def upsert_batch(db_path: Path, specs: Iterable[ArxivSpec]) -> int:
    """Insert specs idempotently. Returns number of *new* rows."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    inserted = 0
    with sqlite3.connect(db_path) as conn:
        conn.executescript(DB_SCHEMA)
        cur = conn.executemany(
            INSERT_SQL,
            [
                (
                    s.DOI,
                    s.Date.isoformat(timespec="seconds"),
                    s.Title,
                    s.Authors,
                    s.Categories,
                    s.Comments,
                    s.MSCClass,
                    s.ACMClass,
                    s.Abstract,
                )
                for s in specs
            ],
        )
        inserted = cur.rowcount
        conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_date_arg(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def _main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        type=Path,
        required=True,
        help="SQLite database file (created if missing)",
    )
    parser.add_argument(
        "--label",
        metavar="LABEL",
        dest="arxiv_label",
        type=str,
        default="arxiv",
        help="gmail message label for filter arxiv messages (default: arxiv)",
    )
    parser.add_argument(
        "--last-week",
        action="store_true",
        help="scan messages received during the last 7 days (inclusive)",
    )
    parser.add_argument(
        "--from",
        dest="date_from",
        type=_parse_date_arg,
        help="ignore messages strictly before YYYY-MM-DD (inclusive)",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        type=_parse_date_arg,
        help="ignore messages strictly after YYYY-MM-DD (inclusive)",
    )
    args = parser.parse_args(argv)

    # Build effective date range
    if args.last_week:
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        args.date_from = today - timedelta(days=6)  # 7 days inclusive
        args.date_to = today
    elif args.date_from is None and args.date_to is None:
        # No filter at all
        args.date_from = None
        args.date_to = None
    elif args.date_from is None or args.date_to is None:
        parser.error("--from and --to must both be provided together")

    service = build("gmail", "v1", credentials=_get_credentials())
    label = _find_arxiv_label(service, args.arxiv_label)
    if not label:
        sys.exit(1)

    try:
        query_parts = [f'label:{args.arxiv_label}']
        if args.date_from is not None:
            query_parts.append(f"after:{args.date_from.strftime('%Y/%m/%d')}")
        if args.date_to is not None:
            query_parts.append(f"before:{args.date_to.strftime('%Y/%m/%d')}")
        msgs = (
            service.users()
            .messages()
            .list(userId="me", q=' '.join(query_parts), maxResults=500)
            .execute()
            .get("messages", [])
        )
    except HttpError as e:
        print("Gmail API error:", e, file=sys.stderr)
        sys.exit(1)

    total_new = 0
    for meta in msgs:
        meta_id = meta["id"]
        try:
            msg = service.users().messages().get(userId="me", id=meta_id).execute()
            body = _decode_body(msg)
            specs = _parse_digest(body)
            new_rows = upsert_batch(args.db, specs)
            total_new += new_rows
            print(f"{meta_id}: {len(specs)} articles parsed")
            if new_rows:
                print(f"{meta_id}: {new_rows} new articles")
        except Exception as e:
            print("Skipping", meta_id, e, file=sys.stderr)
    print("Done. Total new rows:", total_new)


if __name__ == "__main__":
    _main(sys.argv[1:])
