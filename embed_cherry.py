#!/usr/bin/env python3
"""
embed_cherry.py

Compute embeddings for every arXiv article and insert them into
Cherry Studio's knowledge-base table `vectors`.

The SQLite file is *assumed* to have the exact schema:

    CREATE TABLE vectors (
        id              TEXT PRIMARY KEY,
        pageContent     TEXT UNIQUE,
        uniqueLoaderId  TEXT NOT NULL,
        source          TEXT NOT NULL,
        vector          F32_BLOB(768),
        metadata        TEXT
    );

Usage
-----
python embed_cherry.py \
    --arxiv-db arxiv.db \
    --cherry-db ~/.config/CherryStudio/Data/KnowledgeBase/<database_file> \
    --model nomic-embed-text \
    --ollama http://localhost:11434
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import struct
import sys
from datetime import datetime
from pathlib import Path
from typing import Generator, List

import httpx
from arxiv_spec import ArxivSpec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _fetch_articles(conn: sqlite3.Connection) -> Generator[ArxivSpec]:
    """
    Return list of arxiv specs for rows that do NOT already exist in the vectors table.
    """
    cur = conn.execute("""SELECT doi, date, title, authors, categories, comments, msc_class, acm_class, abstract
                          FROM arxiv_specs""")
    for doi, date, title, authors, categories, comments, msc_class, acm_class, abstract in cur:
        spec = ArxivSpec()
        spec.DOI = doi
        spec.Date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
        spec.Title = title
        spec.Authors = authors
        spec.Categories = categories
        spec.Comments = comments
        spec.MSCClass = msc_class
        spec.ACMClass = acm_class
        spec.Abstract = abstract
        yield spec


def _chop_str(s: str, max_length: int = 1000, overlap: int = 50) -> Generator[str]:
    while len(s) > max_length:
        yield s[:max_length]
        s = s[max_length - overlap:]
    yield s


def _chop(spec: ArxivSpec, max_length: int = 1000, overlap: int = 50) -> Generator[ArxivSpec]:
    spec.Authors = ', '.join(spec.Authors.split(', ')[:3])
    spec.Authors = ', '.join(spec.Authors.split(' and ')[:3])
    escape_len = len(json.dumps(spec.Abstract)) - len(spec.Abstract)
    meta_length = len(spec.to_json()) - len(json.dumps(spec.Abstract)) + escape_len
    for abstract in _chop_str(spec.Abstract, max_length - meta_length, overlap):
        spec.Abstract = abstract
        if len(spec.to_json()) > max_length:
            print("spec:", spec.to_json())
            print("exceeded max length:", max_length)
            print("spec length:", len(spec.to_json()))
            assert False
        yield spec


def _embed(text: str, model: str, base_url: str) -> bytes:
    """Call Ollama /api/embeddings and return 768-float little-endian blob."""
    url = f"{base_url.rstrip('/')}/api/embeddings"
    resp = httpx.post(url, json={"model": model, "prompt": text}, timeout=180)
    resp.raise_for_status()
    vec: List[float] = resp.json()["embedding"]
    if len(vec) != 768:
        raise ValueError(f"Model returned {len(vec)} dims, expected 768")
    return struct.pack(f"<{len(vec)}f", *vec)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arxiv-db",
                        default="arxiv.db",
                        type=Path,
                        help="arxiv.db file (default: arxiv.db)")
    parser.add_argument("--cherry-db",
                        required=True,
                        type=Path,
                        help="Cherry Studio vectors.db")
    parser.add_argument("--model",
                        default="nomic-embed-text",
                        type=str,
                        help="Ollama embedding model name (default: nomic-embed-text)")
    parser.add_argument("--ollama",
                        default="http://localhost:11434",
                        help="Ollama base URL (default: http://localhost:11434)")
    args = parser.parse_args(argv)
    # ---------- sanity checks ----------
    if not args.arxiv_db.exists():
        sys.exit(f"arxiv.db not found: {args.arxiv_db}")
    if not args.cherry_db.exists():
        sys.exit(f"cherry vectors db not found: {args.cherry_db}")

    # ---------- main loop ----------
    with sqlite3.connect(args.arxiv_db) as arxiv_conn, \
         sqlite3.connect(args.cherry_db) as cherry_conn:
        print("Embedding articles...")
        inserted = 0
        for spec_ in _fetch_articles(arxiv_conn):
            doi = spec_.DOI.split(':')[1].split(' ')[0]
            unique_loader = f"WebLoader_arxiv_{doi}"
            if cherry_conn.execute("SELECT COUNT(*) FROM vectors WHERE uniqueLoaderId = ?", (unique_loader,)).fetchone()[0] != 0:
                continue # skip already added specs
            source = f"https://arxiv.org/abs/{doi}"
            print("source:", source)
            for spec in _chop(spec_):
                page_content = spec.to_json()
                cur = cherry_conn.execute("SELECT COUNT(*) FROM vectors WHERE uniqueLoaderId = ?", (unique_loader,))
                suffix = cur.fetchone()[0]
                meta_id = f"{unique_loader}_{suffix}"
                metadata_obj = {
                    "source": source,
                    "type": "WebLoader",
                    "uniqueLoaderId": unique_loader,
                    "id": meta_id,
                }
                vector_blob = _embed(page_content, args.model, args.ollama)
                try:
                    cherry_conn.execute(
                        """
                        INSERT OR IGNORE INTO vectors
                        (id, pageContent, uniqueLoaderId, source, vector, metadata)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            spec.DOI,
                            page_content,
                            unique_loader,
                            metadata_obj["source"],
                            vector_blob,
                            json.dumps(metadata_obj, ensure_ascii=False),
                        ),
                    )
                    inserted += 1
                except sqlite3.IntegrityError:
                    # UNIQUE constraint already satisfied
                    pass
            cherry_conn.commit()
            print("Inserted vectors:", inserted)
        print("Inserted vectors:", inserted)


if __name__ == "__main__":
    _main(sys.argv[1:])
