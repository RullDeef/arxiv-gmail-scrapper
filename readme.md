<p align="center">
  <img src="https://img.shields.io/badge/python-3.11+-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/license-MIT-green.svg" alt="License">
  <!--img src="https://img.shields.io/badge/tests-passing-brightgreen.svg" alt="Tests"-->
</p>

<h1 align="center">
  📚 arXiv-Gmail-Scraper
</h1>

<p align="center">
  <b>Seamlessly sync your Gmail “arXiv” digests into a local SQLite database</b><br>
  <i>Lightning-fast, idempotent, and ready for automation.</i>
</p>

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🗓️ **Date filtering** | `--last-week`, `--from`, `--to` (all done server-side) |
| 🔁 **Idempotent** | Re-running never duplicates rows (unique DOI key) |
| 🧑‍💻 **Zero-config OAuth** | First run opens browser; token is cached |
| 🗄️ **SQLite storage** | Portable, query-ready, perfect for local ML pipelines |
| ⚡ **Efficient** | Uses Gmail search queries to minimise API calls |
| 🎯 **Tiny footprint** | One file, pure Python, no external deps except Google libs |

---

## 🚀 Quick Start

### 1. Install
```bash
git clone https://github.com/RullDeef/arxiv-gmail-scrapper.git
cd arxiv-gmail-scrapper
pip install -r requirements.txt
```

### 2. First-time OAuth

```bash
python arxiv_gmail_sync.py --db arxiv.db --last-week
# Browser opens → login → done. Token saved in `token.json`
```

### 3. Query your data

```bash
sqlite3 arxiv.db "SELECT title FROM arxiv_specs WHERE authors LIKE '%Hinton%';"
```

## 📋 Usage

```
usage: arxiv_gmail_sync.py [-h] (--db DB | --last-week | --from FROM --to TO)

Sync Gmail “arXiv” label to a local SQLite database.

optional arguments:
  -h, --help     show this help message and exit
  --db DB        SQLite database file (created if missing)
  --last-week    Only messages received in the last 7 days (inclusive)
  --from FROM    Lower bound: YYYY-mm-dd (inclusive)
  --to TO        Upper bound: YYYY-mm-dd (exclusive)

Examples:
  # All-time
  python arxiv_gmail_sync.py --db arxiv.db

  # Last 7 days
  python arxiv_gmail_sync.py --db arxiv.db --last-week

  # Custom range
  python arxiv_gmail_sync.py --db arxiv.db --from 2024-06-01 --to 2024-06-30
```

## 🏗️ Schema

| Column       | Type | Note                 |
| ------------ | ---- | -------------------- |
| `doi`        | TEXT | PRIMARY KEY          |
| `date`       | TEXT | ISO-8601             |
| `title`      | TEXT |                      |
| `authors`    | TEXT |                      |
| `categories` | TEXT | Space-separated list |
| `comments`   | TEXT |                      |
| `msc_class`  | TEXT |                      |
| `acm_class`  | TEXT |                      |
| `abstract`   | TEXT |                      |

## 🤝 Contributing

Issues and PRs welcome!
