# storage.py

import sqlite3
import json
from typing import List, Dict, Any
from config import DB_PATH
from models import MovieDetails

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    """
    Create the two tables:
      - movie_links(url, processed)
      - movie_details(id, url, title, …, api_url, downloaded_path)
    """
    conn = _get_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS movie_links (
            url TEXT PRIMARY KEY,
            processed INTEGER NOT NULL DEFAULT 0
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS movie_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            title TEXT,
            poster_url TEXT,
            genre TEXT,
            studio TEXT,
            year INTEGER,
            directors TEXT,
            length TEXT,
            countries TEXT,
            budget TEXT,
            box_office TEXT,
            plot TEXT,
            actors TEXT,
            api_url TEXT,
            downloaded_path TEXT DEFAULT NULL,
            FOREIGN KEY(url) REFERENCES movie_links(url)
        );
    """)
    conn.commit()
    conn.close()

def save_links(links: List[str]) -> None:
    """
    Insert each URL into movie_links (ignoring duplicates).
    """
    conn = _get_conn()
    c = conn.cursor()
    for url in links:
        c.execute("INSERT OR IGNORE INTO movie_links(url) VALUES(?)", (url,))
    conn.commit()
    conn.close()

def fetch_unprocessed_links() -> List[str]:
    """
    Return all URLs where processed == 0.
    """
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT url FROM movie_links WHERE processed = 0")
    rows = c.fetchall()
    conn.close()
    return [r["url"] for r in rows]

def save_details(details: MovieDetails) -> None:
    """
    Persist a MovieDetails object, serialize list fields as JSON,
    and mark its URL as processed in movie_links.
    
    Assumes your MovieDetails dataclass has a `url` attribute
    that exactly matches the link in movie_links.url.
    """
    conn = _get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO movie_details (
          url, title, poster_url, genre, studio, year, directors,
          length, countries, budget, box_office, plot, actors, api_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        details.url,
        details.title,
        details.poster_url,
        json.dumps(details.genre),
        details.studio,
        details.year,
        json.dumps(details.directors),
        details.length,
        json.dumps(details.countries),
        details.budget,
        details.box_office,
        details.plot,
        json.dumps(details.actors),
        details.api_url
    ))
    # mark the link as done
    c.execute("UPDATE movie_links SET processed = 1 WHERE url = ?", (details.url,))
    conn.commit()
    conn.close()

def fetch_pending_downloads() -> List[Dict[str, Any]]:
    """
    Return a list of dicts with 'id' and 'api_url' for any movie
    whose downloaded_path is still NULL.
    """
    conn = _get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id, api_url
          FROM movie_details
         WHERE downloaded_path IS NULL
    """)
    rows = c.fetchall()
    conn.close()
    return [{"id": r["id"], "api_url": r["api_url"]} for r in rows]

def mark_downloaded(detail_id: int, file_path: str) -> None:
    """
    After downloading, record the local file path for the given
    movie_details.id so you never re‐download it.
    """
    conn = _get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE movie_details
           SET downloaded_path = ?
         WHERE id = ?
    """, (file_path, detail_id))
    conn.commit()
    conn.close()
