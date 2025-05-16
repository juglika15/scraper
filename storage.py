import sqlite3
import json
from typing import List, Dict, Mapping, Any
from config import DB_PATH
from models import MovieDetails

def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    conn = _get_conn()
    c = conn.cursor()
    c.execute("""
      CREATE TABLE IF NOT EXISTS movie_details (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        url            TEXT UNIQUE,
        imdb_id        TEXT,
        title_ge       TEXT,
        title_en       TEXT,
        poster_link    TEXT,
        genre          TEXT,
        studio         TEXT,
        year           INTEGER,
        directors      TEXT,
        length         TEXT,
        countries      TEXT,
        budget         TEXT,
        box_office     TEXT,
        plot           TEXT,
        actors         TEXT,
        api_url        TEXT,
        downloaded_path TEXT
      );
    """)
    conn.commit()
    conn.close()

def save_details(details: Mapping[str, Any]) -> None:
    """
    details is a dict with keys:
      url, imdb_id, title ({"ge":…, "en":…}), poster_link,
      movie_info dict (keys like "Genre","Studio",…),
      actors dict, api_url (optional)
    """
    # pull fields out, with safe defaults
    url         = details["url"]
    imdb_id     = details.get("imdb_id", "")
    poster_link = details.get("poster_link", "")
    title_ge    = details.get("title", {}).get("ge", "")
    title_en    = details.get("title", {}).get("en", "")
    mi          = details.get("movie_info", {})
    genre       = mi.get("Genre", "")
    studio      = mi.get("Studio", "")
    year        = int(mi.get("Year", 0)) if mi.get("Year") else None
    directors   = mi.get("Director", "")
    length      = mi.get("Length", "")
    countries   = mi.get("Country", "")
    budget      = mi.get("Budget", "")
    box_office  = mi.get("Box office", "")
    plot        = mi.get("ფილმის სიუჟეტი", "")  # whatever key you used
    actors      = details.get("actors", {})
    api_url     = details.get("api_movie_link", details.get("movie_api_url", ""))

    conn = _get_conn()
    c = conn.cursor()
    c.execute("""
      INSERT OR REPLACE INTO movie_details
      (url, imdb_id, title_ge, title_en, poster_link,
       genre, studio, year, directors, length,
       countries, budget, box_office, plot, actors, api_url)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
      url, imdb_id, title_ge, title_en, poster_link,
      genre, studio, year, directors, length,
      countries, budget, box_office, plot,
      json.dumps(actors), api_url
    ))
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

def clear_movie_links() -> None:
    """
    Delete all rows from movie_links (so next run starts fresh).
    """
    conn = _get_conn()
    conn.execute("DELETE FROM movie_links;")
    conn.commit()
    conn.close()

def clear_movie_details() -> None:
    """
    Delete all rows from movie_details.
    """
    conn = _get_conn()
    conn.execute("DELETE FROM movie_details;")
    conn.commit()
    conn.close()
    
def reset_links_processed() -> None:
    """
    Mark every movie_links.processed = 0 so step2 can re-run on all of them.
    """
    conn = _get_conn()
    conn.execute("UPDATE movie_links SET processed = 0;")
    conn.commit()
    conn.close()