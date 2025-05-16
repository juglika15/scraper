import sqlite3
import json
from typing import Any, Mapping
from config import DB_PATH


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """
    Ensure tables exist and add missing columns including download_status.
    """
    conn = _get_conn()
    c = conn.cursor()

    # Base tables: links and details
    c.execute("""
    CREATE TABLE IF NOT EXISTS movie_links (
      url TEXT PRIMARY KEY,
      processed INTEGER NOT NULL DEFAULT 0
    );""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS movie_details (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      url TEXT UNIQUE
    );""")

    # Add missing columns with migrations
    migrations = [
        ("imdb_id",         "TEXT"),
        ("title_ge",        "TEXT"),
        ("title_en",        "TEXT"),
        ("poster_link",     "TEXT"),
        ("genre",           "TEXT"),
        ("studio",          "TEXT"),
        ("year",            "INTEGER"),
        ("directors",       "TEXT"),
        ("length",          "TEXT"),
        ("countries",       "TEXT"),
        ("budget",          "TEXT"),
        ("box_office",      "TEXT"),
        ("plot",            "TEXT"),
        ("actors",          "TEXT"),
        ("api_url",         "TEXT"),
        ("downloaded_path", "TEXT"),
        ("download_status", "INTEGER NOT NULL DEFAULT 0")
    ]
    for col, typ in migrations:
        try:
            c.execute(f"ALTER TABLE movie_details ADD COLUMN {col} {typ};")
        except sqlite3.OperationalError:
            pass  # already exists

    # Use WAL mode to reduce locking
    try:
        c.execute("PRAGMA journal_mode=WAL;")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()
    conn.close()


def clear_movie_links() -> None:
    conn = _get_conn()
    conn.execute("DELETE FROM movie_links;")
    conn.commit()
    conn.close()


def clear_movie_details() -> None:
    conn = _get_conn()
    conn.execute("DELETE FROM movie_details;")
    conn.commit()
    conn.close()


def reset_links_processed() -> None:
    conn = _get_conn()
    conn.execute("UPDATE movie_links SET processed = 0;")
    conn.commit()
    conn.close()


def save_links(links: list[str]) -> None:
    conn = _get_conn()
    c = conn.cursor()
    for url in links:
        c.execute("INSERT OR IGNORE INTO movie_links(url) VALUES(?);", (url,))
    conn.commit()
    conn.close()


def fetch_unprocessed_links() -> list[str]:
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT url FROM movie_links WHERE processed = 0;")
    rows = c.fetchall()
    conn.close()
    return [r["url"] for r in rows]


def save_details(details: Mapping[str, Any]) -> None:
    """
    Accepts dict with keys: url, imdb_id, title:{ge,en}, poster_link,
      movie_info dict, actors list, api_movie_link
    """
    def get(key, default=None): return details.get(key, default)

    url         = get("url", "")
    imdb_id     = get("imdb_id", "")
    poster_link = get("poster_link", "")
    title = get("title", {})
    title_ge    = title.get("ge", "")
    title_en    = title.get("en", "")

    mi = get("movie_info", {})
    # Georgian or English keys
    genre     = mi.get("ჟანრი", mi.get("Genre", ""))
    studio    = mi.get("სტუდია", mi.get("Studio", ""))
    year_txt  = mi.get("გამოშვების წელი", mi.get("Year", ""))
    year      = int(year_txt) if year_txt and year_txt.isdigit() else None
    directors = mi.get("რეჟისორი", mi.get("Director", ""))
    length    = mi.get("ხანგრძლივობა", mi.get("Length", ""))
    countries = (
        mi.get("ქვეყანა", mi.get("ქვეყნები",
        mi.get("Country", mi.get("Countries", ""))))
    )
    budget    = mi.get("ბიუჯეტი", mi.get("Budget", ""))
    box_off   = mi.get("შემოსავალი", mi.get("Box office", ""))
    plot      = mi.get("ფილმის სიუჟეტი", mi.get("Plot", ""))

    actors_list = get("actors", [])
    api_url     = get("api_movie_link", "")

    conn = _get_conn()
    c = conn.cursor()
    c.execute("""
    INSERT OR REPLACE INTO movie_details
      (url, imdb_id, title_ge, title_en, poster_link,
       genre, studio, year, directors, length,
       countries, budget, box_office, plot, actors, api_url)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    """, (
        url, imdb_id, title_ge, title_en, poster_link,
        genre, studio, year, directors, length,
        countries, budget, box_off, plot,
        json.dumps(actors_list, ensure_ascii=False), api_url
    ))
    # mark processed
    c.execute("UPDATE movie_links SET processed = 1 WHERE url = ?;", (url,))
    conn.commit()
    conn.close()


def fetch_pending_downloads() -> list[dict[str, Any]]:
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT id, api_url FROM movie_details WHERE download_status = 0;")
    rows = c.fetchall()
    conn.close()
    return [{"id": r["id"], "api_url": r["api_url"]} for r in rows]


def mark_downloaded(detail_id: int, file_path: str) -> None:
    conn = _get_conn()
    c = conn.cursor()
    c.execute(
        "UPDATE movie_details SET downloaded_path = ?, download_status = 1 WHERE id = ?;",
        (file_path, detail_id)
    )
    conn.commit()
    conn.close()