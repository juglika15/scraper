import sqlite3

DB = "movies.db"
with sqlite3.connect(DB) as conn:
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM movie_links")
    total = c.fetchone()[0]
    print(f"Total movie links: {total}\n")

    print("First 10:")
    for url, processed in c.execute(
        "SELECT url, processed FROM movie_links LIMIT 20"
    ):
        print(f" • [{processed}] {url}")
