# config.py

# Path to your SQLite database file
DB_PATH = "movies.db"

# URL template for Step 1 paging
BASE_URL_TEMPLATE = (
    "https://ge.movie/"
    "filter-movies?search=&type=movie&languages=ka&imdb=6;10.0&year=2025;2027&page={}"
)
