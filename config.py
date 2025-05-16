# config.py

# Path to the SQLite DB file
DB_PATH = "movies.db"

# For Step 1 paging
BASE_URL_TEMPLATE = (
    "https://ge.movie/"
    "filter-movies?search=&type=movie&languages=ka&imdb=6;10.0&year=2025;2027&page={}"
)

# Hostname for DNS‐mapping and URL prefixing
HOSTNAME = "ge.movie"

# User-agents pool for Selenium
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_1) AppleWebKit/605.1.15 '
    '(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:132.0) '
    'Gecko/20100101 Firefox/132.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_1) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Vivaldi/7.0.3495.15',
]

# Step 1 limits
MAX_PAGES = 50
PAGE_DELAY = 10  # seconds between page fetches
