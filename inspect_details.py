import sqlite3
import pandas as pd

DB = "movies.db"
with sqlite3.connect(DB) as conn:
    df = pd.read_sql("SELECT * FROM movie_details", conn)
    print(df.info())      # high-level summary
    print(df.head(10))    # first 10 records
