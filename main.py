# main.py

import argparse
from storage import init_db, save_links
from fetchers import step1_collect_links

def run_step1():
    # 1) Ensure your SQLite tables exist
    init_db()
    # 2) Crawl and collect all movie-page URLs
    links = step1_collect_links()
    # 3) Persist them into your movie_links table
    save_links(links)
    print(f"Saved {len(links)} links to the database.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["1","2","3","all"])
    args = p.parse_args()

    if args.step == "1":
        run_step1()
    # (…handle steps 2, 3, and all…)
