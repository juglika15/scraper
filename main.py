# main.py

import argparse
from storage import init_db, save_links, clear_movie_links
from fetchers import step1_collect_links

def run_step1():
    init_db()               # make sure tables exist
    clear_movie_links()     # 🔥 blow away any old links
    links = step1_collect_links()
    save_links(links)
    print(f"Saved {len(links)} fresh links to the database.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["1","2","3","all"])
    args = p.parse_args()

    if args.step == "1":
        run_step1()
    # (…handle steps 2, 3, and all…)
