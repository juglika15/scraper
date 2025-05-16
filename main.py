import argparse
from storage import (
    init_db,
    clear_movie_links,
    clear_movie_details,
    reset_links_processed,
    save_links,
    fetch_unprocessed_links,
    save_details
)
from fetchers_step1 import step1_collect_links
from fetchers_step2 import step2_fetch_details  # <- updated import

def run_step1():
    init_db()
    clear_movie_links()
    links = step1_collect_links()
    save_links(links)
    print(f"Step 1: saved {len(links)} links")

def run_step2():
    init_db()
    clear_movie_details()
    reset_links_processed()
    for url in fetch_unprocessed_links():
        details = step2_fetch_details(url)
        if details:
            save_details(details)
            print(f"Step 2: saved details for {details['imdb_id']}")
        else:
            print(f"Step 2: failed for {url}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["1","2","all"])
    args = p.parse_args()

    if args.step == "1":
        run_step1()
    elif args.step == "2":
        run_step2()
    else:  # "all"
        run_step1()
        run_step2()
