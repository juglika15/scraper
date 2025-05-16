import argparse
from storage import (
    clear_movie_links,
    save_links,
    init_db,
    clear_movie_details,
    reset_links_processed,
    fetch_unprocessed_links,
    save_details
)
from fetchers_step1 import step1_collect_links
from fetchers_step2 import step2_fetch_details

def run_step1():
    clear_movie_links()
    links = step1_collect_links()
    save_links(links)

def run_step2():
    init_db()                # ensure tables exist
    clear_movie_details()    # 🔥 drop old scraped details
    reset_links_processed()  # 🔄 un-mark all links so we re-process every link
    for url in fetch_unprocessed_links():
        details = step2_fetch_details(url)
        if details:
            save_details(details)
            print(f"Saved details for {details['imdb_id']}")
        else:
            print(f"Failed to get details for {url}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["1","2","3","all"])
    args = p.parse_args()

    if args.step == "1":
        run_step1()
    elif args.step == "2":
        run_step2()
    elif args.step == "all":
        run_step1()
        run_step2()

