import asyncio
import argparse
from storage import (
    init_db,
    clear_movie_links,
    clear_movie_details,
    reset_links_processed,
    save_links,
    fetch_unprocessed_links,
    save_details,
    fetch_pending_downloads,
    mark_downloaded
)
from fetchers_step1 import step1_collect_links
from fetchers_step2 import step2_fetch_details
from downloader_step3 import download_movie

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

def run_step3():
    # 1) grab all movies not yet downloaded
    pending = fetch_pending_downloads()
    print(f"Step 3: {len(pending)} movies to download.")
    if not pending:
        return

    # common headers from your original code
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9,ka;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "priority": "i",
        "range": "bytes=0-",
        "referer": "https://em.kinoflix.tv/",
        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
        "sec-fetch-dest": "video",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "cross-site",
        "sec-fetch-storage-access": "none",
        "user-agent": "Mozilla/5.0 (Linux; Android 13; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Mobile Safari/537.36"
    }

    for rec in pending:
        movie_id = rec["id"]
        url = rec["api_url"]
        # build a unique local filename, e.g. movies/<id>_<basename>.mp4
        basename = url.split("/")[-1].split("?")[0]
        dest     = f"movies/{movie_id}_{basename}"

        print(f"\n[Downloading] {movie_id} → {dest}")
        # 2) actually download it
        asyncio.run(download_movie(url, dest, headers))

        # 3) update the DB so we don’t re-download
        mark_downloaded(movie_id, dest)
        print(f"[Updated DB] movie_details.id={movie_id} marked downloaded\n")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["1", "2", "3", "all"])
    args = p.parse_args()

    if args.step == "1":
        run_step1()
    elif args.step == "2":
        run_step2()
    elif args.step == "3":
        run_step3()
    elif args.step == "all":
        run_step1()
        run_step2()
        run_step3()
