import asyncio
import dns.resolver
from typing import Optional
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError

from storage import clear_movie_details, reset_links_processed, init_db, fetch_unprocessed_links, save_details
from config import HOSTNAME, USER_AGENTS

async def _resolve_ip_async(hostname: str = HOSTNAME) -> Optional[str]:
    resolver = dns.resolver.Resolver()
    resolver.nameservers = ["1.1.1.1", "1.0.0.1"]
    loop = asyncio.get_running_loop()
    answers = await loop.run_in_executor(None, resolver.resolve, hostname, "A")
    return answers[0].address if answers else None

async def _fetch_one(movie_link: str) -> dict | None:
    init_db()
    target_ip = await _resolve_ip_async()
    if not target_ip:
        return None

    media_urls: set[str] = set()
    def is_media(u: str) -> bool:
        return any(ext in u.lower() for ext in (".m3u8", ".mp4", ".ts")) and not u.startswith("data:")

    async def handle_response(response):
        url_base = response.url.split("?")[0]
        if is_media(url_base) and url_base not in media_urls:
            media_urls.add(url_base)

    async with async_playwright() as p:
        iphone = p.devices["iPhone 12"]
        context = await p.chromium.launch_persistent_context(
            user_data_dir="", headless=True,
            args=[f"--host-resolver-rules=MAP {HOSTNAME} {target_ip}"],
            ignore_https_errors=True,
            viewport=iphone["viewport"],
            user_agent=iphone["user_agent"],
            is_mobile=iphone["is_mobile"],
            has_touch=iphone["has_touch"],
            device_scale_factor=iphone["device_scale_factor"]
        )

        page = context.pages[0] if context.pages else await context.new_page()
        page.on("response", handle_response)
        try:
            await page.goto(movie_link, timeout=90000, wait_until="domcontentloaded")
        except TimeoutError:
            await context.close()
            return None

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        section = soup.select_one("section.content div.movies-full")

        # IMDb ID
        imdb_href = section.select_one(".movies-full__inside-rates a")["href"]
        imdb_id = imdb_href.split("title/")[1].split("/")[0]

        # Poster
        poster = section.select_one(".movies-full__poster img")["src"]
        if poster.startswith("/"):
            poster = f"https://{HOSTNAME}{poster}"

        # Titles: use the global h1, not nested selector
        h1 = soup.find("h1")
        divs = h1.find_all("div") if h1 else []
        title_ge = divs[0].text.strip().split(" (2")[0] if len(divs) > 0 else ""
        title_en = divs[1].text.strip().split("\t")[0] if len(divs) > 1 else ""

        # Movie info
        movie_info = {}
        for p in soup.select("section.content div.textOf p"):
            txt = p.text.strip()
            key, val = (txt.split(": ", 1) if ": " in txt else (None, None))
            if key:
                movie_info[key.strip()] = val.strip()

        # Actors list
        actors = []
        names = soup.find_all("p", class_="actor-name")
        imgs  = soup.select("div.actor-img img[src]")
        for name_el, img_el in zip(names, imgs):
            img_url = img_el["src"]
            if img_url.startswith("/"):
                img_url = f"https://{HOSTNAME}{img_url}"
            actors.append({"name": name_el.text.strip(), "img": img_url})

        # Trigger player to intercept
        try:
            iframe = await page.wait_for_selector('iframe[src*="player.php"]', timeout=30000)
            content = await iframe.content_frame()
            await content.wait_for_load_state('domcontentloaded', timeout=20000)
            await asyncio.sleep(2)
            await content.locator("body").tap(timeout=10000)
            await asyncio.sleep(25)
        except Exception:
            pass

        await context.close()

    # api_movie_link: prefer CDN+GEO+SD or fallback to iframe src
    html_iframe = section.select_one(".movies-full__content iframe")
    html_api = html_iframe["src"] if html_iframe else ""
    candidates = [u for u in media_urls if all(x in u for x in ("cd","GEO","SD"))]
    api_link = candidates[0] if candidates else html_api

    return {
        "url": movie_link,
        "imdb_id": imdb_id,
        "title": {"ge": title_ge, "en": title_en},
        "poster_link": poster,
        "movie_info": movie_info,
        "actors": actors,
        "api_movie_link": api_link
    }


def step2_fetch_details(link: str) -> dict | None:
    return asyncio.run(_fetch_one(link))