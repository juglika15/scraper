import asyncio
import dns.resolver
from typing import Optional
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError
import time

from storage import clear_movie_details, reset_links_processed, init_db, fetch_unprocessed_links, save_details
from config import HOSTNAME, USER_AGENTS

async def _resolve_ip_async(hostname: str = HOSTNAME) -> Optional[str]:
    resolver = dns.resolver.Resolver()
    resolver.nameservers = ["1.1.1.1", "1.0.0.1"]
    loop = asyncio.get_running_loop()
    answers = await loop.run_in_executor(None, resolver.resolve, hostname, "A")
    return answers[0].address if answers else None

def safe_split(text, delimiter=": ", default_key=None):
    if delimiter in text:
        return text.split(delimiter, 1)
    if default_key:
        return (default_key, text.strip())
    return (None, None)

def safe_get_text(el):
    return el.text.strip() if el else ""

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
        print(f"  → {title_en}")

        # Movie info
        movie_info = {}
        info_block = soup.find("div", class_="textOf")
        paras = info_block.find_all("p") if info_block else []
        for i, p in enumerate(paras):
            key, val = safe_split(
                p.text.strip(),
                ": ",
                "ფილმის სიუჟეტი" if i == len(paras) - 1 else None
            )
            if key:
                movie_info[key] = val

        # Actors list
        actors = []
        for item in soup.select("div.actor-item"):
            name_el = item.select_one("p.actor-name")
            img_el  = item.select_one("div.actor-img img")

            name = name_el.get_text(strip=True) if name_el else ""
            # prefer data-src (lazy-loaded), fallback to src
            img  = img_el.get("data-src") or img_el.get("src") or ""

            actors.append({
                "name": name,
                "img": img
            })

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