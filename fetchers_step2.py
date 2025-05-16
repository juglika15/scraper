# fetchers_step2.py

import asyncio
import dns.resolver
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError

# Adjust these to match your project’s config
HOSTNAME = "ge.movie"

def safe_get_text(el):
    return el.text.strip() if el else ""

def safe_find(container, selector, attr=None):
    """
    selector: tuple for .find(), e.g. ("img",)
    attr: attribute name to .get()
    """
    if not container:
        return ""
    el = container.find(*selector)
    if not el:
        return ""
    return el.get(attr).strip() if attr else el.text.strip()

def safe_split(text, delimiter=": ", default_key=None):
    if delimiter in text:
        return text.split(delimiter, 1)
    if default_key:
        return (default_key, text.strip())
    return (None, None)

async def _resolve_ip_async(hostname: str = HOSTNAME) -> str | None:
    """Resolve via Cloudflare DNS in an async-friendly way."""
    try:
        resolver = dns.resolver.Resolver()
        resolver.nameservers = ["1.1.1.1", "1.0.0.1"]
        loop = asyncio.get_running_loop()
        answers = await loop.run_in_executor(None, resolver.resolve, hostname, "A")
        return answers[0].address if answers else None
    except Exception as e:
        print(f"❌ DNS resolution failed: {e}")
        return None

async def _fetch_one(movie_link: str) -> dict | None:
    """Load a movie page, scrape fields, tap the player, intercept media URLs, and pick the CDN/GEO/SD link."""
    target_ip = await _resolve_ip_async()
    if not target_ip:
        return None

    media_urls: set[str] = set()
    def is_media(url: str) -> bool:
        return any(x in url.lower() for x in (".m3u8", ".mp4", ".ts", "/stream", "/playlist")) \
               and not url.startswith("data:")

    async def handle_response(response):
        base = response.url.split("?")[0]
        if is_media(base) and base not in media_urls:
            media_urls.add(base)
            print(f"▶️ Intercepted media URL: {base}")

    async with async_playwright() as p:
        iphone = p.devices["iPhone 12"]
        context = await p.chromium.launch_persistent_context(
            user_data_dir="",
            headless=True,
            args=[f"--host-resolver-rules=MAP {HOSTNAME} {target_ip}"],
            ignore_https_errors=True,
            viewport=iphone["viewport"],
            user_agent=iphone["user_agent"],
            is_mobile=iphone["is_mobile"],
            has_touch=iphone["has_touch"],
            device_scale_factor=iphone["device_scale_factor"],
        )

        page = context.pages[0] if context.pages else await context.new_page()
        page.on("response", handle_response)

        try:
            print(f"[Step 2] Loading {movie_link}")
            await page.goto(movie_link, timeout=90_000, wait_until="domcontentloaded")
        except TimeoutError:
            print("❌ Page load timed out")
            await context.close()
            return None
        except Exception as e:
            print(f"❌ Error loading page: {e}")
            await context.close()
            return None

        # parse HTML
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        body = soup.find("body")

        # IMDb ID
        imdb_href = safe_find(
            body.find("div", class_="movies-full__inside-rates"),
            ("a",),
            "href"
        )
        imdb_id = imdb_href.split("title/")[1].split("/")[0] if "title/" in imdb_href else ""

        # Poster link
        poster_link = safe_find(
            body.find("div", class_="movies-full__poster"),
            ("img",),
            "src"
        )

        # Titles
        h1 = body.find("h1")
        title_divs = h1.find_all("div") if h1 else []
        title_ge = title_divs[0].text.strip().split(" (2")[0] if len(title_divs) > 0 else ""
        title_en = title_divs[1].text.strip().split("\t")[0] if len(title_divs) > 1 else ""

        # Movie info block
        movie_info = {}
        info_block = body.find("div", class_="textOf")
        paras = info_block.find_all("p") if info_block else []
        for i, p in enumerate(paras):
            key, val = safe_split(
                p.text.strip(),
                ": ",
                "ფილმის სიუჟეტი" if i == len(paras) - 1 else None
            )
            if key:
                movie_info[key] = val

        # Actors
        actor_names  = [safe_get_text(p) for p in body.find_all("p", class_="actor-name")]
        actor_imgs   = [
            div.find("img")["src"]
            for div in body.find_all("div", class_="actor-img")
            if div.find("img")
        ]
        actors = dict(zip(actor_names, actor_imgs))

        # Tap the player iframe to fire off media requests
        try:
            frame_el = await page.wait_for_selector('iframe[src*="player.php"]', timeout=30_000)
            iframe = await frame_el.content_frame()
            await iframe.wait_for_load_state("domcontentloaded", timeout=20_000)
            await asyncio.sleep(2)
            await iframe.locator("body").tap(timeout=10_000)
            await asyncio.sleep(25)
        except Exception as e:
            print(f"⚠️ Player interaction failed: {e}")

        await context.close()

    # filter only the CDN + GEO + SD link
    candidates = [
        url for url in media_urls
        if "cd" in url and "GEO" in url and "SD" in url
    ]
    api_movie_link = candidates[0] if candidates else ""

    return {
        "url":           movie_link,
        "imdb_id":       imdb_id,
        "title":         {"ge": title_ge, "en": title_en},
        "poster_link":   poster_link,
        "movie_info":    movie_info,
        "actors":        actors,
        "api_movie_link": api_movie_link
    }

def step2_fetch_details(link: str) -> dict | None:
    """Synchronous entry point for step 2."""
    return asyncio.run(_fetch_one(link))
