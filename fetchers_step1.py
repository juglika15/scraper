import time
import random
from typing import List, Optional
import dns.resolver
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from config import BASE_URL_TEMPLATE, HOSTNAME, USER_AGENTS, MAX_PAGES, PAGE_DELAY


def _resolve_ip_sync(hostname: str) -> Optional[str]:
    """Resolve hostname to IPv4 via Cloudflare DNS."""
    try:
        resolver = dns.resolver.Resolver()
        resolver.nameservers = ['1.1.1.1', '1.0.0.1']
        answers = resolver.resolve(hostname, 'A')
        ip = answers[0].address if answers else None
        print(f"Resolved {hostname} → {ip}")
        return ip
    except Exception as e:
        print(f"DNS resolution failed for {hostname}: {e}")
        return None

def _init_driver(hostname: str, target_ip: Optional[str]) -> webdriver.Chrome:
    """Configure and return a headless Chrome WebDriver."""
    opts = Options()
    opts.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    if target_ip:
        opts.add_argument(f"--host-resolver-rules=MAP {hostname} {target_ip}")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    return driver

def step1_collect_links(max_pages: int = MAX_PAGES, delay: int = PAGE_DELAY) -> List[str]:
    """
    Crawl the paginated movie list and return all individual movie-page URLs.
    """
    target_ip = _resolve_ip_sync(HOSTNAME)
    driver = _init_driver(HOSTNAME, target_ip)
    wait = WebDriverWait(driver, 15)
    
    all_links: List[str] = []
    try:
        for page in range(1, max_pages + 1):
            url = BASE_URL_TEMPLATE.format(page)
            print(f"[Page {page}] GET {url}")
            driver.get(url)
            
            # wait for the movie list container
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "section.content div.mlist")))
            except Exception:
                print("No movie list found—assuming end of pages.")
                break
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            anchors = soup.select("section.content div.mlist div.play a[href]")
            if not anchors:
                print("No <a> links found on this page—stopping.")
                break
            
            page_links = [a["href"] for a in anchors]
            print(f"  → Found {len(page_links)} links")
            all_links.extend(page_links)
            
            time.sleep(delay)
        
        if page == max_pages:
            print(f"Reached max_pages ({max_pages}).")
    finally:
        driver.quit()
        print(f"Collected {len(all_links)} total links.")
    
    return all_links