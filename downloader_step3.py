import asyncio
import httpx
import aiofiles
import os
from tqdm import tqdm

async def download_movie(url: str, dest_path: str, headers: dict) -> None:
    """
    Streams a single URL into dest_path (creating parent dir if needed).
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    # timeout = httpx.Timeout(10.0, 300.0)
    async with httpx.AsyncClient(timeout=None) as client:
        async with client.stream("GET", url, headers=headers) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            print(f"📦 Total file size: {total/(1024*1024):.2f}MB for {os.path.basename(dest_path)}")

            pbar = tqdm(total=total, unit="B", unit_scale=True, desc=os.path.basename(dest_path))
            async with aiofiles.open(dest_path, "wb") as f:
                async for chunk in resp.aiter_bytes():
                    if not chunk:
                        break
                    await f.write(chunk)
                    pbar.update(len(chunk))
            pbar.close()
    print(f"✅ Finished {os.path.basename(dest_path)}")
