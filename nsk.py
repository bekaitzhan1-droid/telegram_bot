import ssl

import aiohttp
import certifi

NSK_URL = "https://new.nsk.kz/api/bonus-malus/{iin}"

_ssl_ctx = ssl.create_default_context(cafile=certifi.where())


async def fetch_bonus_malus(iin: str) -> dict | None:
    """Return {'full_name': str, 'class': str} or None if not found / error."""
    url = NSK_URL.format(iin=iin)
    connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as sess:
            async with sess.get(url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                if not isinstance(data, dict) or "full_name" not in data or "class" not in data:
                    return None
                return {"full_name": str(data["full_name"]).strip(), "class": str(data["class"])}
    except Exception:
        return None
