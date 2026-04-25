import asyncio
import base64
import logging
import os
import ssl
from pathlib import Path

import aiohttp
import certifi

APPS_SCRIPT_TOKEN = os.environ.get("APPS_SCRIPT_TOKEN", "")
APPS_SCRIPT_URLS = {
    n: os.environ.get(f"APPS_SCRIPT_URL_{n}", "")
    for n in range(1, 6)
}
# Backward compat: single URL → use for 1-person flow
_legacy = os.environ.get("APPS_SCRIPT_URL", "")
if _legacy and not APPS_SCRIPT_URLS[1]:
    APPS_SCRIPT_URLS[1] = _legacy


def _url_for(people_count: int) -> str:
    url = APPS_SCRIPT_URLS.get(people_count, "")
    if url:
        return url
    # Fall back to URL_1 if specific count URL not set
    return APPS_SCRIPT_URLS.get(1, "")


def _redact(url: str) -> str:
    if not url:
        return "(empty)"
    # Show only deployment ID prefix + suffix to identify which URL is loaded
    if "/macros/s/" in url:
        head, _, tail = url.partition("/macros/s/")
        return head + "/macros/s/" + tail[:14] + "..." + tail[-12:]
    return url[:30] + "..."


import logging as _log
_log.getLogger().info(
    "PDF config: " + ", ".join(f"URL_{n}={_redact(u)}" for n, u in APPS_SCRIPT_URLS.items() if u) +
    f", TOKEN={'set' if APPS_SCRIPT_TOKEN else 'MISSING'}"
)

_ssl_ctx = ssl.create_default_context(cafile=certifi.where())

PDF_FIELDS = (
    "dogovor_no",
    "car_brand", "car_number", "vin",
    "amount", "date_from", "date_to", "dogovor_date",
)

MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2


class PdfError(RuntimeError):
    pass


async def _call_once(url: str, payload: dict) -> bytes:
    connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as sess:
        async with sess.post(url, json=payload, allow_redirects=True) as resp:
            if resp.status != 200:
                raise PdfError(f"Apps Script HTTP {resp.status}")
            body = await resp.json(content_type=None)
    if not body.get("ok"):
        raise PdfError(f"Apps Script: {body.get('error', 'unknown')}")
    pdf_b64 = body.get("pdf_base64")
    if not pdf_b64:
        raise PdfError("Apps Script response missing pdf_base64")
    return base64.b64decode(pdf_b64)


async def generate_pdf(data: dict, output_path: Path) -> Path:
    people_count = int(data.get("people_count", 1))
    url = _url_for(people_count)
    if not url:
        raise PdfError(f"APPS_SCRIPT_URL_{people_count} .env-де берілмеген")

    payload = {"token": APPS_SCRIPT_TOKEN}
    for k in PDF_FIELDS:
        payload[k] = str(data.get(k, ""))
    payload["people_count"] = people_count
    payload["persons"] = [
        {
            "fio": str(p.get("fio", "")),
            "iin": str(p.get("iin", "")),
            "klass": str(p.get("klass", "")),
        }
        for p in data.get("persons", [])
    ]

    last_err: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            pdf_bytes = await _call_once(url, payload)
            output_path.write_bytes(pdf_bytes)
            return output_path
        except (aiohttp.ClientConnectorError, aiohttp.ClientConnectionError,
                asyncio.TimeoutError) as e:
            last_err = e
            logging.warning(f"generate_pdf attempt {attempt} network error: {e}")
            if attempt < MAX_ATTEMPTS:
                await asyncio.sleep(RETRY_DELAY_SECONDS * attempt)
        except PdfError:
            raise
        except Exception as e:
            last_err = e
            logging.warning(f"generate_pdf attempt {attempt} error: {e}")
            if attempt < MAX_ATTEMPTS:
                await asyncio.sleep(RETRY_DELAY_SECONDS * attempt)

    raise PdfError(f"Интернет/желі қатесі ({MAX_ATTEMPTS} рет тырыстым): {last_err}")
