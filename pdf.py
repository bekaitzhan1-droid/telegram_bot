import asyncio
import base64
import logging
import os
import ssl
from pathlib import Path

import aiohttp
import certifi

APPS_SCRIPT_URL = os.environ.get("APPS_SCRIPT_URL", "")
APPS_SCRIPT_TOKEN = os.environ.get("APPS_SCRIPT_TOKEN", "")

_ssl_ctx = ssl.create_default_context(cafile=certifi.where())

PDF_FIELDS = (
    "dogovor_no", "fio", "iin", "klass", "phone",
    "car_brand", "car_number", "vin",
    "amount", "date_from", "date_to", "dogovor_date",
)

MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2


class PdfError(RuntimeError):
    pass


async def _call_once(payload: dict) -> bytes:
    connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as sess:
        async with sess.post(APPS_SCRIPT_URL, json=payload, allow_redirects=True) as resp:
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
    if not APPS_SCRIPT_URL:
        raise PdfError("APPS_SCRIPT_URL .env-де берілмеген")

    payload = {"token": APPS_SCRIPT_TOKEN}
    for k in PDF_FIELDS:
        payload[k] = str(data.get(k, ""))

    last_err: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            pdf_bytes = await _call_once(payload)
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
