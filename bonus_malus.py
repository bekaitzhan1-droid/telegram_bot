"""Класс водителя по ИИН — Eurasia (основной, без капчи) + NSK (запасной, hCaptcha).

Порядок: сначала Eurasia. Если не нашла — NSK с hCaptcha через CapSolver/2captcha.
Формат class на выходе: всегда строка ("5", "M", "0", "9").
"""

import asyncio
import logging
import os
import ssl
import time
from typing import Optional

import aiohttp
import certifi

log = logging.getLogger(__name__)

_ssl_ctx = ssl.create_default_context(cafile=certifi.where())
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)

# ---------- Eurasia ----------
EU_API = "https://eurasia36.kz/Eurasia.API/api"
EU_STA = (
    "31a408997f190debe9c2c187d26fdfe08f1dd346c9dfc7f61215d9970426703d"
    "380cc2a75b26647823571f6d6c3348d773d44f1b1105e84a7fe661d639cb5094"
)
EU_DYN = (
    "904e6cfa8e88f2afc61da952412811bdfe9d1e4013f0014459af2d00dea34dd5"
    "690ae5d36a219fa8237e5475d627aaf04964ff1e5fe69761f6798e77f229ad3e"
)


def _eu_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Bearer {EU_STA}",
        "Origin": "https://eurasia36.kz",
        "Referer": "https://eurasia36.kz/Eurasia/?open=calc",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
    }


def _eu_decrypt(enc: str) -> str:
    n = len(EU_DYN)
    return "".join(chr(ord(c) ^ ord(EU_DYN[i % n])) for i, c in enumerate(enc))


async def _eurasia(iin: str) -> Optional[dict]:
    connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
    timeout = aiohttp.ClientTimeout(total=30)
    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as s:
            # Антибот-гейт перед основным запросом
            try:
                await s.post(
                    f"{EU_API}/Info/CheckRequest",
                    headers=_eu_headers(),
                    json={"IP": "", "Link": "", "Localization": "ru"},
                )
            except Exception:
                pass

            async with s.post(
                f"{EU_API}/Info/GetClient",
                headers=_eu_headers(),
                json={
                    "IIN": iin, "IP": "", "CanFindUL": True, "RequestURL": "",
                    "ProductCode": "OGPO_WEB_PC", "PhoneNumber": None,
                    "RequestReason": "Calculate", "Localization": "ru",
                },
            ) as r:
                if r.status != 200:
                    log.info("eurasia HTTP %s", r.status)
                    return None
                d = await r.json(content_type=None)
        if not d or not d.get("Success") or not d.get("Client"):
            log.info("eurasia not-success: %s", (d or {}).get("Message"))
            return None
        import json as _json
        obj = _json.loads(_eu_decrypt(d["Client"]))
        cls_obj = obj.get("Class") or {}
        name = str(cls_obj.get("NameRu") or cls_obj.get("NameKz") or "")
        klass = name.split()[0] if name else ""
        full = str(obj.get("FullName") or "").strip()
        if not klass:
            return None
        return {"full_name": full, "class": klass}
    except Exception as e:
        log.warning("eurasia error: %s", e)
        return None


# ---------- NSK (hCaptcha) ----------
NSK_URL = "https://www.nsk.kz/api/bonus-malus/{iin}"
NSK_SITEKEY = "2e82f180-fcd2-48ea-814e-5d78c86cc766"
NSK_PAGEURL = "https://www.nsk.kz/ru"

_cached_token: Optional[dict] = None
_inflight: Optional[asyncio.Task] = None
_single_use = False
_lock = asyncio.Lock()


async def _solve_capsolver(session: aiohttp.ClientSession) -> Optional[str]:
    key = os.environ.get("CAPSOLVER_API_KEY", "").strip()
    if not key:
        return None
    try:
        async with session.post(
            "https://api.capsolver.com/createTask",
            json={"clientKey": key, "task": {
                "type": "HCaptchaTaskProxyLess",
                "websiteURL": NSK_PAGEURL,
                "websiteKey": NSK_SITEKEY,
            }},
        ) as r:
            cr = await r.json(content_type=None)
        if cr.get("errorId") or not cr.get("taskId"):
            log.warning("capsolver createTask: %s", cr.get("errorDescription"))
            return None
        task_id = cr["taskId"]
        for _ in range(40):
            await asyncio.sleep(2)
            async with session.post(
                "https://api.capsolver.com/getTaskResult",
                json={"clientKey": key, "taskId": task_id},
            ) as r:
                rr = await r.json(content_type=None)
            if rr.get("errorId"):
                return None
            if rr.get("status") == "ready":
                return (rr.get("solution") or {}).get("gRecaptchaResponse")
        return None
    except Exception as e:
        log.warning("capsolver error: %s", e)
        return None


async def _solve_2captcha(session: aiohttp.ClientSession) -> Optional[str]:
    key = os.environ.get("CAPTCHA_API_KEY", "").strip()
    if not key:
        return None
    try:
        params = {
            "key": key, "method": "hcaptcha",
            "sitekey": NSK_SITEKEY, "pageurl": NSK_PAGEURL, "json": 1,
        }
        async with session.get("https://2captcha.com/in.php", params=params) as r:
            d = await r.json(content_type=None)
        if d.get("status") != 1:
            log.warning("2captcha in: %s", d.get("request"))
            return None
        task_id = d["request"]
        for _ in range(34):
            await asyncio.sleep(5)
            async with session.get(
                "https://2captcha.com/res.php",
                params={"key": key, "action": "get", "id": task_id, "json": 1},
            ) as r:
                d = await r.json(content_type=None)
            if d.get("status") == 1:
                return d.get("request")
            if d.get("request") != "CAPCHA_NOT_READY":
                log.warning("2captcha res: %s", d.get("request"))
                return None
        return None
    except Exception as e:
        log.warning("2captcha error: %s", e)
        return None


def set_manual_token(token: str) -> None:
    """Accept an hCaptcha token solved by a human in the WebApp page."""
    global _cached_token
    _cached_token = {"token": token, "at": time.time()}
    log.info("manual captcha token stored")


def has_token() -> bool:
    return bool(_cached_token) and (time.time() - _cached_token["at"] < 100)


def solver_configured() -> bool:
    return bool(
        os.environ.get("CAPSOLVER_API_KEY", "").strip()
        or os.environ.get("CAPTCHA_API_KEY", "").strip()
    )


async def _obtain_token(session: aiohttp.ClientSession) -> Optional[str]:
    t = await _solve_capsolver(session)
    if t:
        return t
    return await _solve_2captcha(session)


async def _get_token(fresh: bool, session: aiohttp.ClientSession) -> Optional[str]:
    global _cached_token, _inflight
    async with _lock:
        if not fresh and _cached_token and (time.time() - _cached_token["at"] < 100):
            return _cached_token["token"]
        if fresh:
            _cached_token = None
        if not solver_configured():
            return None
        if _inflight is None or _inflight.done():
            _inflight = asyncio.create_task(_obtain_token(session))
        task = _inflight

    token = await task
    async with _lock:
        if token:
            _cached_token = {"token": token, "at": time.time()}
        _inflight = None
    return token


async def _nsk(iin: str) -> Optional[dict]:
    global _single_use, _cached_token
    connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
    timeout = aiohttp.ClientTimeout(total=120)
    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as s:
            for attempt in range(2):
                # Without a solver the only token we have is the human-solved
                # one, so never force-refresh it away.
                reuse = attempt == 0 and not (_single_use and solver_configured())
                token = await _get_token(not reuse, s)
                if not token:
                    log.info("nsk: no captcha token available")
                    return None
                headers = {
                    "accept": "*/*",
                    "referer": "https://www.nsk.kz/ru",
                    "user-agent": UA,
                    "x-captcha-token": token,
                }
                async with s.get(NSK_URL.format(iin=iin), headers=headers) as r:
                    if r.status == 403:
                        if reuse:
                            _single_use = True
                        async with _lock:
                            _cached_token = None
                        continue
                    if r.status != 200:
                        log.info("nsk HTTP %s", r.status)
                        return None
                    d = await r.json(content_type=None)
                if not isinstance(d, dict) or d.get("class") is None:
                    return None
                return {
                    "full_name": str(d.get("full_name") or "").strip(),
                    "class": str(d["class"]),
                }
        return None
    except Exception as e:
        log.warning("nsk error: %s", e)
        return None


async def fetch_bonus_malus(iin: str) -> Optional[dict]:
    """Return {'full_name': str, 'class': str} or None if not found."""
    digits = "".join(ch for ch in str(iin) if ch.isdigit())
    if len(digits) != 12:
        return None

    r = await _eurasia(digits)
    if r and r.get("class"):
        log.info("class found via eurasia: %s", r["class"])
        return r

    r = await _nsk(digits)
    if r and r.get("class"):
        log.info("class found via nsk: %s", r["class"])
        return r

    return None
