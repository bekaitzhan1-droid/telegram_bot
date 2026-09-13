"""Tiny HTTP server serving a manual hCaptcha page for the NSK fallback.

Flow: bot opens /captcha?k=<key> in a Telegram WebApp → operator solves the
widget → page POSTs the token to /captcha/submit → token lands in the
bonus_malus cache → bot retries the NSK lookup.
"""

import logging
import os
import secrets
import time

from aiohttp import web

import bonus_malus

log = logging.getLogger(__name__)

SITEKEY = bonus_malus.NSK_SITEKEY
KEY_TTL = 600

_sessions: dict[str, dict] = {}


def new_session(user_id: int) -> str:
    _prune()
    key = secrets.token_urlsafe(16)
    _sessions[key] = {"user_id": user_id, "at": time.time()}
    return key


def _prune() -> None:
    now = time.time()
    for k in [k for k, v in _sessions.items() if now - v["at"] > KEY_TTL]:
        _sessions.pop(k, None)


def public_url(key: str) -> str | None:
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    if not domain:
        return None
    return f"https://{domain}/captcha?k={key}"


PAGE = """<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Проверка</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script src="https://js.hcaptcha.com/1/api.js" async defer></script>
<style>
  body {{ margin:0; padding:24px 16px; font:15px/1.5 -apple-system,system-ui,sans-serif;
         display:flex; flex-direction:column; align-items:center; gap:18px;
         background:var(--tg-theme-bg-color,#fff); color:var(--tg-theme-text-color,#111); }}
  h1 {{ font-size:17px; margin:0; text-align:center; }}
  p  {{ margin:0; text-align:center; color:var(--tg-theme-hint-color,#666); font-size:13px; }}
  #status {{ font-size:14px; min-height:20px; text-align:center; }}
  .ok  {{ color:#1a9c46; }}
  .err {{ color:#c0392b; }}
</style></head>
<body>
  <h1>Подтвердите, что вы не робот</h1>
  <p>Это нужно один раз — дальше поиск пойдёт автоматически несколько минут.</p>
  <div class="h-captcha" data-sitekey="{sitekey}" data-callback="onSolved"
       data-expired-callback="onExpired" data-error-callback="onExpired"></div>
  <div id="status"></div>
<script>
  var tg = window.Telegram && window.Telegram.WebApp;
  if (tg) {{ tg.ready(); tg.expand(); }}
  var KEY = {key_json};
  function say(t, cls) {{
    var el = document.getElementById('status');
    el.textContent = t; el.className = cls || '';
  }}
  function onExpired() {{ say('Проверка истекла, попробуйте ещё раз.', 'err'); }}
  function onSolved(token) {{
    say('Отправляю…');
    fetch('/captcha/submit', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ k: KEY, token: token }})
    }})
    .then(function (r) {{ return r.json(); }})
    .then(function (d) {{
      if (!d.ok) {{ say(d.error || 'Не получилось, повторите.', 'err'); return; }}
      say('Готово!', 'ok');
      if (tg) {{ tg.sendData('solved'); }} else {{ setTimeout(function(){{ window.close(); }}, 800); }}
    }})
    .catch(function () {{ say('Сеть недоступна, повторите.', 'err'); }});
  }}
</script>
</body></html>"""


async def _page(request: web.Request) -> web.Response:
    key = request.query.get("k", "")
    _prune()
    if key not in _sessions:
        return web.Response(status=404, text="Ссылка устарела. Запросите новую в боте.")
    import json as _json
    html = PAGE.format(sitekey=SITEKEY, key_json=_json.dumps(key))
    return web.Response(text=html, content_type="text/html")


async def _submit(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad json"}, status=400)

    key = str(body.get("k") or "")
    token = str(body.get("token") or "")
    _prune()
    sess = _sessions.get(key)
    if not sess:
        return web.json_response({"ok": False, "error": "Ссылка устарела"}, status=403)
    if len(token) < 20:
        return web.json_response({"ok": False, "error": "Пустой токен"}, status=400)

    _sessions.pop(key, None)
    bonus_malus.set_manual_token(token)
    log.info("captcha token accepted from user %s", sess["user_id"])
    return web.json_response({"ok": True})


async def _health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def start_webserver() -> None:
    app = web.Application()
    app.router.add_get("/captcha", _page)
    app.router.add_post("/captcha/submit", _submit)
    app.router.add_get("/health", _health)
    app.router.add_get("/", _health)

    port = int(os.environ.get("PORT", "8080"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("webapp listening on :%s (domain=%s)", port,
             os.environ.get("RAILWAY_PUBLIC_DOMAIN") or "not set")
