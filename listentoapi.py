import asyncio
import aiohttp
import json
import hashlib
import time
import re
import sys
from urllib.parse import urlparse, parse_qs, urlencode
from typing import Set

# ================= 配置 =================
INTERVAL = 0.001
TIMEOUT = 200
MAX_CONCURRENT = 1500

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bilibili.com/",
    "Origin": "https://www.bilibili.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

MIXIN_KEY_ENC_TAB = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
]

WBI_URLS = [
    "https://api.bilibili.com/x/web-interface/wbi/index/top/feed/rcmd?fresh_type=3&ps=10&fresh_idx=1&fresh_idx_1h=1",
    "https://api.bilibili.com/x/web-interface/wbi/index/top/feed/rcmd?fresh_type=3&ps=10&fresh_idx=2&fresh_idx_1h=2",
    "https://api.bilibili.com/x/web-interface/wbi/index/top/feed/rcmd?fresh_type=3&ps=10&fresh_idx=3&fresh_idx_1h=3",
]

POPULAR_URL = "https://api.bilibili.com/x/web-interface/popular?ps=5&pn=1"

pending: Set[asyncio.Task] = set()
sem = asyncio.Semaphore(MAX_CONCURRENT)
_wbi_cache = {"key": None, "exp": 0}


# ---------- 输出 ----------
def emit(code):
    sys.stdout.write(str(code))
    sys.stdout.flush()


# ---------- WBI ----------
async def get_mixin_key(session):
    now = time.time()
    if _wbi_cache["key"] and now < _wbi_cache["exp"]:
        return _wbi_cache["key"]

    try:
        async with session.get(
            "https://api.bilibili.com/x/web-interface/nav",
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            data = await r.json(content_type=None)
    except Exception:
        return None

    wbi_img = data.get("data", {}).get("wbi_img", {}) if isinstance(data, dict) else {}
    img_url = wbi_img.get("img_url", "")
    sub_url = wbi_img.get("sub_url", "")
    m1 = re.search(r"/([^/]+)\.png", img_url)
    m2 = re.search(r"/([^/]+)\.png", sub_url)

    if not m1 or not m2:
        return None

    raw = m1.group(1) + m2.group(1)
    mixin_key = "".join(raw[i] for i in MIXIN_KEY_ENC_TAB)[:32]
    _wbi_cache["key"] = mixin_key
    _wbi_cache["exp"] = now + 1800
    return mixin_key


def wbi_sign(url, mixin_key):
    parsed = urlparse(url)
    params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    params["wts"] = str(int(time.time()))
    items = sorted(params.items())
    filtered = [(k, re.sub(r"[!'()*]", "", str(v))) for k, v in items]
    query = urlencode(filtered)
    w_rid = hashlib.md5((query + mixin_key).encode()).hexdigest()
    final = urlencode(filtered + [("w_rid", w_rid)])
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{final}"


# ---------- 请求 ----------
async def request_json(session, url):
    try:
        async with session.get(
            url, headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT)
        ) as r:
            text = await r.text()
            try:
                return json.loads(text), None
            except json.JSONDecodeError:
                return None, "非JSON"
    except asyncio.TimeoutError:
        return None, "请求超时"
    except Exception as e:
        return None, type(e).__name__


# ---------- 任务 ----------
async def fetch_wbi(session, url):
    async with sem:
        try:
            key = await get_mixin_key(session)
            if not key:
                emit(-1)
                return
            signed = wbi_sign(url, key)
            data, err = await request_json(session, signed)
            if err:
                emit(-1)
                return
            emit(data.get("code", -1))
        finally:
            pending.discard(asyncio.current_task())


async def fetch_simple(session, url):
    async with sem:
        try:
            data, err = await request_json(session, url)
            if err:
                emit(-1)
                return
            emit(data.get("code", -1))
        finally:
            pending.discard(asyncio.current_task())


# ---------- 主循环 ----------
async def main():
    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300, force_close=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            t = asyncio.create_task(fetch_simple(session, POPULAR_URL))
            pending.add(t)

            for url in WBI_URLS:
                t = asyncio.create_task(fetch_wbi(session, url))
                pending.add(t)

            await asyncio.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except ValueError as e:
        if "Invalid file descriptor" not in str(e):
            raise