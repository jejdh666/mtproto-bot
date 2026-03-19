from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import struct
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

import aiohttp
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Sources ───────────────────────────────────────────────────────────────────
# Only verified, actively maintained sources:
#
# 1. SoliSpirit/mtproto — 465⭐, auto-updated every 12h, proxies pre-verified
# 2. Argh94/Proxy-List  — auto-updated every 2-4h, 1000+ proxies, large list
# 3. mtpro.xyz API      — referenced by hookzof (924⭐), updates every 5 min
#
PROXY_SOURCES_TEXT = [
    "https://raw.githubusercontent.com/SoliSpirit/mtproto/master/all_proxies.txt",
    "https://raw.githubusercontent.com/Argh94/Proxy-List/main/MTProto.txt",
]
PROXY_SOURCES_API = [
    "https://mtpro.xyz/api/otherproxies",
]

# ─── Config ────────────────────────────────────────────────────────────────────
REFRESH_INTERVAL = 20 * 60      # 20 min background refresh
STALE_THRESHOLD = 30 * 60       # 30 min before auto-refresh on request
COOLDOWN = 3 * 60               # 3 min per user for /refresh
VALIDATE_TIMEOUT = 5.0
VALIDATE_CONCURRENCY = 80
PROXIES_PER_REQUEST = 5
NOTIFY_INTERVAL = 4 * 60 * 60   # auto-notify subscribers every 4h
NOTIFY_COUNT = 3                # proxies in auto-notification

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
FAVORITES_FILE = DATA_DIR / "favorites.json"
SUBSCRIBERS_FILE = DATA_DIR / "subscribers.json"

# Speed tiers (ms)
SPEED_FAST = 100
SPEED_MEDIUM = 300


# ─── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class MTProxy:
    server: str
    port: int
    secret: str
    is_valid: bool = False
    latency_ms: int = 0
    country: str = ""

    @property
    def tme_link(self) -> str:
        return f"https://t.me/proxy?server={self.server}&port={self.port}&secret={self.secret}"

    def speed_emoji(self) -> str:
        if self.latency_ms <= SPEED_FAST:
            return "🟢"
        if self.latency_ms <= SPEED_MEDIUM:
            return "🟡"
        return "🔴"

    def to_dict(self) -> dict:
        return {"server": self.server, "port": self.port, "secret": self.secret}

    @classmethod
    def from_dict(cls, d: dict) -> MTProxy:
        return cls(server=d["server"], port=d["port"], secret=d["secret"])


@dataclass
class ProxyManager:
    valid_proxies: list[MTProxy] = field(default_factory=list)
    last_refresh: datetime | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def is_stale(self) -> bool:
        if self.last_refresh is None:
            return True
        elapsed = (datetime.now(timezone.utc) - self.last_refresh).total_seconds()
        return elapsed > STALE_THRESHOLD

    def get_filtered(self, speed: str | None = None) -> list[MTProxy]:
        proxies = self.valid_proxies
        if speed == "fast":
            proxies = [p for p in proxies if p.latency_ms <= SPEED_FAST]
        elif speed == "medium":
            proxies = [p for p in proxies if p.latency_ms <= SPEED_MEDIUM]
        return proxies


# ─── Persistence helpers ───────────────────────────────────────────────────────

def _ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_favorites() -> dict[int, list[dict]]:
    try:
        return json.loads(FAVORITES_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_favorites(data: dict[int, list[dict]]):
    _ensure_data_dir()
    FAVORITES_FILE.write_text(json.dumps(data, ensure_ascii=False))


def load_subscribers() -> set[int]:
    try:
        return set(json.loads(SUBSCRIBERS_FILE.read_text()))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def save_subscribers(subs: set[int]):
    _ensure_data_dir()
    SUBSCRIBERS_FILE.write_text(json.dumps(list(subs)))


# ─── Parsing ───────────────────────────────────────────────────────────────────

def parse_proxy_url(url: str) -> MTProxy | None:
    url = url.strip()
    if not url:
        return None
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        server = params.get("server", [None])[0]
        port = params.get("port", [None])[0]
        secret = params.get("secret", [None])[0]
        if not all([server, port, secret]):
            return None
        port_int = int(port)
        if not (1 <= port_int <= 65535):
            return None
        return MTProxy(server=server.rstrip("."), port=port_int, secret=secret)
    except (ValueError, IndexError, TypeError):
        return None


def parse_proxy_json(data: list | dict) -> list[MTProxy]:
    """Parse hookzof/socks5_list JSON format."""
    proxies = []
    items = data if isinstance(data, list) else [data]
    for item in items:
        if isinstance(item, dict):
            server = item.get("host") or item.get("server")
            port = item.get("port")
            secret = item.get("secret")
            if server and port and secret:
                try:
                    proxies.append(MTProxy(server=str(server).rstrip("."), port=int(port), secret=str(secret)))
                except (ValueError, TypeError):
                    pass
    return proxies


# ─── Fetching ──────────────────────────────────────────────────────────────────

async def fetch_text_source(session: aiohttp.ClientSession, url: str) -> list[MTProxy]:
    """Fetch proxies from a text file (one tg://proxy or https://t.me/proxy link per line)."""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                logger.warning(f"Failed to fetch {url}: HTTP {resp.status}")
                return []
            text = await resp.text()
            proxies = []
            for line in text.splitlines():
                proxy = parse_proxy_url(line)
                if proxy:
                    proxies.append(proxy)
            logger.info(f"Fetched {len(proxies)} proxies from {url}")
            return proxies
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return []


async def fetch_api_source(session: aiohttp.ClientSession, url: str) -> list[MTProxy]:
    """Fetch proxies from mtpro.xyz JSON API."""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                logger.warning(f"Failed to fetch API {url}: HTTP {resp.status}")
                return []
            data = await resp.json(content_type=None)
            proxies = parse_proxy_json(data)
            logger.info(f"Fetched {len(proxies)} proxies from API {url}")
            return proxies
    except Exception as e:
        logger.error(f"Error fetching API {url}: {e}")
        return []


# ─── Validation ────────────────────────────────────────────────────────────────

# Suspicious patterns — these are web servers, not MTProto proxies
_SUSPICIOUS_HOSTNAMES = {"http.", "https.", "www.", "api.", "cdn."}


def _is_suspicious_host(server: str) -> bool:
    """Filter out hosts that are obviously web servers, not MTProto proxies."""
    lower = server.lower()
    for prefix in _SUSPICIOUS_HOSTNAMES:
        if lower.startswith(prefix):
            return True
    return False


async def validate_proxy(proxy: MTProxy, timeout: float = VALIDATE_TIMEOUT) -> bool:
    """
    Four-stage validation:
      1) Hostname sanity check — reject obvious web servers
      2) TCP connect — measure latency, reject suspiciously fast (<5ms)
      3) Unsolicited data check — real MTProto proxies WAIT for the client,
         they never send data first. If the server pushes data immediately,
         it's not MTProto (e.g. SSH banner, SMTP greeting, etc.) → reject.
      4) HTTP probe — send an HTTP GET; if the server responds with HTTP,
         it's a web server → reject.
    """
    # Stage 1: hostname filter
    if _is_suspicious_host(proxy.server):
        logger.debug(f"Skipping suspicious host: {proxy.server}")
        proxy.is_valid = False
        return False

    try:
        # Stage 2: TCP connect + latency check
        start = time.monotonic()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(proxy.server, proxy.port),
            timeout=timeout,
        )
        proxy.latency_ms = int((time.monotonic() - start) * 1000)

        # Reject suspiciously fast connections (<5ms) — no real internet
        # proxy responds that fast, it's a load balancer / open port / honeypot
        if proxy.latency_ms < 5:
            logger.debug(f"Suspiciously fast ({proxy.latency_ms}ms), rejecting: {proxy.server}:{proxy.port}")
            proxy.is_valid = False
            writer.close()
            await writer.wait_closed()
            return False

        # Stage 3: check for unsolicited data
        # Real MTProto proxies are silent — they wait for the client to
        # initiate the handshake. Services like SSH, SMTP, FTP, game servers
        # send a banner/greeting immediately → reject.
        try:
            unsolicited = await asyncio.wait_for(reader.read(256), timeout=1.5)
            if unsolicited:
                logger.debug(
                    f"Unsolicited data ({len(unsolicited)}b), not MTProto: "
                    f"{proxy.server}:{proxy.port}"
                )
                proxy.is_valid = False
                writer.close()
                await writer.wait_closed()
                return False
        except asyncio.TimeoutError:
            pass  # No unsolicited data = good, expected for MTProto

        # Stage 4: HTTP probe
        # Send a minimal HTTP request. Real MTProto proxies will NOT reply
        # with HTTP. If we get an HTTP response → it's a web server → reject.
        try:
            http_probe = b"GET / HTTP/1.0\r\nHost: check\r\n\r\n"
            writer.write(http_probe)
            await writer.drain()

            try:
                response = await asyncio.wait_for(reader.read(512), timeout=2.0)
                if response:
                    resp_lower = response[:256].lower()
                    if (
                        response[:4] == b"HTTP"
                        or b"<html" in resp_lower
                        or b"<!doc" in resp_lower
                        or b"text/html" in resp_lower
                        or b"nginx" in resp_lower
                        or b"apache" in resp_lower
                        or b"server:" in resp_lower
                        or b"content-type" in resp_lower
                    ):
                        logger.debug(f"HTTP response detected: {proxy.server}:{proxy.port}")
                        proxy.is_valid = False
                        writer.close()
                        await writer.wait_closed()
                        return False
            except asyncio.TimeoutError:
                pass  # No HTTP response = good, likely MTProto

        except (OSError, ConnectionError):
            # Connection reset after probe — fine for MTProto
            pass

        try:
            writer.close()
            await writer.wait_closed()
        except (OSError, ConnectionError):
            pass

        proxy.is_valid = True
        return True

    except (OSError, asyncio.TimeoutError, OverflowError, ConnectionError):
        proxy.is_valid = False
        return False


async def validate_batch(proxies: list[MTProxy]) -> list[MTProxy]:
    sem = asyncio.Semaphore(VALIDATE_CONCURRENCY)

    async def check(p: MTProxy):
        async with sem:
            await validate_proxy(p)

    await asyncio.gather(*(check(p) for p in proxies))
    return [p for p in proxies if p.is_valid]


def deduplicate(proxies: list[MTProxy]) -> list[MTProxy]:
    seen: set[tuple[str, int]] = set()
    result = []
    for p in proxies:
        key = (p.server.lower(), p.port)
        if key not in seen:
            seen.add(key)
            result.append(p)
    return result


# ─── GeoIP (best-effort) ──────────────────────────────────────────────────────

async def enrich_country(session: aiohttp.ClientSession, proxies: list[MTProxy]):
    """Batch GeoIP lookup — best effort, failures silently ignored."""
    if not proxies:
        return
    # ip-api.com free batch: max 100
    batch = proxies[:100]
    payload = [{"query": p.server, "fields": "countryCode"} for p in batch]
    try:
        async with session.post(
            "http://ip-api.com/batch?fields=countryCode",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status == 200:
                results = await resp.json()
                for proxy, info in zip(batch, results):
                    if isinstance(info, dict):
                        proxy.country = info.get("countryCode", "")
    except Exception as e:
        logger.debug(f"GeoIP enrichment failed: {e}")


# ─── Refresh ───────────────────────────────────────────────────────────────────

async def refresh_proxies(manager: ProxyManager) -> None:
    async with manager._lock:
        logger.info("Refreshing proxy list...")
        all_proxies: list[MTProxy] = []
        async with aiohttp.ClientSession() as session:
            # Fetch from text sources and API sources in parallel
            tasks = []
            for url in PROXY_SOURCES_TEXT:
                tasks.append(fetch_text_source(session, url))
            for url in PROXY_SOURCES_API:
                tasks.append(fetch_api_source(session, url))

            results = await asyncio.gather(*tasks)
            for batch in results:
                all_proxies.extend(batch)

            all_proxies = deduplicate(all_proxies)
            logger.info(f"Total unique proxies: {len(all_proxies)}, validating...")

            valid = await validate_batch(all_proxies)
            valid.sort(key=lambda p: p.latency_ms)

            # GeoIP enrichment on validated proxies
            await enrich_country(session, valid)

        manager.valid_proxies = valid
        manager.last_refresh = datetime.now(timezone.utc)
        logger.info(f"Validation complete: {len(valid)} working proxies")


# ─── Formatting helpers ───────────────────────────────────────────────────────

async def safe_edit(message: Message, text: str, **kwargs):
    """Edit message, silently ignoring 'message is not modified' error."""
    try:
        await message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass  # same content, ignore
        else:
            raise


def escape_md(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    specials = r"_*[]()~`>#+-=|{}.!\\"
    result = []
    for ch in text:
        if ch in specials:
            result.append(f"\\{ch}")
        else:
            result.append(ch)
    return "".join(result)


def _proxy_key(server: str, port: int) -> str:
    """Short unique key for callback data (max 64 bytes in Telegram)."""
    import hashlib
    raw = f"{server}:{port}"
    return hashlib.md5(raw.encode()).hexdigest()[:10]


def format_proxy_line(i: int, p: MTProxy) -> str:
    emoji = p.speed_emoji()
    display = escape_md(f"{p.server}:{p.port}")
    country = f" {p.country}" if p.country else ""
    return f"{i}\\. {emoji} [{display}]({p.tme_link}) — {p.latency_ms}ms{escape_md(country)}"


def format_proxy_list(proxies: list[MTProxy], last_refresh: datetime | None) -> str:
    lines = [format_proxy_line(i, p) for i, p in enumerate(proxies, 1)]
    updated = last_refresh.strftime("%H:%M:%S UTC") if last_refresh else "?"
    text = "⚡ *Рабочие MTProto прокси:*\n\n" + "\n".join(lines)
    text += f"\n\n_Обновлено: {escape_md(updated)}_"
    return text


def proxy_list_keyboard(proxies: list[MTProxy]) -> InlineKeyboardMarkup:
    """Keyboard with a ⭐ save button per proxy + navigation."""
    rows: list[list[InlineKeyboardButton]] = []

    # Save buttons row — one per proxy
    save_buttons = []
    for i, p in enumerate(proxies, 1):
        key = _proxy_key(p.server, p.port)
        save_buttons.append(
            InlineKeyboardButton(text=f"⭐ {i}", callback_data=f"save:{key}")
        )
    rows.append(save_buttons)

    # Navigation
    rows.append([
        InlineKeyboardButton(text="🔄 Обновить", callback_data="cb_refresh"),
        InlineKeyboardButton(text="📊 Статус", callback_data="cb_status"),
    ])
    rows.append([
        InlineKeyboardButton(text="🚀 Быстрые (<100ms)", callback_data="cb_fast"),
        InlineKeyboardButton(text="⭐ Избранное", callback_data="cb_favorites"),
    ])
    rows.append([
        InlineKeyboardButton(text="🔔 Подписка", callback_data="cb_subscribe"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def main_keyboard() -> InlineKeyboardMarkup:
    """Keyboard without save buttons (for non-proxy-list messages)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data="cb_refresh"),
            InlineKeyboardButton(text="📊 Статус", callback_data="cb_status"),
        ],
        [
            InlineKeyboardButton(text="🚀 Быстрые (<100ms)", callback_data="cb_fast"),
            InlineKeyboardButton(text="⭐ Избранное", callback_data="cb_favorites"),
        ],
        [
            InlineKeyboardButton(text="🔔 Подписка", callback_data="cb_subscribe"),
        ],
    ])


def favorites_keyboard(proxies: list[MTProxy]) -> InlineKeyboardMarkup:
    """Keyboard with 🗑 delete buttons per favorite + clear all."""
    rows: list[list[InlineKeyboardButton]] = []

    del_buttons = []
    for i, p in enumerate(proxies, 1):
        key = _proxy_key(p.server, p.port)
        del_buttons.append(
            InlineKeyboardButton(text=f"🗑 {i}", callback_data=f"delfav:{key}")
        )
    rows.append(del_buttons)
    rows.append([
        InlineKeyboardButton(text="🗑 Очистить всё", callback_data="cb_clearfavs"),
        InlineKeyboardButton(text="« Назад", callback_data="cb_back"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─── Bot setup ─────────────────────────────────────────────────────────────────

router = Router()
manager = ProxyManager()
refresh_cooldowns: dict[int, datetime] = {}
bot_instance: Bot | None = None


# /start
@router.message(Command("start"))
async def cmd_start(message: Message):
    text = (
        "⚡ *MTProto Proxy Bot*\n\n"
        "Нахожу, проверяю и отдаю рабочие MTProto\\-прокси для Telegram\\.\n\n"
        "📋 *Команды:*\n"
        "/proxy — получить 5 лучших прокси\n"
        "/fast — только быстрые \\(<100ms\\)\n"
        "/refresh — принудительно обновить\n"
        "/status — статистика\n"
        "/subscribe — авто\\-уведомления каждые 4ч\n"
        "/unsubscribe — отключить уведомления\n"
        "/favorites — мои избранные\n\n"
        "⭐ _Нажми кнопку 1–5 под списком прокси чтобы сохранить в избранное_\n"
        "🗑 _В избранном — кнопки удаления для каждого прокси_\n\n"
        "_Или используй кнопки ниже_ 👇"
    )
    await message.answer(text, reply_markup=main_keyboard(), disable_web_page_preview=True)


# Store last shown proxies per chat so save buttons can find them
_last_shown: dict[int, list[MTProxy]] = {}


# /proxy [fast|medium]
@router.message(Command("proxy", "get"))
async def cmd_proxy(message: Message, command: CommandObject | None = None):
    speed_filter = None
    if command and command.args:
        arg = command.args.strip().lower()
        if arg in ("fast", "medium"):
            speed_filter = arg

    await _send_proxies(message, speed_filter=speed_filter)


# /fast shortcut
@router.message(Command("fast"))
async def cmd_fast(message: Message):
    await _send_proxies(message, speed_filter="fast")


async def _send_proxies(message: Message, speed_filter: str | None = None, edit: bool = False):
    if manager.is_stale() or not manager.valid_proxies:
        wait_msg = await message.answer("⏳ Загружаю свежие прокси, подожди\\.\\.\\.")
        await refresh_proxies(manager)
        try:
            await wait_msg.delete()
        except Exception:
            pass

    proxies = manager.get_filtered(speed_filter)
    if not proxies:
        label = f" ({speed_filter})" if speed_filter else ""
        await message.answer(
            f"Рабочих прокси{label} не найдено. Попробуй /refresh или убери фильтр.",
            parse_mode=None,
        )
        return

    count = min(PROXIES_PER_REQUEST, len(proxies))
    selected = proxies[:count]
    text = format_proxy_list(selected, manager.last_refresh)

    if speed_filter:
        text += f"\n\n_Фильтр: {escape_md(speed_filter)}_"

    # Remember shown proxies so ⭐ buttons can reference them
    _last_shown[message.chat.id] = selected

    await message.answer(text, reply_markup=proxy_list_keyboard(selected), disable_web_page_preview=True)


# /refresh
@router.message(Command("refresh"))
async def cmd_refresh(message: Message):
    user_id = message.from_user.id
    now = datetime.now(timezone.utc)

    last = refresh_cooldowns.get(user_id)
    if last and (now - last).total_seconds() < COOLDOWN:
        remaining = int(COOLDOWN - (now - last).total_seconds())
        await message.answer(f"⏳ Подожди {remaining} сек. перед повторным обновлением.", parse_mode=None)
        return

    refresh_cooldowns[user_id] = now
    wait_msg = await message.answer("🔄 Обновляю список прокси...")
    await refresh_proxies(manager)
    try:
        await wait_msg.delete()
    except Exception:
        pass
    n = len(manager.valid_proxies)
    await message.answer(
        f"✅ Готово\\! Найдено *{n}* рабочих прокси\\.\n\nЖми /proxy или кнопку ниже 👇",
        reply_markup=main_keyboard(),
    )


# /status
@router.message(Command("status"))
async def cmd_status(message: Message):
    subs = load_subscribers()
    await message.answer(_status_text(subs), parse_mode=None)


def _status_text(subs: set[int] | None = None) -> str:
    if manager.last_refresh:
        updated = manager.last_refresh.strftime("%Y-%m-%d %H:%M UTC")
    else:
        updated = "ещё не обновлялось"

    total = len(manager.valid_proxies)
    fast = len([p for p in manager.valid_proxies if p.latency_ms <= SPEED_FAST])
    medium = len([p for p in manager.valid_proxies if p.latency_ms <= SPEED_MEDIUM])

    countries = {}
    for p in manager.valid_proxies:
        if p.country:
            countries[p.country] = countries.get(p.country, 0) + 1
    top_countries = sorted(countries.items(), key=lambda x: -x[1])[:5]
    country_str = ", ".join(f"{c}: {n}" for c, n in top_countries) if top_countries else "нет данных"

    sub_count = len(subs) if subs else 0

    return (
        f"📊 Статистика\n\n"
        f"Всего рабочих: {total}\n"
        f"🟢 Быстрые (<{SPEED_FAST}ms): {fast}\n"
        f"🟡 Средние (<{SPEED_MEDIUM}ms): {medium}\n"
        f"🔴 Медленные: {total - medium}\n\n"
        f"🌍 Топ стран: {country_str}\n"
        f"📡 Источников: {len(PROXY_SOURCES_TEXT) + len(PROXY_SOURCES_API)}\n"
        f"🔔 Подписчиков: {sub_count}\n"
        f"Обновлено: {updated}"
    )


# ─── Subscribe / Unsubscribe ──────────────────────────────────────────────────

@router.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    subs = load_subscribers()
    uid = message.from_user.id
    if uid in subs:
        await message.answer("Ты уже подписан! 🔔 Уведомления каждые 4 часа.", parse_mode=None)
        return
    subs.add(uid)
    save_subscribers(subs)
    await message.answer(
        "✅ Подписка оформлена!\n"
        "Буду присылать лучшие прокси каждые 4 часа.\n"
        "Отписаться: /unsubscribe",
        parse_mode=None,
    )


@router.message(Command("unsubscribe"))
async def cmd_unsubscribe(message: Message):
    subs = load_subscribers()
    uid = message.from_user.id
    if uid not in subs:
        await message.answer("Ты не подписан.", parse_mode=None)
        return
    subs.discard(uid)
    save_subscribers(subs)
    await message.answer("🔕 Подписка отменена.", parse_mode=None)


# ─── Favorites ─────────────────────────────────────────────────────────────────

@router.message(Command("favorites", "favs"))
async def cmd_favorites(message: Message):
    await _show_favorites(message)


async def _show_favorites(target: Message | CallbackQuery):
    """Show favorites — works from both /favorites command and ⭐ button."""
    if isinstance(target, CallbackQuery):
        uid = str(target.from_user.id)
    else:
        uid = str(target.from_user.id)

    favs = load_favorites()
    user_favs = favs.get(uid, [])

    if not user_favs:
        text = "У тебя пока нет избранных прокси\\.\n\nНажми ⭐ 1–5 под списком прокси чтобы сохранить\\."
        if isinstance(target, CallbackQuery):
            await safe_edit(target.message,text, reply_markup=main_keyboard())
        else:
            await target.answer(text, reply_markup=main_keyboard())
        return

    # Re-validate favorites
    proxies = [MTProxy.from_dict(d) for d in user_favs]
    valid = await validate_batch(proxies)

    all_proxies = proxies  # keep all for delete buttons
    if valid:
        valid.sort(key=lambda p: p.latency_ms)

    # Show all saved (mark dead ones)
    lines = []
    display_proxies = []
    for i, d in enumerate(user_favs, 1):
        p = MTProxy.from_dict(d)
        # check if this one is in valid list
        is_alive = any(v.server == p.server and v.port == p.port for v in valid)
        if is_alive:
            matched = next(v for v in valid if v.server == p.server and v.port == p.port)
            lines.append(format_proxy_line(i, matched))
        else:
            display = escape_md(f"{p.server}:{p.port}")
            lines.append(f"{i}\\. ❌ [{display}]({p.tme_link}) — не работает")
        display_proxies.append(p)

    working = len(valid)
    total = len(user_favs)
    text = f"⭐ *Избранные прокси* \\({working}/{total} работают\\):\n\n" + "\n".join(lines)

    kb = favorites_keyboard(display_proxies)
    if isinstance(target, CallbackQuery):
        await safe_edit(target.message,text, reply_markup=kb, disable_web_page_preview=True)
    else:
        await target.answer(text, reply_markup=kb, disable_web_page_preview=True)


@router.message(Command("delfav", "clearfav"))
async def cmd_clear_favorites(message: Message):
    favs = load_favorites()
    uid = str(message.from_user.id)
    if uid in favs:
        del favs[uid]
        save_favorites(favs)
    await message.answer("🗑 Избранное очищено.", parse_mode=None)


# ─── Inline callbacks ─────────────────────────────────────────────────────────

@router.callback_query(F.data == "cb_refresh")
async def cb_refresh(callback: CallbackQuery):
    user_id = callback.from_user.id
    now = datetime.now(timezone.utc)

    last = refresh_cooldowns.get(user_id)
    if last and (now - last).total_seconds() < COOLDOWN:
        remaining = int(COOLDOWN - (now - last).total_seconds())
        await callback.answer(f"Подожди {remaining} сек.", show_alert=True)
        return

    refresh_cooldowns[user_id] = now
    await callback.answer("Обновляю...")
    await safe_edit(callback.message,"🔄 Обновляю список прокси\\.\\.\\.")
    await refresh_proxies(manager)

    proxies = manager.valid_proxies[:PROXIES_PER_REQUEST]
    if proxies:
        _last_shown[callback.message.chat.id] = proxies
        text = format_proxy_list(proxies, manager.last_refresh)
        await safe_edit(callback.message,text, reply_markup=proxy_list_keyboard(proxies), disable_web_page_preview=True)
    else:
        await safe_edit(callback.message,
            "Рабочих прокси не найдено\\. Попробуй позже\\.",
            reply_markup=main_keyboard(),
        )


@router.callback_query(F.data == "cb_status")
async def cb_status(callback: CallbackQuery):
    subs = load_subscribers()
    await callback.answer()
    await safe_edit(callback.message,
        escape_md(_status_text(subs)),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="« Назад к прокси", callback_data="cb_back")],
        ]),
    )


@router.callback_query(F.data == "cb_fast")
async def cb_fast(callback: CallbackQuery):
    await callback.answer()
    proxies = manager.get_filtered("fast")
    if not proxies:
        await callback.answer("Нет быстрых прокси (<100ms). Попробуй обновить.", show_alert=True)
        return
    selected = proxies[:PROXIES_PER_REQUEST]
    _last_shown[callback.message.chat.id] = selected
    text = format_proxy_list(selected, manager.last_refresh)
    text += f"\n\n_Фильтр: только быстрые \\(<100ms\\)_"
    await safe_edit(callback.message,text, reply_markup=proxy_list_keyboard(selected), disable_web_page_preview=True)


@router.callback_query(F.data == "cb_favorites")
async def cb_favorites(callback: CallbackQuery):
    await callback.answer()
    await _show_favorites(callback)


# ⭐ Save a specific proxy from the list
@router.callback_query(F.data.startswith("save:"))
async def cb_save_proxy(callback: CallbackQuery):
    key = callback.data.split(":", 1)[1]
    chat_id = callback.message.chat.id
    shown = _last_shown.get(chat_id, [])

    # Find the proxy by hash key
    target = None
    for p in shown:
        if _proxy_key(p.server, p.port) == key:
            target = p
            break

    if not target:
        await callback.answer("Прокси не найден. Обнови список.", show_alert=True)
        return

    uid = str(callback.from_user.id)
    favs = load_favorites()
    user_favs = favs.get(uid, [])

    # Check duplicate
    for f in user_favs:
        if f["server"] == target.server and f["port"] == target.port:
            await callback.answer("Уже в избранном!", show_alert=False)
            return

    if len(user_favs) >= 20:
        await callback.answer("Максимум 20 избранных. Удали старые.", show_alert=True)
        return

    user_favs.append(target.to_dict())
    favs[uid] = user_favs
    save_favorites(favs)
    await callback.answer(f"⭐ Сохранено: {target.server}:{target.port}", show_alert=False)


# 🗑 Delete a specific proxy from favorites
@router.callback_query(F.data.startswith("delfav:"))
async def cb_del_fav(callback: CallbackQuery):
    key = callback.data.split(":", 1)[1]
    uid = str(callback.from_user.id)
    favs = load_favorites()
    user_favs = favs.get(uid, [])

    # Find and remove
    new_favs = []
    removed = False
    for f in user_favs:
        if _proxy_key(f["server"], f["port"]) == key and not removed:
            removed = True
            continue
        new_favs.append(f)

    if not removed:
        await callback.answer("Не найдено.", show_alert=False)
        return

    favs[uid] = new_favs
    save_favorites(favs)
    await callback.answer("🗑 Удалено из избранного", show_alert=False)

    # Refresh the favorites view
    await _show_favorites(callback)


# 🗑 Clear all favorites
@router.callback_query(F.data == "cb_clearfavs")
async def cb_clear_favs(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    favs = load_favorites()
    if uid in favs and favs[uid]:
        del favs[uid]
        save_favorites(favs)
        await callback.answer("🗑 Избранное очищено", show_alert=True)
    else:
        await callback.answer("Избранное уже пусто", show_alert=False)

    # Go back to proxy list
    await _cb_go_back(callback)


@router.callback_query(F.data == "cb_subscribe")
async def cb_subscribe(callback: CallbackQuery):
    subs = load_subscribers()
    uid = callback.from_user.id
    if uid in subs:
        subs.discard(uid)
        save_subscribers(subs)
        await callback.answer("🔕 Подписка отменена", show_alert=True)
    else:
        subs.add(uid)
        save_subscribers(subs)
        await callback.answer("🔔 Подписка оформлена! Уведомления каждые 4ч", show_alert=True)


async def _cb_go_back(callback: CallbackQuery):
    """Shared logic: go back to proxy list."""
    if not manager.valid_proxies:
        await safe_edit(callback.message,
            "Прокси ещё не загружены\\. Нажми 🔄 Обновить\\.",
            reply_markup=main_keyboard(),
        )
        return
    proxies = manager.valid_proxies[:PROXIES_PER_REQUEST]
    _last_shown[callback.message.chat.id] = proxies
    text = format_proxy_list(proxies, manager.last_refresh)
    await safe_edit(callback.message,text, reply_markup=proxy_list_keyboard(proxies), disable_web_page_preview=True)


@router.callback_query(F.data == "cb_back")
async def cb_back(callback: CallbackQuery):
    await callback.answer()
    await _cb_go_back(callback)


# ─── Background tasks ─────────────────────────────────────────────────────────

async def periodic_refresh():
    while True:
        await asyncio.sleep(REFRESH_INTERVAL)
        try:
            await refresh_proxies(manager)
        except Exception as e:
            logger.error(f"Background refresh failed: {e}")


async def periodic_notify(bot: Bot):
    """Send proxy updates to subscribers every NOTIFY_INTERVAL."""
    while True:
        await asyncio.sleep(NOTIFY_INTERVAL)
        try:
            subs = load_subscribers()
            if not subs or not manager.valid_proxies:
                continue

            proxies = manager.valid_proxies[:NOTIFY_COUNT]
            text = "🔔 *Автообновление прокси:*\n\n"
            text += "\n".join(format_proxy_line(i, p) for i, p in enumerate(proxies, 1))
            text += f"\n\n_Следующее обновление через {NOTIFY_INTERVAL // 3600}ч_"

            dead_subs = set()
            for uid in subs:
                try:
                    await bot.send_message(
                        uid, text,
                        disable_web_page_preview=True,
                        reply_markup=main_keyboard(),
                    )
                except Exception as e:
                    logger.warning(f"Failed to notify {uid}: {e}")
                    # If blocked by user, remove from subscribers
                    if "blocked" in str(e).lower() or "deactivated" in str(e).lower():
                        dead_subs.add(uid)

            if dead_subs:
                subs -= dead_subs
                save_subscribers(subs)
                logger.info(f"Removed {len(dead_subs)} dead subscribers")

        except Exception as e:
            logger.error(f"Notification task failed: {e}")


# ─── Startup ───────────────────────────────────────────────────────────────────

async def on_startup(bot: Bot):
    global bot_instance
    bot_instance = bot
    logger.info("Bot starting, loading initial proxies...")
    _ensure_data_dir()
    await refresh_proxies(manager)
    asyncio.create_task(periodic_refresh())
    asyncio.create_task(periodic_notify(bot))
    logger.info("Bot is ready!")


async def main():
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN environment variable is not set")

    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2))
    dp = Dispatcher()
    dp.include_router(router)
    dp.startup.register(on_startup)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
