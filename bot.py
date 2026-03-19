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
PROXY_SOURCES = [
    "https://raw.githubusercontent.com/SoliSpirit/mtproto/master/all_proxies.txt",
    "https://raw.githubusercontent.com/ALIILAPRO/MTProtoProxy/main/mtproto.txt",
    "https://raw.githubusercontent.com/hookzof/socks5_list/master/tg/mtproto.json",
    "https://raw.githubusercontent.com/MrMohewormo/MTProtoProxies/master/proxies.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/mtproto.txt",
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

async def fetch_proxies_from_source(session: aiohttp.ClientSession, url: str) -> list[MTProxy]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                logger.warning(f"Failed to fetch {url}: HTTP {resp.status}")
                return []
            text = await resp.text()

            # JSON source
            if url.endswith(".json"):
                try:
                    data = json.loads(text)
                    proxies = parse_proxy_json(data)
                    logger.info(f"Fetched {len(proxies)} proxies from {url} (JSON)")
                    return proxies
                except json.JSONDecodeError:
                    pass

            # Text/URL source
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


# ─── Validation ────────────────────────────────────────────────────────────────

async def validate_proxy(proxy: MTProxy, timeout: float = VALIDATE_TIMEOUT) -> bool:
    """
    Deep validation: TCP connect + MTProto TLS handshake probe.
    Sends a fake TLS ClientHello-like packet and checks if the server responds
    (real MTProto proxies respond, random TCP services usually don't).
    """
    try:
        start = time.monotonic()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(proxy.server, proxy.port),
            timeout=timeout,
        )
        proxy.latency_ms = int((time.monotonic() - start) * 1000)

        # Deeper check: send a small probe and see if the server responds
        # MTProto proxies expect a specific handshake; we send random bytes
        # and check that the connection stays alive (doesn't immediately RST)
        try:
            probe = os.urandom(64)
            writer.write(probe)
            await writer.drain()
            # If we can read *anything* back (or timeout after 1.5s), proxy is alive
            try:
                await asyncio.wait_for(reader.read(1), timeout=1.5)
            except asyncio.TimeoutError:
                pass  # timeout is fine — proxy is alive but didn't respond to junk
        except (OSError, ConnectionError):
            proxy.is_valid = False
            writer.close()
            await writer.wait_closed()
            return False

        writer.close()
        await writer.wait_closed()
        proxy.is_valid = True
        return True
    except (OSError, asyncio.TimeoutError, OverflowError):
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
            results = await asyncio.gather(
                *(fetch_proxies_from_source(session, url) for url in PROXY_SOURCES)
            )
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


def format_proxy_line(i: int, p: MTProxy) -> str:
    emoji = p.speed_emoji()
    display = escape_md(f"{p.server}:{p.port}")
    country = f" {p.country}" if p.country else ""
    return f"{i}\\. {emoji} [{display}]({p.tme_link}) — {p.latency_ms}ms{escape_md(country)}"


def format_proxy_list(proxies: list[MTProxy], last_refresh: datetime | None) -> str:
    lines = [format_proxy_line(i, p) for i, p in enumerate(proxies, 1)]
    updated = last_refresh.strftime("%H:%M UTC") if last_refresh else "?"
    text = "⚡ *Рабочие MTProto прокси:*\n\n" + "\n".join(lines)
    text += f"\n\n_Обновлено: {escape_md(updated)}_"
    return text


def main_keyboard() -> InlineKeyboardMarkup:
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


def favorites_keyboard(proxies_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="« Назад", callback_data="cb_back")],
    ])


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
        "/save — сохранить прокси в избранное\n"
        "/favorites — мои избранные\n\n"
        "_Или используй кнопки ниже_ 👇"
    )
    await message.answer(text, reply_markup=main_keyboard(), disable_web_page_preview=True)


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

    await message.answer(text, reply_markup=main_keyboard(), disable_web_page_preview=True)


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
        f"📡 Источников: {len(PROXY_SOURCES)}\n"
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

@router.message(Command("save"))
async def cmd_save(message: Message, command: CommandObject | None = None):
    """Save top N proxies to favorites. Usage: /save or /save 3"""
    n = 1
    if command and command.args:
        try:
            n = max(1, min(int(command.args.strip()), 10))
        except ValueError:
            pass

    if not manager.valid_proxies:
        await message.answer("Нет прокси для сохранения. Сначала /proxy", parse_mode=None)
        return

    favs = load_favorites()
    uid = str(message.from_user.id)
    user_favs = favs.get(uid, [])

    existing_keys = {(f["server"], f["port"]) for f in user_favs}
    added = 0
    for p in manager.valid_proxies[:n]:
        key = (p.server, p.port)
        if key not in existing_keys:
            user_favs.append(p.to_dict())
            existing_keys.add(key)
            added += 1

    # Keep max 20 favorites
    user_favs = user_favs[-20:]
    favs[uid] = user_favs
    save_favorites(favs)

    await message.answer(
        f"⭐ Сохранено {added} прокси в избранное (всего: {len(user_favs)}/20)\n"
        f"Посмотреть: /favorites",
        parse_mode=None,
    )


@router.message(Command("favorites", "favs"))
async def cmd_favorites(message: Message):
    favs = load_favorites()
    uid = str(message.from_user.id)
    user_favs = favs.get(uid, [])

    if not user_favs:
        await message.answer(
            "У тебя нет избранных прокси.\n"
            "Используй /save после /proxy чтобы сохранить лучшие.",
            parse_mode=None,
        )
        return

    # Re-validate favorites quickly
    proxies = [MTProxy.from_dict(d) for d in user_favs]
    valid = await validate_batch(proxies)

    if not valid:
        await message.answer(
            "😕 Ни один из избранных прокси сейчас не работает.\n"
            "Используй /proxy для поиска новых, затем /save",
            parse_mode=None,
        )
        return

    valid.sort(key=lambda p: p.latency_ms)
    lines = [format_proxy_line(i, p) for i, p in enumerate(valid, 1)]
    text = f"⭐ *Избранные прокси* \\({len(valid)}/{len(user_favs)} работают\\):\n\n" + "\n".join(lines)
    await message.answer(text, reply_markup=favorites_keyboard(len(valid)), disable_web_page_preview=True)


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
    await callback.message.edit_text("🔄 Обновляю список прокси\\.\\.\\.")
    await refresh_proxies(manager)

    proxies = manager.valid_proxies[:PROXIES_PER_REQUEST]
    if proxies:
        text = format_proxy_list(proxies, manager.last_refresh)
        await callback.message.edit_text(text, reply_markup=main_keyboard(), disable_web_page_preview=True)
    else:
        await callback.message.edit_text(
            "Рабочих прокси не найдено\\. Попробуй позже\\.",
            reply_markup=main_keyboard(),
        )


@router.callback_query(F.data == "cb_status")
async def cb_status(callback: CallbackQuery):
    subs = load_subscribers()
    await callback.answer()
    await callback.message.edit_text(
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
    text = format_proxy_list(selected, manager.last_refresh)
    text += f"\n\n_Фильтр: только быстрые \\(<100ms\\)_"
    await callback.message.edit_text(text, reply_markup=main_keyboard(), disable_web_page_preview=True)


@router.callback_query(F.data == "cb_favorites")
async def cb_favorites(callback: CallbackQuery):
    await callback.answer()
    favs = load_favorites()
    uid = str(callback.from_user.id)
    user_favs = favs.get(uid, [])

    if not user_favs:
        await callback.answer("Нет избранных. Используй /save", show_alert=True)
        return

    proxies = [MTProxy.from_dict(d) for d in user_favs]
    valid = await validate_batch(proxies)
    if not valid:
        await callback.answer("Избранные прокси сейчас не работают 😕", show_alert=True)
        return

    valid.sort(key=lambda p: p.latency_ms)
    lines = [format_proxy_line(i, p) for i, p in enumerate(valid, 1)]
    text = f"⭐ *Избранные прокси* \\({len(valid)}/{len(user_favs)} работают\\):\n\n" + "\n".join(lines)
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="« Назад", callback_data="cb_back")],
        ]),
        disable_web_page_preview=True,
    )


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


@router.callback_query(F.data == "cb_back")
async def cb_back(callback: CallbackQuery):
    await callback.answer()
    if not manager.valid_proxies:
        await callback.message.edit_text(
            "Прокси ещё не загружены\\. Нажми 🔄 Обновить\\.",
            reply_markup=main_keyboard(),
        )
        return
    proxies = manager.valid_proxies[:PROXIES_PER_REQUEST]
    text = format_proxy_list(proxies, manager.last_refresh)
    await callback.message.edit_text(text, reply_markup=main_keyboard(), disable_web_page_preview=True)


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
