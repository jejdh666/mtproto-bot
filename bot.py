from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Set, Tuple
from urllib.parse import urlparse, parse_qs

import aiohttp
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROXY_SOURCES = [
    "https://raw.githubusercontent.com/SoliSpirit/mtproto/master/all_proxies.txt",
    "https://raw.githubusercontent.com/ALIILAPRO/MTProtoProxy/main/mtproto.txt",
]

REFRESH_INTERVAL = 20 * 60  # 20 minutes
STALE_THRESHOLD = 30 * 60   # 30 minutes
COOLDOWN = 5 * 60           # 5 minutes per user for /refresh
VALIDATE_TIMEOUT = 5.0
VALIDATE_CONCURRENCY = 50
PROXIES_PER_REQUEST = 5


@dataclass
class MTProxy:
    server: str
    port: int
    secret: str
    is_valid: bool = False
    latency_ms: int = 0

    @property
    def tme_link(self) -> str:
        return f"https://t.me/proxy?server={self.server}&port={self.port}&secret={self.secret}"


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
        return MTProxy(
            server=server.rstrip("."),
            port=port_int,
            secret=secret,
        )
    except (ValueError, IndexError, TypeError):
        return None


async def fetch_proxies_from_source(session: aiohttp.ClientSession, url: str) -> list[MTProxy]:
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


async def validate_proxy(proxy: MTProxy, timeout: float = VALIDATE_TIMEOUT) -> bool:
    try:
        start = time.monotonic()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(proxy.server, proxy.port),
            timeout=timeout,
        )
        proxy.latency_ms = int((time.monotonic() - start) * 1000)
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
        manager.valid_proxies = valid
        manager.last_refresh = datetime.now(timezone.utc)
        logger.info(f"Validation complete: {len(valid)} working proxies")


# --- Bot setup ---

router = Router()
manager = ProxyManager()
refresh_cooldowns: dict[int, datetime] = {}


@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "MTProto Proxy Bot\n\n"
        "Нахожу и проверяю рабочие MTProto\\-прокси для Telegram\\.\n\n"
        "Команды:\n"
        "/proxy \\— получить рабочие прокси\n"
        "/refresh \\— обновить список\n"
        "/status \\— статистика",
        parse_mode=ParseMode.MARKDOWN_V2,
    )


@router.message(Command("proxy", "get"))
async def cmd_proxy(message: Message):
    if manager.is_stale() or not manager.valid_proxies:
        wait_msg = await message.answer("Загружаю свежие прокси, подожди...")
        await refresh_proxies(manager)
        try:
            await wait_msg.delete()
        except Exception:
            pass

    if not manager.valid_proxies:
        await message.answer("Рабочих прокси не найдено. Попробуй позже через /refresh", parse_mode=None)
        return

    count = min(PROXIES_PER_REQUEST, len(manager.valid_proxies))
    fastest = manager.valid_proxies[:count]

    lines = []
    for i, p in enumerate(fastest, 1):
        display = f"{p.server}:{p.port}".replace("-", "\\-").replace(".", "\\.")
        lines.append(f"{i}\\. [{display}]({p.tme_link}) \\— {p.latency_ms}ms")

    updated = manager.last_refresh.strftime("%H:%M UTC") if manager.last_refresh else "?"
    text = "Рабочие MTProto прокси:\n\n" + "\n".join(lines)
    text += f"\n\n_Обновлено: {updated}_"
    await message.answer(text, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)


@router.message(Command("refresh"))
async def cmd_refresh(message: Message):
    user_id = message.from_user.id
    now = datetime.now(timezone.utc)

    last = refresh_cooldowns.get(user_id)
    if last and (now - last).total_seconds() < COOLDOWN:
        remaining = int(COOLDOWN - (now - last).total_seconds())
        await message.answer(f"Подожди {remaining} сек. перед повторным обновлением.", parse_mode=None)
        return

    refresh_cooldowns[user_id] = now
    wait_msg = await message.answer("Обновляю список прокси...")
    await refresh_proxies(manager)
    try:
        await wait_msg.delete()
    except Exception:
        pass
    await message.answer(f"Готово\\! Найдено {len(manager.valid_proxies)} рабочих прокси\\. Жми /proxy")


@router.message(Command("status"))
async def cmd_status(message: Message):
    if manager.last_refresh:
        updated = manager.last_refresh.strftime("%Y-%m-%d %H:%M UTC")
    else:
        updated = "ещё не обновлялось"

    await message.answer(
        f"Рабочих прокси: {len(manager.valid_proxies)}\n"
        f"Последнее обновление: {updated}\n"
        f"Источников: {len(PROXY_SOURCES)}",
        parse_mode=None,
    )


async def periodic_refresh():
    while True:
        await asyncio.sleep(REFRESH_INTERVAL)
        try:
            await refresh_proxies(manager)
        except Exception as e:
            logger.error(f"Background refresh failed: {e}")


async def on_startup(bot: Bot):
    logger.info("Bot starting, loading initial proxies...")
    await refresh_proxies(manager)
    asyncio.create_task(periodic_refresh())
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
