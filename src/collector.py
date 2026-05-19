#!/usr/bin/env python3
"""
collector.py — сборщик VLESS-серверов из нескольких источников.

Источники:
  1. Публичные GitHub-репозитории (список в GITHUB_REPOS)
  2. Публичные Telegram-каналы (список в TELEGRAM_CHANNELS)
  3. Ручной список IP/подсетей (MANUAL_HOSTS)

После сбора каждый сервер проверяется TCP-соединением.
Прошедшие проверку пишутся в output/subscription.txt (plain) и
output/subscription_b64.txt (base64 для клиентов).
"""

import asyncio
import base64
import json
import logging
import os
import re
import socket
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

# ─────────────────────────────────────────────────────────────
# КОНФИГУРАЦИЯ  ← редактируй этот блок
# ─────────────────────────────────────────────────────────────

# GitHub: список файлов с VLESS-конфигурациями
GITHUB_REPOS: list[str] = [
    "https://raw.githubusercontent.com/zieng2/wl/main/vless_lite.txt",
    "https://raw.githubusercontent.com/zieng2/wl/main/vless_universal.txt",
    # Добавляй свои источники:
    # "https://raw.githubusercontent.com/АВТОР/РЕПО/main/vless.txt",
]

# Telegram: публичные каналы (только открытые, без токена)
# Скрипт парсит HTML публичной страницы t.me/s/<channel>
TELEGRAM_CHANNELS: list[str] = [
    "zieng2",          # официальный канал автора
    # "vpnxxx",        # добавляй свои
]

# Ручной список: IP:PORT или просто IP (будет использован порт 443)
# Для них генерируется базовый VLESS-шаблон с REALITY
MANUAL_HOSTS: list[str] = [
    # "1.2.3.4:443",
    # "5.6.7.8",
]

# UUID по умолчанию для ручных хостов (замени на свой)
MANUAL_UUID = "00000000-0000-0000-0000-000000000000"

# Параметры проверки
TCP_TIMEOUT   = 5      # секунд на одно TCP-соединение
MAX_WORKERS   = 100    # параллельных проверок
RETRY_COUNT   = 1      # повторных попыток при неудаче

# Пути вывода
OUTPUT_DIR    = Path(__file__).parent.parent / "output"
OUT_PLAIN     = OUTPUT_DIR / "subscription.txt"
OUT_B64       = OUTPUT_DIR / "subscription_b64.txt"
OUT_STATS     = OUTPUT_DIR / "stats.json"

# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════
# Структура данных
# ══════════════════════════════════════════════

@dataclass
class Server:
    raw: str                   # полная vless:// строка
    host: str = ""
    port: int = 443
    label: str = ""
    country: str = ""
    source: str = ""
    alive: bool = False
    latency_ms: int = -1

    def __post_init__(self):
        self._parse()

    def _parse(self):
        """Извлекает host:port и метку из raw-строки."""
        try:
            # vless://UUID@HOST:PORT?...#LABEL
            body = self.raw[len("vless://"):]
            at_idx = body.rfind("@")
            if at_idx == -1:
                return
            rest = body[at_idx + 1:]
            # HOST:PORT или [IPv6]:PORT
            if rest.startswith("["):
                bracket_end = rest.index("]")
                self.host = rest[1:bracket_end]
                rest = rest[bracket_end + 1:]
                if rest.startswith(":"):
                    port_part = rest[1:].split("?")[0].split("#")[0]
                    self.port = int(port_part)
            else:
                hp = rest.split("?")[0].split("#")[0]
                if ":" in hp:
                    h, p = hp.rsplit(":", 1)
                    self.host = h
                    self.port = int(p)
                else:
                    self.host = hp

            if "#" in self.raw:
                self.label = unquote(self.raw.split("#", 1)[1])
                self.country = _extract_country(self.label)
        except Exception:
            pass

    @property
    def uid(self) -> str:
        """Уникальный ключ для дедупликации."""
        return f"{self.host}:{self.port}"


# ══════════════════════════════════════════════
# Парсинг меток
# ══════════════════════════════════════════════

def _extract_country(label: str) -> str:
    clean = re.sub(r"[\U0001F1E6-\U0001F1FF]{2}", "", label).strip()
    for sep in [" \u2014 ", " - ", "#"]:
        if sep in clean:
            clean = clean.split(sep)[0].strip()
            break
    return clean


def _extract_vless_lines(text: str) -> list[str]:
    """Находит все строки vless:// в тексте."""
    lines = []
    for line in text.splitlines():
        line = line.strip()
        # Иногда серверы встречаются внутри HTML/JSON — ищем подстроку
        matches = re.findall(r'vless://[^\s\'"<>]+', line)
        lines.extend(matches)
    return lines


# ══════════════════════════════════════════════
# Источник 1: GitHub
# ══════════════════════════════════════════════

def fetch_github(url: str) -> list[Server]:
    log.info(f"GitHub ← {url}")
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "wl-collector/2.0"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            text = r.read().decode("utf-8")
        raws = _extract_vless_lines(text)
        servers = [Server(raw=r, source="github") for r in raws]
        log.info(f"  получено: {len(servers)}")
        return servers
    except Exception as e:
        log.warning(f"  ошибка: {e}")
        return []


# ══════════════════════════════════════════════
# Источник 2: Telegram (публичный HTML)
# ══════════════════════════════════════════════

def fetch_telegram(channel: str) -> list[Server]:
    """Парсит последние посты публичного Telegram-канала через t.me/s/."""
    url = f"https://t.me/s/{channel}"
    log.info(f"Telegram ← @{channel}")
    try:
        req = urllib.request.Request(
            url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "ru-RU,ru;q=0.9",
            }
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8")

        raws = _extract_vless_lines(html)

        # Также ищем base64-блоки (некоторые каналы постят подписки в b64)
        b64_blocks = re.findall(r'[A-Za-z0-9+/=]{100,}', html)
        for block in b64_blocks:
            try:
                decoded = base64.b64decode(block + "==").decode("utf-8", errors="ignore")
                raws.extend(_extract_vless_lines(decoded))
            except Exception:
                pass

        servers = [Server(raw=r, source=f"tg:{channel}") for r in raws]
        log.info(f"  получено: {len(servers)}")
        return servers
    except Exception as e:
        log.warning(f"  ошибка: {e}")
        return []


# ══════════════════════════════════════════════
# Источник 3: Ручные хосты → генерация VLESS
# ══════════════════════════════════════════════

# Шаблоны для разных типов Reality SNI (белые списки)
REALITY_SNI_POOL = [
    "ads.x5.ru", "api-maps.yandex.ru", "eh.vk.com",
    "event.yandex.ru", "megafon.ru", "kinopoisk.ru",
]

def _make_vless(host: str, port: int, idx: int) -> str:
    sni = REALITY_SNI_POOL[idx % len(REALITY_SNI_POOL)]
    label = f"Manual%20%E2%80%94%20%23{idx + 1}"
    return (
        f"vless://{MANUAL_UUID}@{host}:{port}"
        f"?flow=xtls-rprx-vision&encryption=none&type=tcp"
        f"&security=reality&fp=chrome&sni={sni}"
        f"#{label}"
    )


def fetch_manual() -> list[Server]:
    servers = []
    for i, entry in enumerate(MANUAL_HOSTS):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry and not entry.startswith("["):
            host, port_s = entry.rsplit(":", 1)
            port = int(port_s)
        else:
            host, port = entry, 443
        raw = _make_vless(host, port, i)
        servers.append(Server(raw=raw, source="manual"))
    if servers:
        log.info(f"Manual ← {len(servers)} хостов")
    return servers


# ══════════════════════════════════════════════
# Проверка TCP
# ══════════════════════════════════════════════

async def _tcp_check(server: Server, sem: asyncio.Semaphore) -> Server:
    """Проверяет TCP-соединение к host:port."""
    async with sem:
        for attempt in range(RETRY_COUNT + 1):
            try:
                t0 = asyncio.get_event_loop().time()
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(server.host, server.port),
                    timeout=TCP_TIMEOUT,
                )
                latency = int((asyncio.get_event_loop().time() - t0) * 1000)
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                server.alive    = True
                server.latency_ms = latency
                return server
            except Exception:
                if attempt < RETRY_COUNT:
                    await asyncio.sleep(0.3)
        server.alive = False
        return server


async def check_all(servers: list[Server]) -> list[Server]:
    sem = asyncio.Semaphore(MAX_WORKERS)
    tasks = [_tcp_check(s, sem) for s in servers]
    total = len(tasks)
    log.info(f"Проверяю {total} серверов (до {MAX_WORKERS} параллельно)…")

    results = []
    done = 0
    for coro in asyncio.as_completed(tasks):
        s = await coro
        results.append(s)
        done += 1
        if done % 50 == 0 or done == total:
            alive = sum(1 for r in results if r.alive)
            log.info(f"  {done}/{total} проверено, живых: {alive}")

    return results


# ══════════════════════════════════════════════
# Дедупликация
# ══════════════════════════════════════════════

def deduplicate(servers: list[Server]) -> list[Server]:
    seen: set[str] = set()
    unique = []
    for s in servers:
        key = s.uid
        if key not in seen:
            seen.add(key)
            unique.append(s)
    removed = len(servers) - len(unique)
    if removed:
        log.info(f"Дедупликация: удалено {removed} дублей")
    return unique


# ══════════════════════════════════════════════
# Сохранение
# ══════════════════════════════════════════════

def save(servers: list[Server], all_count: int, checked_count: int) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Сортируем: сначала живые по задержке, потом мёртвые
    alive   = sorted([s for s in servers if s.alive],  key=lambda x: x.latency_ms)
    dead    = [s for s in servers if not s.alive]

    lines_plain = [
        f"# Подписка — собственный источник",
        f"# Обновлено: {ts}",
        f"# Всего найдено: {all_count} | уникальных: {checked_count} | живых: {len(alive)}",
        "",
    ] + [s.raw for s in alive]

    # plain
    OUT_PLAIN.write_text("\n".join(lines_plain), encoding="utf-8")
    log.info(f"Сохранено (plain):  {OUT_PLAIN}  [{len(alive)} серверов]")

    # base64 (только vless-строки, без заголовков)
    b64_content = base64.b64encode(
        "\n".join(s.raw for s in alive).encode("utf-8")
    ).decode("ascii")
    OUT_B64.write_text(b64_content, encoding="utf-8")
    log.info(f"Сохранено (base64): {OUT_B64}")

    # статистика JSON
    stats = {
        "updated_at": ts,
        "total_found": all_count,
        "unique": checked_count,
        "alive": len(alive),
        "dead": len(dead),
        "sources": {},
        "countries": {},
    }
    for s in alive:
        stats["sources"][s.source] = stats["sources"].get(s.source, 0) + 1
        c = s.country or "Unknown"
        stats["countries"][c] = stats["countries"].get(c, 0) + 1

    OUT_STATS.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"Статистика:         {OUT_STATS}")

    # Краткий итог в консоль
    print("\n" + "═" * 50)
    print(f"  Найдено всего : {all_count}")
    print(f"  Уникальных    : {checked_count}")
    print(f"  Живых (TCP OK): {len(alive)}")
    print(f"  Мёртвых       : {len(dead)}")
    if alive:
        print(f"  Лучший пинг   : {alive[0].latency_ms} мс  ({alive[0].host}:{alive[0].port})")
    print("═" * 50)


# ══════════════════════════════════════════════
# Точка входа
# ══════════════════════════════════════════════

async def main() -> None:
    all_servers: list[Server] = []

    # 1. GitHub
    for url in GITHUB_REPOS:
        all_servers.extend(fetch_github(url))

    # 2. Telegram
    for ch in TELEGRAM_CHANNELS:
        all_servers.extend(fetch_telegram(ch))

    # 3. Ручные хосты
    all_servers.extend(fetch_manual())

    total_found = len(all_servers)
    log.info(f"Всего собрано: {total_found}")

    if not all_servers:
        log.error("Серверов не найдено. Проверь источники.")
        sys.exit(1)

    # Дедупликация
    all_servers = deduplicate(all_servers)
    unique_count = len(all_servers)

    # TCP-проверка
    all_servers = await check_all(all_servers)

    # Сохранение
    save(all_servers, total_found, unique_count)


if __name__ == "__main__":
    asyncio.run(main())
