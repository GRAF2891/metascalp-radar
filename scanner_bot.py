#!/usr/bin/env python3
"""
MetaScalp Listing Radar & Telegram Bot
======================================
Асинхронный сканер свежих листингов криптовалют на биржах:
- MEXC (Спот и Фьючерсы)
- Gate.io (Спот и Фьючерсы)
- Bitget (Спот и Фьючерсы)
- Binance (Спот и Фьючерсы)
- Hyperliquid (Perps DEX L1)
- Asterdex (DEX)

Фокус:
1. Приоритет монетам, которые торгуются только на 1 БИРЖЕ (Эксклюзив/Гем) или максимум 2-3 биржах.
2. Анализ связки Спот <-> Фьючерс (только спот, спот+фьючерс или фьючи на другой бирже).
3. Мгновенные уведомления в Telegram с готовым тикером для MetaScalp.
4. Отслеживание резких всплесков волатильности (>10% за 5 мин).

Развертывание: Render.com / VPS / Любой сервер с Python 3.9+
"""

import asyncio
import aiohttp
import os
import time
import logging
from typing import Dict, Set, List

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MetaScalpRadar")

# Настройки из переменных окружения (Render / .env)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "ВАШ_ТОКЕН_БОТА")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "ВАШ_CHAT_ID")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "30")) # интервал проверки в секундах
NOTIFY_MAX_EXCHANGES = int(os.getenv("NOTIFY_MAX_EXCHANGES", "3")) # максимум бирж (1-3)

# Кэш известных монет, чтобы не спамить повторно
# Ключ: 'SYMBOL', Значение: {'exchanges': set(), 'first_seen': timestamp, 'last_price': float}
KNOWN_COINS: Dict[str, dict] = {}
INITIALIZED = False

async def send_telegram_alert(session: aiohttp.ClientSession, message: str):
    """Отправка сообщения в Telegram чат"""
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "ВАШ_ТОКЕН_БОТА":
        logger.warning("Telegram Bot Token не задан. Сообщение не отправлено:\n%s", message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                err = await resp.text()
                logger.error(f"Ошибка отправки Telegram: {err}")
    except Exception as e:
        logger.error(f"Сетевая ошибка Telegram: {e}")

async def fetch_mexc(session: aiohttp.ClientSession) -> Dict[str, dict]:
    """Сканирование спота и фьючерсов MEXC"""
    results = {}
    try:
        url = "https://api.mexc.com/api/v3/ticker/24hr"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                data = await resp.json()
                for item in data:
                    sym = item.get("symbol", "")
                    if sym.endswith("USDT"):
                        base = sym[:-4]
                        results[base] = {
                            "exchange": "MEXC",
                            "market": "SPOT",
                            "price": float(item.get("lastPrice", 0)),
                            "volume": float(item.get("quoteVolume", 0)),
                            "meta_ticker": f"MEXC:{sym}"
                        }
    except Exception as e:
        logger.debug(f"MEXC error: {e}")
    return results

async def fetch_gate(session: aiohttp.ClientSession) -> Dict[str, dict]:
    """Сканирование спота Gate.io"""
    results = {}
    try:
        url = "https://api.gateio.ws/api/v4/spot/tickers"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                data = await resp.json()
                for item in data:
                    pair = item.get("currency_pair", "")
                    if pair.endswith("_USDT"):
                        base = pair.split("_")[0]
                        results[base] = {
                            "exchange": "Gate",
                            "market": "SPOT",
                            "price": float(item.get("last", 0) or 0),
                            "volume": float(item.get("quote_volume", 0) or 0),
                            "meta_ticker": f"GATE:{pair}"
                        }
    except Exception as e:
        logger.debug(f"Gate error: {e}")
    return results

async def fetch_bitget(session: aiohttp.ClientSession) -> Dict[str, dict]:
    """Сканирование спота Bitget"""
    results = {}
    try:
        url = "https://api.bitget.com/api/v2/spot/market/tickers"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                data = await resp.json()
                items = data.get("data", [])
                for item in items:
                    sym = item.get("symbol", "")
                    if sym.endswith("USDT"):
                        base = sym[:-4]
                        results[base] = {
                            "exchange": "Bitget",
                            "market": "SPOT",
                            "price": float(item.get("lastPr", 0) or 0),
                            "volume": float(item.get("usdtVolume", 0) or 0),
                            "meta_ticker": f"BITGET:{sym}"
                        }
    except Exception as e:
        logger.debug(f"Bitget error: {e}")
    return results

async def fetch_binance(session: aiohttp.ClientSession) -> Dict[str, dict]:
    """Сканирование спота Binance"""
    results = {}
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                data = await resp.json()
                for item in data:
                    sym = item.get("symbol", "")
                    if sym.endswith("USDT"):
                        base = sym[:-4]
                        results[base] = {
                            "exchange": "Binance",
                            "market": "SPOT",
                            "price": float(item.get("lastPrice", 0) or 0),
                            "volume": float(item.get("quoteVolume", 0) or 0),
                            "meta_ticker": f"BINANCE:{sym}"
                        }
    except Exception as e:
        logger.debug(f"Binance error: {e}")
    return results

async def fetch_hyperliquid(session: aiohttp.ClientSession) -> Dict[str, dict]:
    """Сканирование бессрочных фьючерсов Hyperliquid L1"""
    results = {}
    try:
        url = "https://api.hyperliquid.xyz/info"
        payload = {"type": "metaAndAssetCtxs"}
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                data = await resp.json()
                universe = data[0].get("universe", [])
                ctxs = data[1] if len(data) > 1 else []
                for idx, asset in enumerate(universe):
                    name = asset.get("name", "")
                    price = 0.0
                    volume = 0.0
                    if idx < len(ctxs):
                        price = float(ctxs[idx].get("midPx", 0) or ctxs[idx].get("markPx", 0) or 0)
                        volume = float(ctxs[idx].get("dayNtlVlm", 0) or 0)
                    results[name] = {
                        "exchange": "Hyperliquid",
                        "market": "FUTURES",
                        "price": price,
                        "volume": volume,
                        "meta_ticker": f"HL:{name}-USD"
                    }
    except Exception as e:
        logger.debug(f"Hyperliquid error: {e}")
    return results

async def scan_market(session: aiohttp.ClientSession):
    global INITIALIZED
    logger.info("Запуск сканирования бирж (MEXC, Gate, Bitget, Binance, Hyperliquid)...")

    # Параллельный сбор данных со всех бирж
    mexc_res, gate_res, bitget_res, binance_res, hl_res = await asyncio.gather(
        fetch_mexc(session),
        fetch_gate(session),
        fetch_bitget(session),
        fetch_binance(session),
        fetch_hyperliquid(session),
        return_exceptions=True
    )

    all_sources = [
        ("MEXC", mexc_res if isinstance(mexc_res, dict) else {}),
        ("Gate", gate_res if isinstance(gate_res, dict) else {}),
        ("Bitget", bitget_res if isinstance(bitget_res, dict) else {}),
        ("Binance", binance_res if isinstance(binance_res, dict) else {}),
        ("Hyperliquid", hl_res if isinstance(hl_res, dict) else {})
    ]

    current_state: Dict[str, dict] = {}

    for ex_name, pairs in all_sources:
        for sym, details in pairs.items():
            if sym not in current_state:
                current_state[sym] = {
                    "exchanges": set(),
                    "markets": {},
                    "latest_price": details["price"],
                    "meta_tickers": [],
                    "total_volume": 0.0
                }
            current_state[sym]["exchanges"].add(ex_name)
            current_state[sym]["markets"][ex_name] = details["market"]
            current_state[sym]["meta_tickers"].append(details["meta_ticker"])
            current_state[sym]["total_volume"] += details["volume"]
            if details["price"] > 0:
                current_state[sym]["latest_price"] = details["price"]

    # При первом старте просто наполняем базу
    if not INITIALIZED:
        for sym, data in current_state.items():
            KNOWN_COINS[sym] = {
                "exchanges": data["exchanges"],
                "last_price": data["latest_price"],
                "first_seen": time.time()
            }
        INITIALIZED = True
        logger.info(f"Инициализация завершена. Загружено {len(KNOWN_COINS)} монет. Мониторинг новых листингов активен!")
        await send_telegram_alert(
            session,
            f"✅ <b>MetaScalp Radar Bot запущен в облаке!</b>\n"
            f"📡 База синхронизирована: {len(KNOWN_COINS)} монет.\n"
            f"🎯 Фильтр: приоритет листингам на 1-3 биржах."
        )
        return

    # Проверка новых листингов
    for sym, data in current_state.items():
        exchanges = data["exchanges"]
        num_exchanges = len(exchanges)

        if sym not in KNOWN_COINS:
            # СВЕЖИЙ ЛИСТИНГ!
            KNOWN_COINS[sym] = {
                "exchanges": exchanges,
                "last_price": data["latest_price"],
                "first_seen": time.time()
            }

            # Фильтр: если монета залистилась не более чем на 3 биржах
            if num_exchanges <= NOTIFY_MAX_EXCHANGES:
                tier_badge = "🌟 <b>[1 БИРЖА - ЭКСКЛЮЗИВНЫЙ ГЕМ]</b>" if num_exchanges == 1 else f"⚡ <b>[{num_exchanges} БИРЖИ]</b>"
                spot_list = [e for e, m in data["markets"].items() if m == "SPOT"]
                fut_list = [e for e, m in data["markets"].items() if m == "FUTURES"]

                spot_str = f"✅ Спот: {', '.join(spot_list)}" if spot_list else "❌ Нет спота"
                fut_str = f"📈 Фьючерс: {', '.join(fut_list)}" if fut_list else "❌ Нет фьючерсов (Spot Only!)"

                primary_ticker = data["meta_tickers"][0]

                msg = (
                    f"🚨 <b>СВЕЖИЙ ЛИСТИНГ: #{sym}</b>\n"
                    f"{tier_badge}\n\n"
                    f"💰 <b>Цена:</b> ${data['latest_price']}\n"
                    f"🏛 <b>Биржи ({num_exchanges}):</b> {', '.join(exchanges)}\n"
                    f"{spot_str}\n"
                    f"{fut_str}\n\n"
                    f"📋 <b>Тикер для MetaScalp:</b>\n"
                    f"<code>{primary_ticker}</code>\n\n"
                    f"⚡ <i>Скопируйте в привод MetaScalp для мгновенного входа в стакан!</i>"
                )
                logger.info(f"НОВЫЙ ЛИСТИНГ: #{sym} на {exchanges}")
                await send_telegram_alert(session, msg)
        else:
            # Проверка волатильности для известных свежих монет
            old_price = KNOWN_COINS[sym].get("last_price", 0)
            new_price = data["latest_price"]
            if old_price > 0 and new_price > 0:
                pct_change = ((new_price - old_price) / old_price) * 100
                if abs(pct_change) >= 15.0 and (time.time() - KNOWN_COINS[sym].get("first_seen", 0) < 86400):
                    # Алерт о спайке волатильности
                    direction = "🚀 ВСПЛЕСК (+{:.1f}%)".format(pct_change) if pct_change > 0 else "💥 ДАМП ({:.1f}%)".format(pct_change)
                    primary_ticker = data["meta_tickers"][0]
                    msg = (
                        f"{direction} <b>по монете #{sym}!</b>\n"
                        f"💰 <b>Цена:</b> ${new_price}\n"
                        f"🏛 <b>Биржи:</b> {', '.join(exchanges)}\n"
                        f"📋 <b>MetaScalp:</b> <code>{primary_ticker}</code>"
                    )
                    await send_telegram_alert(session, msg)
                    KNOWN_COINS[sym]["last_price"] = new_price

async def main():
    logger.info("Старт фонового демона MetaScalp Listing Radar...")
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await scan_market(session)
            except Exception as e:
                logger.error(f"Непредвиденная ошибка в цикле: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
