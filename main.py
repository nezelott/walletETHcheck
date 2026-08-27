"""
Wallet Activity Tracker — Telegram Bot
========================================

Следит за одним (или несколькими) кошельками в сети Ethereum и присылает
уведомление в Telegram на каждую активность:

  • переводы ETH (входящие и исходящие)
  • переводы любых ERC-20 токенов
  • переводы NFT (ERC-721)
  • взаимодействия с контрактами (с названием вызванной функции)
  • неудачные транзакции — тоже показываются, помечены отдельно

ПОЧЕМУ ETHERSCAN, А НЕ ПРЯМОЕ ЧТЕНИЕ БЛОКЧЕЙНА
-----------------------------------------------
Переводы токенов видны в блокчейне как события (логи) и их можно поймать
через eth_getLogs. А вот переводы самого ETH событий НЕ создают — чтобы
поймать их через RPC, пришлось бы перебирать все транзакции в каждом
блоке, это тысячи запросов. Etherscan уже проиндексировал всё это и
отдаёт готовым: и обычные транзакции, и внутренние, и токены, и даже
расшифрованное имя вызванной функции контракта. Бесплатный лимит —
100 000 запросов в день, бот расходует около 9 000.

ОДНА ТРАНЗАКЦИЯ = ОДНО СООБЩЕНИЕ
---------------------------------
Один обмен на бирже порождает сразу несколько записей: обычную
транзакцию, внутренние переводы ETH и несколько переводов токенов. Бот
собирает всё это по хешу транзакции в одно понятное сообщение, а не
присылает пять отдельных уведомлений об одном и том же действии.
"""

import asyncio
import logging
import os
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

import aiohttp
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# ============================================================================
# КОНФИГУРАЦИЯ (переменные окружения в Render → Environment)
# ============================================================================
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
WALLETS_RAW = os.getenv("WALLET_ADDRESS", "0x7860c40ea085a2b4c275900def5f894570b452d1").strip()
CHAIN_ID = int(os.getenv("CHAIN_ID", "1"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "30"))
TRACK_NFT = os.getenv("TRACK_NFT", "1").strip() not in ("0", "false", "no")
PORT = int(os.getenv("PORT", "10000"))

ETHERSCAN_BASE = "https://api.etherscan.io/v2/api"
# Etherscan разрешает 5 запросов в секунду — держим паузу с запасом
API_PAUSE_SEC = float(os.getenv("API_PAUSE_SEC", "0.3"))
# Сколько хешей помним, чтобы не прислать одно и то же дважды
SEEN_LIMIT = 800

_EXPLORERS = {
    1: "https://etherscan.io",
    42161: "https://arbiscan.io",
    10: "https://optimistic.etherscan.io",
    8453: "https://basescan.org",
    56: "https://bscscan.com",
    137: "https://polygonscan.com",
}
EXPLORER = _EXPLORERS.get(CHAIN_ID, "https://etherscan.io")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("wallet-bot")

WALLETS = [w.strip().lower() for w in re.split(r"[,\s]+", WALLETS_RAW) if w.strip()]


# ============================================================================
# HTTP-сервер — только чтобы Render считал сервис живым
# ============================================================================
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write("Wallet tracker bot is running.".encode("utf-8"))

    def log_message(self, format, *args):
        pass


def _run_health_server():
    HTTPServer(("0.0.0.0", PORT), _HealthHandler).serve_forever()


# ============================================================================
# СОСТОЯНИЕ
# ============================================================================
@dataclass
class WalletState:
    address: str
    last_block: int = 0
    seen: OrderedDict = field(default_factory=OrderedDict)

    def remember(self, tx_hash: str):
        self.seen[tx_hash] = True
        while len(self.seen) > SEEN_LIMIT:
            self.seen.popitem(last=False)

    def already_seen(self, tx_hash: str) -> bool:
        return tx_hash in self.seen


@dataclass
class BotState:
    alerts_enabled: bool = True
    started_at: float = 0.0
    alerts_sent: int = 0
    api_calls: int = 0
    last_error_notified_at: float = 0.0
    wallets: dict = field(default_factory=dict)


state = BotState()
for w in WALLETS:
    state.wallets[w] = WalletState(address=w)

http_session: Optional[aiohttp.ClientSession] = None


# ============================================================================
# ОБРАЩЕНИЕ К ETHERSCAN
# ============================================================================
def _sanitize(text) -> str:
    """Убирает ключ API и ссылки из текста ошибки — они уходят в Telegram."""
    s = re.sub(r"apikey=[^&\s]+", "apikey=<скрыт>", str(text))
    if ETHERSCAN_API_KEY:
        s = s.replace(ETHERSCAN_API_KEY, "<скрыт>")
    return s


class EtherscanError(RuntimeError):
    pass


async def _etherscan(params: dict, attempts: int = 3):
    """Запрос к Etherscan. Возвращает список записей (может быть пустым).

    Особенность API: status="0" означает и «ошибка», и «просто ничего не
    найдено» — второе для нас нормальная ситуация, не ошибка."""
    query = {
        "chainid": CHAIN_ID,
        "apikey": ETHERSCAN_API_KEY,
        **params,
    }
    last_err = None
    for attempt in range(attempts):
        try:
            async with http_session.get(
                ETHERSCAN_BASE, params=query, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                state.api_calls += 1
                text = await resp.text()
                if resp.status != 200:
                    raise EtherscanError(f"HTTP {resp.status}: {text[:150]}")
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    raise EtherscanError(f"нечитаемый ответ: {text[:150]}")

                status = str(data.get("status", ""))
                message = str(data.get("message", ""))
                result = data.get("result")

                if status == "1":
                    return result if isinstance(result, list) else []

                # "No transactions found" — не ошибка, просто пусто
                if "no transactions found" in message.lower() or \
                   "no records found" in message.lower():
                    return []

                # Превышен лимит частоты — подождать и повторить
                if "rate limit" in str(result).lower() or "rate limit" in message.lower():
                    last_err = EtherscanError(f"лимит частоты запросов: {result}")
                    if attempt < attempts - 1:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                    raise last_err

                raise EtherscanError(f"{message}: {str(result)[:150]}")

        except EtherscanError:
            raise
        except Exception as e:
            last_err = EtherscanError(_sanitize(e))
            if attempt < attempts - 1:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            raise last_err from e

    raise last_err if last_err else EtherscanError("неизвестная ошибка")


async def _get_current_block() -> int:
    """Отдельно, т.к. модуль proxy отвечает в формате JSON-RPC, а не
    как остальные эндпоинты Etherscan."""
    query = {
        "chainid": CHAIN_ID,
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": ETHERSCAN_API_KEY,
    }
    async with http_session.get(
        ETHERSCAN_BASE, params=query, timeout=aiohttp.ClientTimeout(total=20)
    ) as resp:
        state.api_calls += 1
        data = await resp.json(content_type=None)
        raw = data.get("result")
        if isinstance(raw, str) and raw.startswith("0x"):
            return int(raw, 16)
        raise EtherscanError(f"не смог получить номер блока: {str(data)[:150]}")


# ============================================================================
# СБОР АКТИВНОСТИ ПО КОШЕЛЬКУ
# ============================================================================
async def _fetch_activity(wallet: str, start_block: int) -> dict:
    """Собирает все виды активности начиная с блока start_block и
    группирует их по хешу транзакции."""
    base = {
        "module": "account",
        "address": wallet,
        "startblock": start_block,
        "endblock": 99999999,
        "page": 1,
        "offset": 100,
        "sort": "asc",
    }

    sources = [("normal", "txlist"), ("internal", "txlistinternal"), ("token", "tokentx")]
    if TRACK_NFT:
        sources.append(("nft", "tokennfttx"))

    grouped = {}
    for kind, action in sources:
        rows = await _etherscan({**base, "action": action})
        for row in rows:
            tx_hash = row.get("hash")
            if not tx_hash:
                continue
            entry = grouped.setdefault(tx_hash, {
                "hash": tx_hash,
                "block": int(row.get("blockNumber", 0) or 0),
                "time": int(row.get("timeStamp", 0) or 0),
                "normal": None, "internal": [], "token": [], "nft": [],
            })
            if kind == "normal":
                entry["normal"] = row
            else:
                entry[kind].append(row)
        await asyncio.sleep(API_PAUSE_SEC)

    return grouped


def _fmt_amount(raw_value, decimals) -> str:
    try:
        d = int(decimals)
        value = Decimal(str(raw_value)) / (10 ** d)
    except Exception:
        return str(raw_value)
    if value == value.to_integral_value():
        return f"{value:,.0f}"
    if value < 1:
        return f"{value:,.6f}".rstrip("0").rstrip(".")
    return f"{value:,.4f}".rstrip("0").rstrip(".")


def _short(addr: str) -> str:
    if not addr or len(addr) < 12:
        return addr or "—"
    return f"{addr[:6]}…{addr[-4:]}"


def _format_activity(wallet: str, entry: dict) -> str:
    lines = []
    normal = entry.get("normal")
    w = wallet.lower()

    # Заголовок и направление
    header = "🔔 <b>Активность кошелька</b>"
    if normal:
        if str(normal.get("isError", "0")) == "1" or str(normal.get("txreceipt_status", "1")) == "0":
            header = "❌ <b>Неудачная транзакция</b>"
        elif (normal.get("from") or "").lower() == w:
            header = "📤 <b>Исходящая транзакция</b>"
        else:
            header = "📥 <b>Входящая транзакция</b>"
    lines.append(header)
    lines.append("")

    # Переводы самого ETH — из обычной транзакции
    if normal:
        try:
            eth_value = Decimal(normal.get("value", "0")) / Decimal(10 ** 18)
        except Exception:
            eth_value = Decimal(0)
        if eth_value > 0:
            direction = "отправлено" if (normal.get("from") or "").lower() == w else "получено"
            lines.append(f"💎 ETH {direction}: <b>{_fmt_amount(normal.get('value'), 18)}</b>")

        counterparty = normal.get("to") if (normal.get("from") or "").lower() == w else normal.get("from")
        if counterparty:
            lines.append(f"Контрагент: <code>{counterparty}</code>")

        func = (normal.get("functionName") or "").strip()
        if func:
            lines.append(f"⚙️ Метод: {func.split('(')[0]}")

    # Внутренние переводы ETH (например, вывод из контракта)
    for item in entry.get("internal", [])[:5]:
        try:
            if Decimal(item.get("value", "0")) <= 0:
                continue
        except Exception:
            continue
        direction = "отправлено" if (item.get("from") or "").lower() == w else "получено"
        lines.append(f"💎 ETH {direction} (внутр.): <b>{_fmt_amount(item.get('value'), 18)}</b>")

    # Переводы токенов
    token_lines = []
    for item in entry.get("token", []):
        symbol = item.get("tokenSymbol") or "?"
        amount = _fmt_amount(item.get("value"), item.get("tokenDecimal", 18))
        if (item.get("from") or "").lower() == w:
            token_lines.append(f"  🔴 −{amount} {symbol} → {_short(item.get('to'))}")
        else:
            token_lines.append(f"  🟢 +{amount} {symbol} ← {_short(item.get('from'))}")
    if token_lines:
        lines.append("")
        lines.append("🪙 <b>Токены:</b>")
        lines.extend(token_lines[:10])
        if len(token_lines) > 10:
            lines.append(f"  …и ещё {len(token_lines) - 10}")

    # NFT
    nft_lines = []
    for item in entry.get("nft", []):
        name = item.get("tokenName") or item.get("tokenSymbol") or "NFT"
        token_id = item.get("tokenID") or item.get("tokenId") or "?"
        if (item.get("from") or "").lower() == w:
            nft_lines.append(f"  🔴 отправлен {name} #{token_id}")
        else:
            nft_lines.append(f"  🟢 получен {name} #{token_id}")
    if nft_lines:
        lines.append("")
        lines.append("🖼 <b>NFT:</b>")
        lines.extend(nft_lines[:5])

    # Время и ссылка
    ts = entry.get("time")
    if ts:
        when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
        lines.append("")
        lines.append(f"🕐 {when}")

    lines.append(f'<a href="{EXPLORER}/tx/{entry["hash"]}">Смотреть транзакцию</a>')

    if len(WALLETS) > 1:
        lines.append(f"<i>кошелёк {_short(wallet)}</i>")

    return "\n".join(lines)


async def _notify_error(bot, text: str):
    """Ошибки в Telegram, но не чаще раза в 10 минут."""
    now = time.time()
    if now - state.last_error_notified_at < 600:
        return
    state.last_error_notified_at = now
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"⚠️ {text}")
    except Exception:
        pass


# ============================================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================================
async def check_wallets(context: ContextTypes.DEFAULT_TYPE):
    for wallet, ws in state.wallets.items():
        if ws.last_block == 0:
            # Ещё не знаем стартовый блок — ставим текущий, чтобы не
            # присылать всю историю кошелька разом
            try:
                ws.last_block = await _get_current_block()
                log.info("Кошелёк %s: старт с блока %s", _short(wallet), ws.last_block)
            except Exception as e:
                log.warning("Не смог получить номер блока: %s", _sanitize(e))
            continue

        try:
            grouped = await _fetch_activity(wallet, ws.last_block + 1)
        except Exception as e:
            log.warning("Кошелёк %s: ошибка запроса: %s", _short(wallet), _sanitize(e))
            await _notify_error(context.bot, f"Не удалось получить данные: {_sanitize(e)}")
            continue

        if not grouped:
            continue

        max_block = ws.last_block
        for tx_hash, entry in sorted(grouped.items(), key=lambda kv: kv[1]["block"]):
            max_block = max(max_block, entry["block"])
            if ws.already_seen(tx_hash):
                continue
            ws.remember(tx_hash)

            if not state.alerts_enabled:
                continue

            try:
                await context.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=_format_activity(wallet, entry),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                state.alerts_sent += 1
                log.info("Уведомление: %s блок %s", tx_hash[:12], entry["block"])
            except Exception as e:
                log.warning("Не смог отправить сообщение: %s", _sanitize(e))

        ws.last_block = max_block


# ============================================================================
# КОМАНДЫ
# ============================================================================
def _authorized(update: Update) -> bool:
    if not TELEGRAM_CHAT_ID:
        return True
    return str(update.effective_chat.id) == str(TELEGRAM_CHAT_ID)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wallets = "\n".join(f"<code>{w}</code>" for w in WALLETS)
    await update.message.reply_text(
        f"Слежу за активностью кошельков:\n{wallets}\n\n"
        "Присылаю уведомление на каждую транзакцию: переводы ETH, токенов, "
        "NFT и вызовы контрактов.\n\n"
        "Команды:\n"
        "/status — состояние бота\n"
        "/recent — последние транзакции кошелька\n"
        "/balance — текущий баланс ETH\n"
        "/pause — приостановить уведомления\n"
        "/resume — возобновить уведомления",
        parse_mode=ParseMode.HTML,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = int((time.time() - state.started_at) / 60) if state.started_at else 0
    lines = [
        "⚙️ <b>Статус</b>\n",
    ]
    for wallet, ws in state.wallets.items():
        lines.append(f"Кошелёк: <code>{wallet}</code>")
        lines.append(f"Последний блок: {ws.last_block or '—'}")
    lines.append(f"\nУведомления: {'включены ✅' if state.alerts_enabled else 'на паузе ⏸'}")
    lines.append(f"Отправлено уведомлений: {state.alerts_sent}")
    lines.append(f"Запросов к Etherscan: {state.api_calls} (лимит 100 000/сутки)")
    lines.append(f"Работает: {uptime} мин.")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _authorized(update):
        return
    state.alerts_enabled = False
    await update.message.reply_text("Уведомления приостановлены. /resume — включить обратно.")


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _authorized(update):
        return
    state.alerts_enabled = True
    await update.message.reply_text("Уведомления снова включены.")


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Смотрю баланс...")
    lines = []
    for wallet in WALLETS:
        try:
            query = {
                "chainid": CHAIN_ID, "module": "account", "action": "balance",
                "address": wallet, "tag": "latest", "apikey": ETHERSCAN_API_KEY,
            }
            async with http_session.get(
                ETHERSCAN_BASE, params=query, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                state.api_calls += 1
                data = await resp.json(content_type=None)
                raw = data.get("result")
                lines.append(f"<code>{_short(wallet)}</code>: <b>{_fmt_amount(raw, 18)} ETH</b>")
        except Exception as e:
            lines.append(f"<code>{_short(wallet)}</code>: ошибка ({_sanitize(e)[:60]})")
        await asyncio.sleep(API_PAUSE_SEC)
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_recent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает последние транзакции — проверка, что всё работает,
    без ожидания новой активности."""
    await update.message.reply_text("Смотрю последние транзакции...")
    wallet = WALLETS[0]
    try:
        rows = await _etherscan({
            "module": "account", "action": "txlist", "address": wallet,
            "startblock": 0, "endblock": 99999999,
            "page": 1, "offset": 5, "sort": "desc",
        })
    except Exception as e:
        await update.message.reply_text(
            f"❌ Ошибка запроса к Etherscan:\n<code>{_sanitize(e)}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not rows:
        await update.message.reply_text(
            "Связь с Etherscan работает ✅, но у кошелька нет транзакций."
        )
        return

    lines = ["<b>Последние транзакции:</b>\n"]
    for row in rows:
        ts = int(row.get("timeStamp", 0) or 0)
        when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m %H:%M") if ts else "—"
        direction = "📤" if (row.get("from") or "").lower() == wallet else "📥"
        eth = _fmt_amount(row.get("value"), 18)
        func = (row.get("functionName") or "").split("(")[0]
        descr = func if func else ("перевод ETH" if eth != "0" else "—")
        lines.append(f"{direction} {when} · {descr}")
        if eth != "0":
            lines.append(f"    {eth} ETH")
        lines.append(f"    {EXPLORER}/tx/{row.get('hash')}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML,
                                     disable_web_page_preview=True)


# ============================================================================
# ЗАПУСК
# ============================================================================
async def _post_init(application: Application):
    global http_session
    http_session = aiohttp.ClientSession()
    state.started_at = time.time()

    problems = []

    if not ETHERSCAN_API_KEY:
        problems.append("не задан ETHERSCAN_API_KEY")

    # Самопроверка: реально ли работает связь с Etherscan
    if ETHERSCAN_API_KEY:
        try:
            block = await _get_current_block()
            for ws in state.wallets.values():
                ws.last_block = block
            log.info("Связь с Etherscan работает, текущий блок %s", block)
        except Exception as e:
            problems.append(f"связь с Etherscan не работает: {_sanitize(e)}")

    if TELEGRAM_CHAT_ID:
        if problems:
            text = "⚠️ Бот запустился, но есть проблемы:\n\n" + "\n".join(f"• {p}" for p in problems)
        else:
            wallets = "\n".join(f"<code>{w}</code>" for w in WALLETS)
            text = (
                "✅ Бот запущен, связь с Etherscan проверена.\n\n"
                f"Слежу за:\n{wallets}\n\n"
                "Проверить прямо сейчас: /recent"
            )
        try:
            await application.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.HTML
            )
        except Exception as e:
            log.warning("Не смог отправить стартовое сообщение: %s", _sanitize(e))


def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")
    if not WALLETS:
        raise RuntimeError("WALLET_ADDRESS не задан")
    if not TELEGRAM_CHAT_ID:
        log.warning("TELEGRAM_CHAT_ID не задан — уведомления слать некуда")

    threading.Thread(target=_run_health_server, daemon=True).start()
    log.info("Health-сервер на порту %s", PORT)

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(_post_init).build()
    app.add_handler(CommandHandler(["start", "help"], cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("recent", cmd_recent))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.job_queue.run_repeating(check_wallets, interval=POLL_INTERVAL_SEC, first=10)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
