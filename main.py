import asyncio
import logging
import os
import sqlite3
import time
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import FormData

# ==================== НАСТРОЙКИ ====================
TOKEN = "8837204070:AAEwbxWZbWnnMMfcdJS1YbyWubk7LJM83Ak"
PDF_URL = "https://priem.mgpk.by/uploads/schedule/substitutions.pdf"
REFERER = "https://priem.mgpk.by/students/schedule.html"
PDF_PATH = "substitutions.pdf"
DB_FILE = "bot.db"
CHECK_INTERVAL_MINUTES = 15
API = f"https://api.telegram.org/bot{TOKEN}"
CHUNK = 64 * 1024

# Часовой пояс Москвы (UTC+3)
TZ = ZoneInfo("Europe/Moscow")

# Админ по username (без @)
ADMIN_USERNAME = "Skeletns"

# Анти-спам
RATE_LIMIT = 100        # запросов
RATE_WINDOW = 60        # за N секунд
MUTE_SECONDS = 300      # мут 5 минут

# PDF-расписания рядом с main.py
SCHEDULES = [
    ("1 курс", "course-1.pdf", "I семестр 2026-2027"),
    ("2 курс", "course-2.pdf", "III семестр 2026-2027"),
    ("3 курс", "course-3.pdf", "V семестр 2026-2027"),
    ("4 курс", "course-4.pdf", "VII семестр 2026-2027"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bot")

session: aiohttp.ClientSession | None = None
bcast_session: aiohttp.ClientSession | None = None

# Анти-спам (в памяти)
_rate: dict[int, deque] = {}
_muted_until: dict[int, float] = {}


# ==================== БАЗА ====================
_db = None


def db() -> sqlite3.Connection:
    global _db
    if _db is None:
        _db = sqlite3.connect(DB_FILE, check_same_thread=False, isolation_level=None)
        _db.execute("PRAGMA journal_mode=WAL")
        _db.execute("PRAGMA synchronous=NORMAL")
        _db.execute(
            "CREATE TABLE IF NOT EXISTS subscribers ("
            "chat_id INTEGER PRIMARY KEY, subscribed INTEGER DEFAULT 1)"
        )
        _db.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "chat_id INTEGER PRIMARY KEY, username TEXT, "
            "banned INTEGER DEFAULT 0, created_at INTEGER)"
        )
        _db.execute(
            "CREATE TABLE IF NOT EXISTS activity ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, "
            "username TEXT, action TEXT, ts INTEGER)"
        )
        _db.execute("CREATE INDEX IF NOT EXISTS ix_act_chat ON activity(chat_id, ts)")
        _db.execute("CREATE INDEX IF NOT EXISTS ix_act_ts ON activity(ts)")
    return _db


def register_user(cid: int, username: str | None):
    db().execute(
        "INSERT INTO users(chat_id, username, created_at) VALUES(?,?,?) "
        "ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username",
        (cid, username, int(time.time())),
    )


def is_banned(cid: int) -> bool:
    r = db().execute("SELECT banned FROM users WHERE chat_id=?", (cid,)).fetchone()
    return bool(r and r[0])


def set_ban(cid: int, banned: bool):
    db().execute(
        "UPDATE users SET banned=? WHERE chat_id=?",
        (1 if banned else 0, cid),
    )


def log_action(cid: int, username: str | None, action: str):
    db().execute(
        "INSERT INTO activity(chat_id, username, action, ts) VALUES(?,?,?,?)",
        (cid, username, action[:200], int(time.time())),
    )


def cleanup_activity(days: int = 7):
    cutoff = int(time.time()) - days * 86400
    db().execute("DELETE FROM activity WHERE ts < ?", (cutoff,))


def is_admin(username: str | None) -> bool:
    return bool(username and username.lower() == ADMIN_USERNAME.lower())


# ---------- Подписка ----------
def subscribe(cid: int):
    db().execute(
        "INSERT OR REPLACE INTO subscribers(chat_id, subscribed) VALUES(?, 1)", (cid,)
    )


def unsubscribe(cid: int):
    db().execute("UPDATE subscribers SET subscribed=0 WHERE chat_id=?", (cid,))


def is_subscribed(cid: int) -> bool:
    r = db().execute(
        "SELECT subscribed FROM subscribers WHERE chat_id=?", (cid,)
    ).fetchone()
    return bool(r and r[0])


def all_subs():
    return [
        r[0]
        for r in db().execute(
            "SELECT chat_id FROM subscribers WHERE subscribed=1"
        ).fetchall()
    ]


def count_subs() -> int:
    return db().execute(
        "SELECT COUNT(*) FROM subscribers WHERE subscribed=1"
    ).fetchone()[0]


# ==================== АНТИ-СПАМ ====================
def check_rate(cid: int) -> bool:
    """True — можно пропустить. False — лимит превышен или в муте."""
    now = time.time()
    if _muted_until.get(cid, 0) > now:
        return False

    dq = _rate.setdefault(cid, deque())
    while dq and now - dq[0] > RATE_WINDOW:
        dq.popleft()
    dq.append(now)

    if len(dq) > RATE_LIMIT:
        _muted_until[cid] = now + MUTE_SECONDS
        dq.clear()
        return False
    return True


def mute_remaining(cid: int) -> int:
    return max(0, int(_muted_until.get(cid, 0) - time.time()))


# ==================== PDF ЗАМЕН ====================
_chttp = None


def _get_chttp():
    global _chttp
    if _chttp is None:
        from curl_cffi import requests as r
        _chttp = r
    return _chttp


_HEADERS = {
    "Referer": REFERER,
    "Accept": "application/pdf,application/octet-stream,*/*",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}


def _fetch_sync(url: str, impersonate: str):
    try:
        r = _get_chttp().get(
            url,
            impersonate=impersonate,
            headers=_HEADERS,
            timeout=60,
            verify=False,
            allow_redirects=True,
        )
        if r.status_code == 200 and r.content.startswith(b"%PDF"):
            return r.content
    except Exception as e:
        log.warning("fetch: %s", e)
    return None


async def fetch_pdf():
    ts_url = f"{PDF_URL}?t={int(time.time())}"
    for u, imp in ((PDF_URL, "chrome"), (ts_url, "chrome"), (PDF_URL, "firefox")):
        c = await asyncio.to_thread(_fetch_sync, u, imp)
        if c is not None:
            return c
    return None


def same_as_disk(content: bytes) -> bool:
    if not os.path.exists(PDF_PATH):
        return False
    try:
        if os.path.getsize(PDF_PATH) != len(content):
            return False
        with open(PDF_PATH, "rb") as f:
            off = 0
            while True:
                chunk = f.read(CHUNK)
                if not chunk:
                    return True
                if chunk != content[off:off + CHUNK]:
                    return False
                off += len(chunk)
    except OSError:
        return False


def save_pdf(content: bytes):
    with open(PDF_PATH, "wb") as f:
        f.write(content)


def pdf_date_str() -> str:
    if not os.path.exists(PDF_PATH):
        return "нет данных"
    return datetime.fromtimestamp(os.path.getmtime(PDF_PATH), tz=TZ).strftime(
        "%d.%m.%Y в %H:%M"
    )


def pdf_age_str() -> str:
    if not os.path.exists(PDF_PATH):
        return "PDF ещё не скачивался"
    secs = int(time.time() - os.path.getmtime(PDF_PATH))
    if secs < 60:
        return "только что"
    if secs < 3600:
        return f"{secs // 60} мин назад"
    if secs < 86400:
        return f"{secs // 3600} ч назад"
    return f"{secs // 86400} дн назад"


async def download_if_changed() -> bool:
    content = await fetch_pdf()
    if content is None:
        return False
    if same_as_disk(content):
        log.info("PDF не изменился")
        return False
    save_pdf(content)
    log.info("✅ Новый PDF %db", len(content))
    return True


# ==================== TELEGRAM API ====================
async def api(method: str, params: dict | None = None, timeout: int = 40):
    try:
        async with session.post(
            f"{API}/{method}",
            json=params or {},
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as r:
            return await r.json()
    except Exception as e:
        log.warning("%s: %s", method, e)
        return {"ok": False, "description": str(e)}


async def send_message(chat_id: int, text: str, keyboard: dict | None = None):
    p = {"chat_id": chat_id, "text": text}
    if keyboard is not None:
        p["reply_markup"] = keyboard
    return await api("sendMessage", p)


async def edit_message(chat_id: int, message_id: int, text: str, keyboard: dict):
    return await api(
        "editMessageText",
        {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "reply_markup": keyboard,
        },
    )


async def send_document(chat_id: int, path: str, filename: str,
                        caption: str | None = None):
    data = FormData()
    data.add_field("chat_id", str(chat_id))
    if caption:
        data.add_field("caption", caption)
    try:
        with open(path, "rb") as f:
            data.add_field("document", f, filename=filename,
                           content_type="application/pdf")
            async with session.post(
                f"{API}/sendDocument",
                data=data,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as r:
                return await r.json()
    except Exception as e:
        log.warning("sendDocument: %s", e)
        return {"ok": False, "description": str(e)}


async def send_document_bcast(chat_id: int, path: str, filename: str,
                              caption: str | None = None):
    data = FormData()
    data.add_field("chat_id", str(chat_id))
    if caption:
        data.add_field("caption", caption)
    try:
        with open(path, "rb") as f:
            data.add_field("document", f, filename=filename,
                           content_type="application/pdf")
            async with bcast_session.post(
                f"{API}/sendDocument",
                data=data,
                timeout=aiohttp.ClientTimeout(total=180),
            ) as r:
                return await r.json()
    except Exception as e:
        log.warning("sendDocument(bcast): %s", e)
        return {"ok": False, "description": str(e)}


def _parse_retry_after(desc: str) -> int:
    try:
        if "retry after" in desc.lower():
            part = desc.lower().split("retry after", 1)[1]
            return int(part.strip().split()[0])
    except Exception:
        pass
    return 5


async def setup_commands():
    cmds = [
        {"command": "start",         "description": "🏠 Главное меню"},
        {"command": "substitutions", "description": "📄 Актуальные замены"},
        {"command": "schedule",      "description": "📚 Расписания занятий"},
        {"command": "settings",      "description": "⚙️ Настройки"},
    ]
    res = await api("setMyCommands", {"commands": cmds})
    if res.get("ok"):
        log.info("Команды бота зарегистрированы")
    else:
        log.warning("setMyCommands: %s", res.get("description"))
    await api("setChatMenuButton", {"menu_button": {"type": "commands"}})


# ==================== КЛАВИАТУРЫ ====================
def kb_main() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "📄 Актуальные замены", "callback_data": "now"},
                {"text": "⚙️ Настройки", "callback_data": "settings"},
            ],
            [
                {"text": "📚 Расписания занятий", "callback_data": "sched"},
            ],
        ]
    }


def kb_reply() -> dict:
    return {
        "keyboard": [
            [{"text": "📄 Актуальные замены"}, {"text": "📚 Расписания"}],
            [{"text": "⚙️ Настройки"}],
        ],
        "resize_keyboard": True,
        "persistent": True,
        "input_field_placeholder": "Выбери действие…",
    }


def kb_settings(uid: int) -> dict:
    toggle = (
        {"text": "🔕 Выключить уведомления", "callback_data": "toggle"}
        if is_subscribed(uid)
        else {"text": "🔔 Включить уведомления", "callback_data": "toggle"}
    )
    return {
        "inline_keyboard": [
            [toggle],
            [{"text": "◀️ Назад", "callback_data": "back"}],
        ]
    }


def kb_schedule() -> dict:
    rows = [
        [{"text": f"📘 {title} ({sub})", "callback_data": f"sched:{i}"}]
        for i, (title, _fn, sub) in enumerate(SCHEDULES)
    ]
    rows.append([{"text": "◀️ Назад", "callback_data": "back"}])
    return {"inline_keyboard": rows}


def kb_admin() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "👥 Пользователи", "callback_data": "adm:users:0"}],
            [{"text": "🚫 Бан-лист", "callback_data": "adm:bans"}],
            [{"text": "📋 Журнал активности", "callback_data": "adm:act"}],
            [{"text": "🔄 Обновить", "callback_data": "adm"}],
        ]
    }


def kb_admin_user(cid: int, banned: bool) -> dict:
    ban_btn = (
        {"text": "✅ Разбанить", "callback_data": f"adm:unban:{cid}"}
        if banned
        else {"text": "🚫 Забанить", "callback_data": f"adm:ban:{cid}"}
    )
    return {
        "inline_keyboard": [
            [ban_btn],
            [{"text": "◀️ К списку", "callback_data": "adm:users:0"}],
        ]
    }


def kb_admin_users(page: int) -> dict:
    per = 10
    offset = page * per
    rows = db().execute(
        "SELECT u.chat_id, u.username, u.banned, "
        "COALESCE((SELECT MAX(ts) FROM activity a WHERE a.chat_id=u.chat_id), "
        "u.created_at) AS last_ts "
        "FROM users u ORDER BY last_ts DESC LIMIT ? OFFSET ?",
        (per, offset),
    ).fetchall()
    total = db().execute("SELECT COUNT(*) FROM users").fetchone()[0]

    kb_rows = []
    for cid, uname, banned, last_ts in rows:
        mark = "🚫 " if banned else ""
        name = f"@{uname}" if uname else f"id{cid}"
        t = datetime.fromtimestamp(last_ts, tz=TZ).strftime("%d.%m %H:%M")
        kb_rows.append([{
            "text": f"{mark}{name} · {t}",
            "callback_data": f"adm:u:{cid}",
        }])

    nav = []
    if page > 0:
        nav.append({"text": "◀️", "callback_data": f"adm:users:{page - 1}"})
    if offset + per < total:
        nav.append({"text": "▶️", "callback_data": f"adm:users:{page + 1}"})
    if nav:
        kb_rows.append(nav)

    kb_rows.append([{"text": "◀️ В админку", "callback_data": "adm"}])
    return {"inline_keyboard": kb_rows}


def kb_admin_bans() -> dict:
    rows = db().execute(
        "SELECT chat_id, username FROM users WHERE banned=1 ORDER BY created_at DESC LIMIT 30"
    ).fetchall()
    kb_rows = []
    for cid, uname in rows:
        name = f"@{uname}" if uname else f"id{cid}"
        kb_rows.append([{
            "text": f"✅ Разбанить {name}",
            "callback_data": f"adm:unban:{cid}",
        }])
    if not kb_rows:
        kb_rows.append([{"text": "— пусто —", "callback_data": "adm"}])
    kb_rows.append([{"text": "◀️ В админку", "callback_data": "adm"}])
    return {"inline_keyboard": kb_rows}


# ==================== ТЕКСТЫ ====================
def main_text() -> str:
    return (
        "👋 Привет! Я бот замен МПК.\n\n"
        "Слежу за расписанием и присылаю свежие замены, "
        "как только они появляются на сайте колледжа.\n\n"
        "Выбери действие:"
    )


def settings_text(uid: int) -> str:
    state = "🔔 включены" if is_subscribed(uid) else "🔕 выключены"
    return (
        "⚙️ *Настройки*\n\n"
        f"Уведомления: {state}\n"
        f"Подписчиков всего: {count_subs()}\n\n"
        "📋 *Что умеет бот*\n"
        "• Проверяет сайт МПК каждые 15 минут\n"
        "• Присылает PDF с заменами сразу после обновления\n"
        "• По кнопке «Актуальные замены» показывает свежий файл с датой\n"
        "• Раздел «Расписания занятий» — PDF на 1–4 курс\n"
        "• Если уведомления выключены — просто тихо обновляет файл\n\n"
        f"🗓 Последнее обновление замен: {pdf_date_str()} ({pdf_age_str()})"
    )


def schedule_text() -> str:
    return (
        "📚 *Расписания занятий*\n\n"
        "Выбери свой курс — пришлю PDF-файл.\n"
        "Файл векторный, можно зумить без потери качества."
    )


def admin_panel_text() -> str:
    total = db().execute("SELECT COUNT(*) FROM users").fetchone()[0]
    banned = db().execute("SELECT COUNT(*) FROM users WHERE banned=1").fetchone()[0]
    muted = sum(1 for cid, t in _muted_until.items() if t > time.time())
    return (
        "🛠 *Админ-панель*\n\n"
        f"👥 Пользователей: {total}\n"
        f"🚫 В бане: {banned}\n"
        f"⏸ В муте: {muted}\n"
        f"🔔 Подписчиков: {count_subs()}\n"
    )


def admin_user_text(cid: int) -> str:
    row = db().execute(
        "SELECT username, banned, created_at FROM users WHERE chat_id=?", (cid,)
    ).fetchone()
    if not row:
        return "Пользователь не найден."
    uname, banned, created = row
    acts = db().execute(
        "SELECT action, ts FROM activity WHERE chat_id=? ORDER BY ts DESC LIMIT 15",
        (cid,),
    ).fetchall()

    rem = mute_remaining(cid)
    if banned:
        status = "🚫 забанен"
    elif rem > 0:
        status = f"⏸ в муте ({rem} с)"
    else:
        status = "✅ активен"

    lines = [
        "👤 *Пользователь*",
        f"ID: `{cid}`",
        f"Username: @{uname}" if uname else "Username: —",
        f"Статус: {status}",
        f"Первый контакт: "
        f"{datetime.fromtimestamp(created or 0, tz=TZ).strftime('%d.%m.%Y %H:%M')}",
        "",
        "📋 *Последние действия:*",
    ]
    if not acts:
        lines.append("— пусто —")
    for action, ts in acts:
        t = datetime.fromtimestamp(ts, tz=TZ).strftime("%d.%m %H:%M:%S")
        lines.append(f"`{t}` — {action}")

    return "\n".join(lines)


def admin_activity_text() -> str:
    rows = db().execute(
        "SELECT chat_id, username, action, ts FROM activity "
        "ORDER BY ts DESC LIMIT 40"
    ).fetchall()
    lines = ["📋 *Последняя активность*", ""]
    if not rows:
        lines.append("— пусто —")
    for cid, uname, action, ts in rows:
        t = datetime.fromtimestamp(ts, tz=TZ).strftime("%d.%m %H:%M:%S")
        who = f"@{uname}" if uname else f"id{cid}"
        lines.append(f"`{t}` {who}: {action[:70]}")
    return "\n".join(lines)


# ==================== СЦЕНАРИИ ====================
async def show_current(chat_id: int):
    if not os.path.exists(PDF_PATH):
        await send_message(chat_id, "🔄 Скачиваю актуальный PDF...")
        content = await fetch_pdf()
        if content is None:
            await send_message(chat_id, "❌ Не удалось получить файл с сайта.")
            return
        save_pdf(content)
    else:
        asyncio.create_task(_refresh_bg())

    caption = (
        f"📄 Актуальные замены\n"
        f"🗓 Обновлено: {pdf_date_str()} ({pdf_age_str()})"
    )
    await send_document(chat_id, PDF_PATH, "Замены.pdf", caption)


async def _refresh_bg():
    try:
        if await download_if_changed():
            log.info("PDF обновлён в фоне")
    except Exception as e:
        log.warning("bg refresh: %s", e)


async def send_schedule(chat_id: int, idx: int):
    if idx < 0 or idx >= len(SCHEDULES):
        await send_message(chat_id, "❌ Неверный номер курса.")
        return
    title, fname, sub = SCHEDULES[idx]
    if not os.path.exists(fname):
        log.warning("Нет файла %s", fname)
        await send_message(
            chat_id,
            f"❌ Файл «{title}» не найден рядом с ботом ({fname}).",
        )
        return
    await send_document(
        chat_id, fname, f"{title}.pdf", f"📘 Расписание — {title}\n🗓 {sub}"
    )


# ==================== РАССЫЛКА ====================
async def broadcast():
    log.info("Проверяю замены...")
    if not await download_if_changed():
        return
    if not os.path.exists(PDF_PATH):
        log.warning("PDF-файл отсутствует — рассылка пропущена")
        return

    subs = all_subs()
    if not subs:
        log.info("Подписчиков нет")
        return

    log.info("Рассылка на %d", len(subs))
    caption = f"🔔 Новые замены\n🗓 Обновлено: {pdf_date_str()}"
    sent = failed = 0

    for cid in subs:
        if is_banned(cid):
            continue
        res = await send_document_bcast(cid, PDF_PATH, "Замены.pdf", caption)

        if not res.get("ok"):
            desc = res.get("description") or ""
            if "too many requests" in desc.lower() or "retry after" in desc.lower():
                delay = _parse_retry_after(desc)
                log.warning("Rate limit: ждём %d сек", delay)
                await asyncio.sleep(delay)
                res = await send_document_bcast(cid, PDF_PATH, "Замены.pdf", caption)

        if res.get("ok"):
            sent += 1
        else:
            failed += 1
            err = (res.get("description") or "").lower()
            if any(k in err for k in ("blocked", "not found",
                                      "deactivated", "chat not found",
                                      "user is deactivated", "bot was blocked")):
                unsubscribe(cid)

        await asyncio.sleep(0.1)

    log.info("OK: %d, ошибок: %d", sent, failed)


# ==================== ОБРАБОТЧИКИ ====================
async def on_message(msg: dict):
    chat_id = msg["chat"]["id"]
    uid = chat_id
    uname = (msg.get("from") or {}).get("username")
    text = msg.get("text", "") or ""
    cmd = text.split()[0].split("@")[0] if text.startswith("/") else ""

    if cmd == "/start":
        await send_message(chat_id, main_text(), kb_main())
        await send_message(chat_id, "Кнопки быстрого доступа снизу 👇", kb_reply())
        return

    if cmd == "/substitutions":
        await show_current(chat_id)
        return

    if cmd == "/schedule":
        await send_message(chat_id, schedule_text(), kb_schedule())
        return

    if cmd == "/settings":
        await send_message(chat_id, settings_text(uid), kb_settings(uid))
        return

    if cmd == "/admin":
        if not is_admin(uname):
            await send_message(chat_id, "⛔ Команда недоступна.")
            return
        await send_message(chat_id, admin_panel_text(), kb_admin())
        return

    if text == "📄 Актуальные замены":
        await show_current(chat_id)
    elif text == "📚 Расписания":
        await send_message(chat_id, schedule_text(), kb_schedule())
    elif text == "⚙️ Настройки":
        await send_message(chat_id, settings_text(uid), kb_settings(uid))


async def on_callback(cb: dict):
    await api("answerCallbackQuery", {"callback_query_id": cb["id"]})

    data = cb.get("data", "")
    msg = cb.get("message") or {}
    chat_id = msg.get("chat", {}).get("id")
    message_id = msg.get("message_id")
    from_user = cb.get("from") or {}
    uid = from_user.get("id")
    uname = from_user.get("username")
    if not chat_id or uid is None:
        return

    # --- Админ-колбэки ---
    if data == "adm" or data.startswith("adm:"):
        if not is_admin(uname):
            return
        if data == "adm":
            await edit_message(chat_id, message_id,
                               admin_panel_text(), kb_admin())
        elif data.startswith("adm:users:"):
            try:
                page = int(data.split(":", 2)[2])
            except ValueError:
                page = 0
            await edit_message(
                chat_id, message_id,
                f"👥 *Пользователи* (стр. {page + 1})",
                kb_admin_users(page),
            )
        elif data == "adm:bans":
            await edit_message(chat_id, message_id,
                               "🚫 *Бан-лист*\nНажми на юзера, чтобы разбанить:",
                               kb_admin_bans())
        elif data == "adm:act":
            await edit_message(chat_id, message_id,
                               admin_activity_text(), kb_admin())
        elif data.startswith("adm:u:"):
            try:
                target = int(data.split(":", 2)[2])
            except ValueError:
                return
            banned = is_banned(target)
            await edit_message(chat_id, message_id,
                               admin_user_text(target),
                               kb_admin_user(target, banned))
        elif data.startswith("adm:ban:"):
            try:
                target = int(data.split(":", 2)[2])
            except ValueError:
                return
            set_ban(target, True)
            log_action(target, None, "🚫 Забанен админом")
            await edit_message(chat_id, message_id,
                               admin_user_text(target),
                               kb_admin_user(target, True))
        elif data.startswith("adm:unban:"):
            try:
                target = int(data.split(":", 2)[2])
            except ValueError:
                return
            set_ban(target, False)
            log_action(target, None, "✅ Разбанен админом")
            await edit_message(chat_id, message_id,
                               admin_user_text(target),
                               kb_admin_user(target, False))
        return

    # --- Обычные колбэки ---
    if data == "now":
        await show_current(chat_id)
    elif data == "settings":
        await edit_message(chat_id, message_id,
                           settings_text(uid), kb_settings(uid))
    elif data == "toggle":
        if is_subscribed(uid):
            unsubscribe(uid)
        else:
            subscribe(uid)
        await edit_message(chat_id, message_id,
                           settings_text(uid), kb_settings(uid))
    elif data == "sched":
        await edit_message(chat_id, message_id, schedule_text(), kb_schedule())
    elif data.startswith("sched:"):
        try:
            idx = int(data.split(":", 1)[1])
        except ValueError:
            return
        await send_schedule(chat_id, idx)
    elif data == "back":
        await edit_message(chat_id, message_id, main_text(), kb_main())


# ==================== ДИСПЕТЧЕР (бан + анти-спам) ====================
async def dispatch(upd: dict):
    try:
        if "message" in upd:
            m = upd["message"]
            from_user = m.get("from") or {}
            cid = from_user.get("id") or m.get("chat", {}).get("id")
            uname = from_user.get("username")
            text = m.get("text", "") or ""
            action = f"msg: {text[:120]}" if text else f"msg: [{len(str(m))}b]"
        elif "callback_query" in upd:
            cq = upd["callback_query"]
            from_user = cq.get("from") or {}
            cid = from_user.get("id")
            uname = from_user.get("username")
            action = f"cb: {cq.get('data', '')[:120]}"
        else:
            return

        if cid is None:
            return

        register_user(cid, uname)

        # Админ не банится и не мутится
        if not is_admin(uname):
            # Забаненные — полностью игнорируем
            if is_banned(cid):
                return

            # Мут (тихо)
            if mute_remaining(cid) > 0:
                return

            # Лимит запросов
            if not check_rate(cid):
                log_action(cid, uname, "⚠️ MUTED (rate limit)")
                await send_message(
                    cid,
                    "🚫 Слишком много запросов. Ты в муте на 5 минут.",
                )
                return

        # Лог действия
        log_action(cid, uname, action)

        if "message" in upd:
            await on_message(upd["message"])
        elif "callback_query" in upd:
            await on_callback(upd["callback_query"])
    except Exception as e:
        log.exception("handler: %s", e)


# ==================== POLLING ====================
async def poll_loop():
    offset = 0
    backoff = 1
    while True:
        res = await api(
            "getUpdates",
            {
                "offset": offset,
                "timeout": 25,
                "allowed_updates": ["message", "callback_query"],
            },
            timeout=45,
        )
        if not res.get("ok"):
            log.warning("getUpdates failed: %s, retry in %ds",
                        res.get("description"), backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        backoff = 1
        for upd in res.get("result", []):
            offset = upd["update_id"] + 1
            asyncio.create_task(dispatch(upd))


# ==================== ПЕРИОДИЧЕСКАЯ ПРОВЕРКА ====================
async def periodic():
    await asyncio.sleep(60)
    while True:
        try:
            await broadcast()
        except Exception as e:
            log.exception("broadcast: %s", e)
        await asyncio.sleep(CHECK_INTERVAL_MINUTES * 60)


# ==================== ЗАПУСК ====================
async def main():
    global session, bcast_session
    db()
    cleanup_activity(7)

    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=60, connect=10, sock_read=50),
        connector=aiohttp.TCPConnector(
            limit=20, ttl_dns_cache=300, keepalive_timeout=30,
        ),
    )
    bcast_session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=180, connect=15, sock_read=120),
        connector=aiohttp.TCPConnector(
            limit=3, ttl_dns_cache=300, keepalive_timeout=30,
        ),
    )

    await setup_commands()
    asyncio.create_task(periodic())
    log.info("Бот запущен. Проверка каждые %d мин.", CHECK_INTERVAL_MINUTES)

    try:
        await poll_loop()
    finally:
        await session.close()
        await bcast_session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено пользователем")