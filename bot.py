"""
Telegram-бот: статистика лайков и комментариев публичной группы ВК (версия для Render).

Всё управление кнопками: напиши боту /start.
  - выбор диапазона: последние 10 / 20 / 30 / 50 постов
  - ❤️ Лайки и 💬 Комментарии: рейтинг людей; имя открывает страницу ВК,
    кнопка 📋 показывает, какие посты лайкнул / какие комментарии оставил
  - 🔄 Обновить данные: сбор свежих данных из ВК за выбранный диапазон

Переменные окружения: TG_TOKEN, VK_TOKEN, VK_GROUP_ID, ADMIN_ID,
  DATABASE_URL, WEBHOOK_SECRET. RENDER_EXTERNAL_URL и PORT задаёт Render.
"""
import asyncio
import os
import time

import aiohttp
import psycopg
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import CallbackQuery
from aiogram.types import InlineKeyboardButton as Btn
from aiogram.types import InlineKeyboardMarkup, LinkPreviewOptions, Message
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

TG_TOKEN = os.environ["TG_TOKEN"]
VK_TOKEN = os.environ["VK_TOKEN"]
GROUP_ID = int(os.environ["VK_GROUP_ID"])
ADMIN_ID = int(os.environ["ADMIN_ID"])
DATABASE_URL = os.environ["DATABASE_URL"]
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
BASE_URL = os.environ["RENDER_EXTERNAL_URL"].rstrip("/")
PORT = int(os.environ.get("PORT", "10000"))

OWNER = -GROUP_ID
API = "https://api.vk.com/method/"
PAGE = 10
RANGES = (10, 20, 30, 50)
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)
# id постов, входящих в выбранный диапазон (самые свежие по дате)
RANGE_SQL = "(SELECT id FROM posts ORDER BY date DESC LIMIT %s)"

bot = Bot(TG_TOKEN)
dp = Dispatcher()
dp.message.filter(F.from_user.id == ADMIN_ID)
dp.callback_query.filter(F.from_user.id == ADMIN_ID)

state = {"n": 20, "busy": False}
tasks = set()


# ---------- база ----------
def db(sql, params=(), fetch=False):
    with psycopg.connect(DATABASE_URL) as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall() if fetch else None


def init_db():
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS posts(id BIGINT PRIMARY KEY, date BIGINT)")
        conn.execute("CREATE TABLE IF NOT EXISTS users(id BIGINT PRIMARY KEY, name TEXT)")
        conn.execute("""CREATE TABLE IF NOT EXISTS likes(
            user_id BIGINT, post_id BIGINT, PRIMARY KEY(user_id, post_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS comments(
            id BIGINT, post_id BIGINT, user_id BIGINT, date BIGINT, text TEXT,
            PRIMARY KEY(post_id, id))""")


def save_post(pid, date, names, likers, comments):
    """Полностью перезаписывает данные одного поста (чтобы снятые лайки пропадали)."""
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("INSERT INTO posts VALUES(%s,%s) ON CONFLICT DO NOTHING", (pid, date))
        conn.execute("DELETE FROM likes WHERE post_id=%s", (pid,))
        conn.execute("DELETE FROM comments WHERE post_id=%s", (pid,))
        for uid in set(likers) | {c[1] for c in comments}:
            if names.get(uid):
                conn.execute("""INSERT INTO users VALUES(%s,%s)
                                ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name""",
                             (uid, names[uid]))
            else:
                conn.execute("INSERT INTO users VALUES(%s,%s) ON CONFLICT DO NOTHING",
                             (uid, f"id{uid}"))
        for uid in likers:
            conn.execute("INSERT INTO likes VALUES(%s,%s) ON CONFLICT DO NOTHING", (uid, pid))
        for cid, uid, d, text in comments:
            conn.execute("INSERT INTO comments VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                         (cid, pid, uid, d, text))


# ---------- ВК ----------
async def vk(session, method, **params):
    params.update(access_token=VK_TOKEN, v="5.199")
    while True:
        async with session.get(API + method, params=params) as r:
            data = await r.json()
        if "error" in data:
            if data["error"]["error_code"] == 6:  # слишком часто
                await asyncio.sleep(1)
                continue
            raise RuntimeError(data["error"]["error_msg"])
        await asyncio.sleep(0.34)  # лимит ~3 запроса/сек
        return data["response"]


def full_name(u):
    return f'{u.get("first_name", "")} {u.get("last_name", "")}'.strip()


async def fetch_likes(s, pid, names):
    ids, lo = set(), 0
    while True:
        res = await vk(s, "likes.getList", type="post", owner_id=OWNER,
                       item_id=pid, extended=1, count=1000, offset=lo)
        for u in res["items"]:
            if u["id"] > 0:  # пропускаем сообщества
                ids.add(u["id"])
                names[u["id"]] = full_name(u)
        lo += 1000
        if lo >= res["count"]:
            return ids


async def fetch_comments(s, pid, names):
    out = []

    def add(c):
        uid = c.get("from_id", 0)
        if uid > 0 and not c.get("deleted"):
            out.append((c["id"], uid, c["date"], (c.get("text") or "")[:100]))

    def add_profiles(res):
        for p in res.get("profiles", []):
            names[p["id"]] = full_name(p)

    offset = 0
    while True:
        res = await vk(s, "wall.getComments", owner_id=OWNER, post_id=pid, count=100,
                       offset=offset, extended=1, thread_items_count=10, sort="asc")
        add_profiles(res)
        for c in res["items"]:
            add(c)
            th = c.get("thread") or {}
            replies = th.get("items", [])
            if th.get("count", 0) > len(replies):  # ответов больше 10 - докачиваем
                replies, o2 = [], 0
                while True:
                    r2 = await vk(s, "wall.getComments", owner_id=OWNER, post_id=pid,
                                  comment_id=c["id"], count=100, offset=o2, extended=1)
                    add_profiles(r2)
                    replies += r2["items"]
                    if len(r2["items"]) < 100:
                        break
                    o2 += 100
            for t in replies:
                add(t)
        if len(res["items"]) < 100:
            return out
        offset += 100


async def collect(n, progress):
    async with aiohttp.ClientSession() as s:
        wall = await vk(s, "wall.get", owner_id=OWNER, count=min(n + 1, 100))
        # +1 запас на закреплённый пост; берём n самых свежих по дате
        items = sorted(wall["items"], key=lambda p: p["date"], reverse=True)[:n]
        skipped = 0
        for i, post in enumerate(items, 1):
            names = {}
            likers = await fetch_likes(s, post["id"], names)
            try:
                comments = await fetch_comments(s, post["id"], names)
            except RuntimeError:  # например, комментарии закрыты
                comments, skipped = [], skipped + 1
            await asyncio.to_thread(save_post, post["id"], post["date"], names, likers, comments)
            await progress(i, len(items))
        return len(items), skipped


async def keepalive():
    """Пока идёт сбор, стучимся на свой адрес, чтобы Render не усыпил сервис."""
    async with aiohttp.ClientSession() as s:
        while True:
            await asyncio.sleep(600)
            try:
                await s.get(BASE_URL + "/health")
            except Exception:
                pass


async def safe_edit(chat_id, msg_id, text, kb=None):
    try:
        await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id,
                                    reply_markup=kb, link_preview_options=NO_PREVIEW)
    except TelegramBadRequest:
        pass  # например, «message is not modified»


async def run_update(chat_id, msg_id, n):
    ka = asyncio.create_task(keepalive())
    last = 0.0

    async def progress(i, total):
        nonlocal last
        if time.time() - last > 4 or i == total:
            last = time.time()
            await safe_edit(chat_id, msg_id, f"⏳ Собираю данные: пост {i} из {total}…")

    try:
        total, skipped = await collect(n, progress)
        note = f"\n(у {skipped} постов комментарии недоступны)" if skipped else ""
        head = f"✅ Готово, обработано постов: {total}.{note}\n\n"
        await safe_edit(chat_id, msg_id, head + await asyncio.to_thread(menu_text), menu_kb())
    except Exception as e:
        await safe_edit(chat_id, msg_id,
                        f"⚠️ Ошибка при сборе: {e}\nПрогресс сохранён, можно нажать «Обновить» ещё раз.",
                        menu_kb())
    finally:
        state["busy"] = False
        ka.cancel()


# ---------- экраны ----------
def fmt(ts):
    return time.strftime("%d.%m.%Y", time.gmtime(ts))


def menu_kb():
    n = state["n"]
    ranges = [Btn(text=("✅ " if r == n else "") + str(r), callback_data=f"n:{r}") for r in RANGES]
    return InlineKeyboardMarkup(inline_keyboard=[
        ranges,
        [Btn(text="❤️ Лайки", callback_data="top:l:0"),
         Btn(text="💬 Комментарии", callback_data="top:c:0")],
        [Btn(text="🔄 Обновить данные", callback_data="upd")],
    ])


def menu_text():
    n = state["n"]
    k = db(f"SELECT COUNT(*) FROM {RANGE_SQL} t", (n,), True)[0][0]
    lk = db(f"SELECT COUNT(*), COUNT(DISTINCT user_id) FROM likes WHERE post_id IN {RANGE_SQL}",
            (n,), True)[0]
    cm = db(f"SELECT COUNT(*), COUNT(DISTINCT user_id) FROM comments WHERE post_id IN {RANGE_SQL}",
            (n,), True)[0]
    text = (f"📊 Статистика сообщества\n"
            f"Диапазон: последние {n} постов (в базе: {k})\n\n"
            f"❤️ Лайков: {lk[0]} (людей: {lk[1]})\n"
            f"💬 Комментариев: {cm[0]} (людей: {cm[1]})\n\n"
            f"Выбери диапазон сверху, затем раздел.")
    if k < n:
        text += "\nДанных меньше, чем выбрано: нажми «🔄 Обновить данные»."
    return text


KIND = {"l": ("likes", "❤️ Лайки"), "c": ("comments", "💬 Комментарии")}


def top_view(kind, page):
    table, title = KIND[kind]
    n = state["n"]
    rows = db(f"""SELECT u.id, u.name, COUNT(*) c FROM {table} t
                  JOIN users u ON u.id = t.user_id
                  WHERE t.post_id IN {RANGE_SQL}
                  GROUP BY u.id, u.name ORDER BY c DESC, u.id LIMIT %s OFFSET %s""",
              (n, PAGE, page * PAGE), True)
    kb = [[Btn(text=f"{name} — {c}", url=f"https://vk.com/id{uid}"),
           Btn(text="📋", callback_data=f"u:{kind}:{uid}")] for uid, name, c in rows]
    nav = []
    if page > 0:
        nav.append(Btn(text="⬅️", callback_data=f"top:{kind}:{page - 1}"))
    if len(rows) == PAGE:
        nav.append(Btn(text="➡️", callback_data=f"top:{kind}:{page + 1}"))
    if nav:
        kb.append(nav)
    kb.append([Btn(text="🏠 Меню", callback_data="menu")])
    if rows:
        text = f"{title}: последние {n} постов, стр. {page + 1}"
    else:
        text = f"{title}: данных нет. Вернись в меню и нажми «Обновить данные»."
    return text, InlineKeyboardMarkup(inline_keyboard=kb)


def user_view(kind, uid):
    n = state["n"]
    name = db("SELECT name FROM users WHERE id=%s", (uid,), True)[0][0]
    head = f"{name}\nhttps://vk.com/id{uid}\n"
    if kind == "l":
        rows = db(f"""SELECT p.id, p.date FROM likes l JOIN posts p ON p.id = l.post_id
                      WHERE l.user_id=%s AND p.id IN {RANGE_SQL} ORDER BY p.date DESC""",
                  (uid, n), True)
        lines = [head + f"❤️ Лайкнул постов: {len(rows)}", ""]
        for pid, ts in rows[:40]:
            lines.append(f"{fmt(ts)} · https://vk.com/wall{OWNER}_{pid}")
        if len(rows) > 40:
            lines.append(f"…и ещё {len(rows) - 40}")
    else:
        rows = db(f"""SELECT post_id, id, date, text FROM comments
                      WHERE user_id=%s AND post_id IN {RANGE_SQL} ORDER BY date DESC""",
                  (uid, n), True)
        lines = [head + f"💬 Комментариев: {len(rows)}", ""]
        for pid, cid, ts, text in rows[:20]:
            line = f"{fmt(ts)} · https://vk.com/wall{OWNER}_{pid}?reply={cid}"
            snippet = " ".join((text or "").split())[:80]
            if snippet:
                line += f"\n«{snippet}»"
            lines.append(line)
        if len(rows) > 20:
            lines.append(f"…и ещё {len(rows) - 20}")
    return "\n".join(lines)[:4000]


# ---------- обработчики ----------
async def show(c: CallbackQuery, text, kb):
    try:
        await c.message.edit_text(text, reply_markup=kb, link_preview_options=NO_PREVIEW)
    except TelegramBadRequest:
        pass


@dp.message(Command("start", "menu"))
async def cmd_start(m: Message):
    await m.answer(await asyncio.to_thread(menu_text), reply_markup=menu_kb())


@dp.callback_query(F.data == "menu")
async def cb_menu(c: CallbackQuery):
    await show(c, await asyncio.to_thread(menu_text), menu_kb())
    await c.answer()


@dp.callback_query(F.data.startswith("n:"))
async def cb_range(c: CallbackQuery):
    state["n"] = int(c.data[2:])
    await show(c, await asyncio.to_thread(menu_text), menu_kb())
    await c.answer()


@dp.callback_query(F.data == "upd")
async def cb_update(c: CallbackQuery):
    if state["busy"]:
        await c.answer("Сбор уже идёт, подожди", show_alert=True)
        return
    state["busy"] = True
    await c.answer()
    await show(c, f"⏳ Собираю последние {state['n']} постов…", None)
    t = asyncio.create_task(run_update(c.message.chat.id, c.message.message_id, state["n"]))
    tasks.add(t)
    t.add_done_callback(tasks.discard)


@dp.callback_query(F.data.startswith("top:"))
async def cb_top(c: CallbackQuery):
    _, kind, page = c.data.split(":")
    text, kb = await asyncio.to_thread(top_view, kind, int(page))
    await show(c, text, kb)
    await c.answer()


@dp.callback_query(F.data.startswith("u:"))
async def cb_user(c: CallbackQuery):
    _, kind, uid = c.data.split(":")
    text = await asyncio.to_thread(user_view, kind, int(uid))
    await c.message.answer(text, link_preview_options=NO_PREVIEW)
    await c.answer()


# ---------- веб-сервер и webhook ----------
async def on_startup(bot: Bot):
    await bot.set_webhook(f"{BASE_URL}/webhook", secret_token=WEBHOOK_SECRET)


def main():
    init_db()
    dp.startup.register(on_startup)
    app = web.Application()
    app.router.add_get("/health", lambda r: web.Response(text="ok"))
    SimpleRequestHandler(dp, bot, secret_token=WEBHOOK_SECRET).register(app, path="/webhook")
    setup_application(app, dp, bot=bot)
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
