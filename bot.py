"""
Telegram-бот: статистика лайков постов публичной группы ВК. Версия для Render (бесплатно).

Отличия от локальной версии:
  - работает через webhook (Telegram сам будит сервис, когда он «спит»);
  - данные хранятся во внешней Postgres-базе (DATABASE_URL), а не в файле,
    потому что файлы на бесплатном Render стираются.

Переменные окружения: TG_TOKEN, VK_TOKEN, VK_GROUP_ID, ADMIN_ID,
  DATABASE_URL, WEBHOOK_SECRET (любая случайная строка из букв/цифр).
RENDER_EXTERNAL_URL и PORT Render задаёт сам.
"""
import asyncio
import os
import time

import aiohttp
import psycopg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, LinkPreviewOptions, Message)
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
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)

bot = Bot(TG_TOKEN)
dp = Dispatcher()
tasks = set()  # чтобы фоновые задачи не удалялись сборщиком мусора


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


def save_post(pid, date, users):
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("INSERT INTO posts VALUES(%s,%s) ON CONFLICT DO NOTHING", (pid, date))
        for uid, name in users:
            conn.execute("""INSERT INTO users VALUES(%s,%s)
                            ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name""", (uid, name))
            conn.execute("INSERT INTO likes VALUES(%s,%s) ON CONFLICT DO NOTHING", (uid, pid))


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


async def collect():
    async with aiohttp.ClientSession() as s:
        offset = 0
        while True:
            wall = await vk(s, "wall.get", owner_id=OWNER, count=100, offset=offset)
            for post in wall["items"]:
                users, lo = [], 0
                while True:
                    res = await vk(s, "likes.getList", type="post", owner_id=OWNER,
                                   item_id=post["id"], extended=1, count=1000, offset=lo)
                    for u in res["items"]:
                        if u["id"] > 0:  # пропускаем сообщества
                            name = f'{u.get("first_name", "")} {u.get("last_name", "")}'.strip()
                            users.append((u["id"], name))
                    lo += 1000
                    if lo >= res["count"]:
                        break
                await asyncio.to_thread(save_post, post["id"], post["date"], users)
            offset += 100
            if offset >= wall["count"]:
                break


async def keepalive():
    """Пока идёт сбор, раз в 10 минут стучимся на свой адрес, чтобы Render не усыпил сервис."""
    async with aiohttp.ClientSession() as s:
        while True:
            await asyncio.sleep(600)
            try:
                await s.get(BASE_URL + "/health")
            except Exception:
                pass


async def run_update(m: Message):
    ka = asyncio.create_task(keepalive())
    t = time.time()
    try:
        await collect()
        await m.answer(f"Готово за {int(time.time() - t)} сек. Жми /top")
    except Exception as e:
        await m.answer(f"Ошибка при сборе: {e}\nМожно запустить /update ещё раз, прогресс сохранён.")
    finally:
        ka.cancel()


# ---------- Telegram ----------
def top_view(page):
    rows = db("""SELECT u.id, u.name, COUNT(*) c FROM likes l
                 JOIN users u ON u.id = l.user_id
                 GROUP BY u.id, u.name ORDER BY c DESC, u.id LIMIT %s OFFSET %s""",
              (PAGE, page * PAGE), fetch=True)
    kb = [[InlineKeyboardButton(text=f"{n} — {c}", url=f"https://vk.com/id{i}"),
           InlineKeyboardButton(text="📋", callback_data=f"u:{i}")]
          for i, n, c in rows]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"p:{page - 1}"))
    if len(rows) == PAGE:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"p:{page + 1}"))
    if nav:
        kb.append(nav)
    total = db("SELECT COUNT(*) FROM posts", fetch=True)[0][0]
    return f"Рейтинг по лайкам (постов в базе: {total}), стр. {page + 1}", \
        InlineKeyboardMarkup(inline_keyboard=kb)


@dp.message(Command("update"), F.from_user.id == ADMIN_ID)
async def cmd_update(m: Message):
    await m.answer("Собираю данные в фоне, это может занять несколько минут…")
    t = asyncio.create_task(run_update(m))
    tasks.add(t)
    t.add_done_callback(tasks.discard)


@dp.message(Command("top"), F.from_user.id == ADMIN_ID)
async def cmd_top(m: Message):
    text, kb = await asyncio.to_thread(top_view, 0)
    await m.answer(text, reply_markup=kb)


@dp.callback_query(F.data.startswith("p:"), F.from_user.id == ADMIN_ID)
async def cb_page(c: CallbackQuery):
    text, kb = await asyncio.to_thread(top_view, int(c.data[2:]))
    await c.message.edit_text(text, reply_markup=kb)
    await c.answer()


@dp.callback_query(F.data.startswith("u:"), F.from_user.id == ADMIN_ID)
async def cb_user(c: CallbackQuery):
    uid = int(c.data[2:])
    name = db("SELECT name FROM users WHERE id=%s", (uid,), fetch=True)[0][0]
    posts = db("""SELECT p.id, p.date FROM likes l JOIN posts p ON p.id = l.post_id
                  WHERE l.user_id=%s ORDER BY p.date DESC""", (uid,), fetch=True)
    lines = [f"{name}: лайкнул {len(posts)} постов", f"https://vk.com/id{uid}", ""]
    for pid, ts in posts[:40]:  # лимит длины сообщения Telegram
        d = time.strftime("%d.%m.%Y", time.gmtime(ts))
        lines.append(f"{d} · https://vk.com/wall{OWNER}_{pid}")
    if len(posts) > 40:
        lines.append(f"…и ещё {len(posts) - 40}")
    await c.message.answer("\n".join(lines), link_preview_options=NO_PREVIEW)
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
