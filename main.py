import asyncio
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import aiosqlite
import uvicorn
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (BufferedInputFile, CallbackQuery, InlineKeyboardButton, LinkPreviewOptions,
                           MenuButtonWebApp, Message, MessageReactionUpdated,
                           ReactionTypeEmoji, WebAppInfo)
from aiogram.utils.keyboard import InlineKeyboardBuilder

import store
from config import ADMINS, BOT_TOKEN, DB_PATH, DEDUP_SQL, MINI_APP_URL, PORT
import miniapp.server as miniserver
from miniapp.server import app as web_app

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("selinbot")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
TEHRAN = timezone(timedelta(hours=3, minutes=30))
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)


class OwnerStates(StatesGroup):
    replying_to_user = State()
    waiting_for_search_id = State()
    waiting_for_new_text = State()
    editing_key = State()


DEFAULT_TEXTS = {
    "start_owner": "🖤 *سلام مالک عزیز، به پنل مدیریت ربات سلین خوش آمدید!*\n\n🔗 لینک پیام ناشناس شما:\n`{link}`\n\nمردم با کلیک روی این لینک می‌توانند به شما پیام ناشناس بدهند.",
    "start_user": "سلام! 🌸\nشما در حال ارسال پیام ناشناس هستید.\nهر متن، عکس، یا ویسی که اینجا بفرستید، مستقیماً و به صورت ناشناس ارسال می‌شود. بفرمایید:",
    "user_blocked": "🔴 شما از ارسال پیام به این ربات مسدود شده‌اید.",
    "chat_paused": "🟡 چت توسط مالک موقتاً متوقف شده است. پیام شما ارسال نشد.",
    "sent_ok": "✅ پیام شما با موفقیت ارسال شد.",
    "media_blocked": "⚠️ در حال حاضر فقط پیام متنی پذیرفته می‌شود.",
    "hidden_notice": "📩 شما یک پیام جدید از طرف مالک دارید.\n\nبرای مشاهده متن پیام، روی دکمه زیر کلیک کنید:",
}


def md(s) -> str:
    """escape برای parse_mode=Markdown (نسخه قدیمی تلگرام): اسم/یوزرنیم با _ یا * پیام رو خراب می‌کرد."""
    return "".join("\\" + c if c in "_*`[" else c for c in str(s or ""))


def conn():
    return aiosqlite.connect(DB_PATH, timeout=30)


# ==========================================
# دیتابیس
# ==========================================
async def init_db():
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    async with conn() as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, first_name TEXT, username TEXT,
            is_blocked INTEGER DEFAULT 0, is_favorite INTEGER DEFAULT 0,
            chat_status TEXT DEFAULT 'active', anon_no INTEGER)''')
        await db.execute('''CREATE TABLE IF NOT EXISTS settings (
            owner_id INTEGER PRIMARY KEY, seen_receipts INTEGER DEFAULT 1,
            edit_tracking INTEGER DEFAULT 1, hidden_replies INTEGER DEFAULT 1)''')
        await db.execute('''CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_msg_id INTEGER, owner_msg_id INTEGER,
            user_id INTEGER, msg_text TEXT, direction TEXT DEFAULT 'in',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, admin_id INTEGER,
            media_type TEXT, file_id TEXT, file_name TEXT, mime TEXT)''')
        await db.execute('''CREATE TABLE IF NOT EXISTS bot_texts (key TEXT PRIMARY KEY, value TEXT)''')

        # مایگریشن دیتابیس‌های قدیمی.
        # باگ قبلی: ALTER با DEFAULT CURRENT_TIMESTAMP روی جدولی که ردیف داره خطا می‌ده
        # (SQLite اجازه نمی‌ده) و خطا هم با except خالی قورت داده می‌شد.
        for ddl in ("ALTER TABLE users ADD COLUMN chat_status TEXT DEFAULT 'active'",
                    "ALTER TABLE messages ADD COLUMN msg_text TEXT",
                    "ALTER TABLE messages ADD COLUMN direction TEXT DEFAULT 'in'",
                    "ALTER TABLE messages ADD COLUMN created_at TIMESTAMP",
                    "ALTER TABLE messages ADD COLUMN admin_id INTEGER",
                    "ALTER TABLE messages ADD COLUMN media_type TEXT",
                    "ALTER TABLE messages ADD COLUMN file_id TEXT",
                    "ALTER TABLE messages ADD COLUMN file_name TEXT",
                    "ALTER TABLE messages ADD COLUMN mime TEXT",
                    "ALTER TABLE users ADD COLUMN anon_no INTEGER",
                    "ALTER TABLE settings ADD COLUMN hidden_replies INTEGER DEFAULT 1"):
            try:
                await db.execute(ddl)
            except sqlite3.OperationalError:
                pass  # ستون از قبل هست
        await db.execute("UPDATE messages SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL")
        # شماره‌ی ناشناس برای هر کاربر (برای حالت store.get("show_sender")=0)
        missing = await (await db.execute("SELECT user_id FROM users WHERE anon_no IS NULL ORDER BY user_id")).fetchall()
        nxt = (await (await db.execute("SELECT COALESCE(MAX(anon_no), 0) FROM users")).fetchone())[0]
        for (uid,) in missing:
            nxt += 1
            await db.execute("UPDATE users SET anon_no = ? WHERE user_id = ?", (nxt, uid))

        for k, v in DEFAULT_TEXTS.items():
            await db.execute("INSERT OR IGNORE INTO bot_texts (key, value) VALUES (?, ?)", (k, v))
        await db.commit()
    await store.load()


async def get_dynamic_text(key):
    async with conn() as db:
        row = await (await db.execute("SELECT value FROM bot_texts WHERE key = ?", (key,))).fetchone()
    return row[0] if row else DEFAULT_TEXTS.get(key, "متن یافت نشد.")


async def add_user(user_id, first_name, username):
    async with conn() as db:
        await db.execute('''INSERT INTO users (user_id, first_name, username, anon_no)
            VALUES (?, ?, ?, (SELECT COALESCE(MAX(anon_no), 0) + 1 FROM users))
            ON CONFLICT(user_id) DO UPDATE SET first_name = excluded.first_name, username = excluded.username''',
                         (user_id, first_name, username))
        await db.commit()


async def log_message(user_msg_id, owner_msg_id, user_id, text, direction, admin_id, media=None):
    """media = (media_type, file_id, file_name, mime) یا None. شناسه‌ی ردیف رو برمی‌گردونه."""
    mt, fid, fname, mime = media or (None, None, None, None)
    async with conn() as db:
        cur = await db.execute(
            "INSERT INTO messages (user_msg_id, owner_msg_id, user_id, msg_text, direction, admin_id, created_at, "
            "media_type, file_id, file_name, mime) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)",
            (user_msg_id, owner_msg_id, user_id, text, direction, admin_id, mt, fid, fname, mime))
        await db.commit()
        return cur.lastrowid


# ---------- رسانه
MEDIA_LABEL = miniserver.MEDIA_LABEL


def extract_media(m: Message):
    """(media_type, file_id, file_name, mime) یا None"""
    if m.photo:
        return ("photo", m.photo[-1].file_id, None, "image/jpeg")
    if m.video:
        return ("video", m.video.file_id, m.video.file_name, m.video.mime_type or "video/mp4")
    if m.animation:  # قبل از document چک بشه، چون انیمیشن document هم داره
        return ("animation", m.animation.file_id, m.animation.file_name, m.animation.mime_type or "video/mp4")
    if m.voice:
        return ("voice", m.voice.file_id, None, m.voice.mime_type or "audio/ogg")
    if m.audio:
        return ("audio", m.audio.file_id, m.audio.file_name, m.audio.mime_type or "audio/mpeg")
    if m.video_note:
        return ("video_note", m.video_note.file_id, None, "video/mp4")
    if m.document:
        return ("document", m.document.file_id, m.document.file_name, m.document.mime_type or "application/octet-stream")
    if m.sticker:
        st = m.sticker
        kind = "sticker_anim" if st.is_animated else ("sticker_video" if st.is_video else "sticker")
        return (kind, st.file_id, None, "video/webm" if st.is_video else "image/webp")
    return None


def message_payload(m: Message):
    media = extract_media(m)
    text = m.text or m.caption or ""
    if not text and m.sticker:
        text = m.sticker.emoji or ""
    if not text and not media:
        text = "[محتوای پشتیبانی‌نشده]"
    return text, media


# (نام متد، نام آرگومان، آیا caption داره)
_SEND = {
    "photo": ("send_photo", "photo", True), "video": ("send_video", "video", True),
    "animation": ("send_animation", "animation", True), "voice": ("send_voice", "voice", True),
    "audio": ("send_audio", "audio", True), "video_note": ("send_video_note", "video_note", False),
    "document": ("send_document", "document", True), "sticker": ("send_sticker", "sticker", False),
    "sticker_video": ("send_sticker", "sticker", False), "sticker_anim": ("send_sticker", "sticker", False),
}


async def send_stored(chat_id, media_type, file, text, reply_to=None, markup=None, silent=False):
    """ارسال متن یا رسانه (file = file_id یا BufferedInputFile) بدون parse_mode."""
    kw = dict(reply_to_message_id=reply_to, allow_sending_without_reply=True,
              reply_markup=markup, disable_notification=silent)
    if not media_type:
        return await bot.send_message(chat_id, text, **kw)
    method, arg, has_caption = _SEND[media_type]
    if has_caption and text:
        kw["caption"] = text
    return await getattr(bot, method)(chat_id, **{arg: file}, **kw)


def pick_type(ctype: str, size: int) -> str:
    ctype = (ctype or "").lower()
    if ctype == "image/gif":
        return "animation"
    if ctype in ("image/jpeg", "image/png", "image/webp"):
        return "photo" if size <= 10 * 1024 * 1024 else "document"
    if ctype.startswith("video/"):
        return "video"
    if ctype.startswith("audio/"):
        return "audio"
    return "document"


async def alias(user_id, first_name=None) -> str:
    """نام قابل نمایش کاربر؛ در حالت ناشناس فقط «ناشناس N»."""
    if store.get("show_sender"):
        return first_name or "نامشخص"
    async with conn() as db:
        row = await (await db.execute("SELECT anon_no FROM users WHERE user_id = ?", (user_id,))).fetchone()
    return f"ناشناس {row[0] if row else '?'}"


def get_settings():
    """(رسید بازدید، ردیابی ویرایش، پیام مخفی) — از قوانینی که مدیر تعیین کرده."""
    return store.get("seen_receipts"), store.get("edit_tracking"), store.get("hidden_replies")


# ==========================================
# محافظ دکمه‌های ادمین (دفاع در عمق)
# ==========================================
class AdminCallbackGuard(BaseMiddleware):
    PUBLIC = ("reveal_", "seeno_")  # فقط این دو دکمه به کاربرهای عادی داده می‌شه

    async def __call__(self, handler, event: CallbackQuery, data):
        if event.from_user.id not in ADMINS and not (event.data or "").startswith(self.PUBLIC):
            await event.answer("⛔️", show_alert=True)
            return
        return await handler(event, data)


dp.callback_query.outer_middleware(AdminCallbackGuard())


# ==========================================
# استارت و راهنما
# ==========================================
@dp.message(Command("help"), F.from_user.id.in_(ADMINS))
async def cmd_help(message: Message):
    await message.answer(
        "📚 *راهنمای جامع پیام‌رسان سلین*\n\n"
        "🔹 *پنل مدیریت:* با ارسال `/start` باز می‌شود.\n"
        "🔹 *حذف دوطرفه:* ریپلای روی پیام و ارسال `/del`.\n"
        "🔹 *لغو عملیات:* ارسال `/cancel` در هر زمان.\n"
        "🔹 *پیام مخفی:* در صورت روشن بودن در تنظیمات، کاربر ابتدا یک اعلان دریافت می‌کند و با زدن دکمه، پیام اصلی را می‌بیند و رسید سین برای شما ارسال می‌شود.\n"
        "🔹 *ریپلای مستقیم:* می‌توانید روی پیام کاربر سوایپ کنید و مستقیماً جواب دهید تا برای او ارسال شود.",
        parse_mode="Markdown")


async def owner_home_content():
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=Selin"
    text = (await get_dynamic_text("start_owner")).replace("{link}", link)
    b = InlineKeyboardBuilder()
    b.button(text="⚙️ قوانین ربات", callback_data="settings_menu")
    b.button(text="👥 مدیریت کاربران", callback_data="users_menu")
    b.button(text="📚 راهنما", callback_data="help_btn")
    if MINI_APP_URL:
        b.button(text="🗂 مینی‌اپ تاریخچه چت", web_app=WebAppInfo(url=MINI_APP_URL))
    else:
        text += "\n\n⚠️ مینی‌اپ هنوز فعال نیست: در Railway بخش Settings → Networking → Generate Domain را بزنید."
    b.adjust(2, 1, 1)
    return text, b.as_markup()


@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    user_id = message.from_user.id
    await add_user(user_id, message.from_user.first_name or "نامشخص", message.from_user.username)

    if user_id in ADMINS:
        text, markup = await owner_home_content()
        try:
            await message.answer(text, reply_markup=markup, parse_mode="Markdown", link_preview_options=NO_PREVIEW)
        except Exception:  # اگه مالک متن استارت رو با مارک‌داون خراب ذخیره کرده باشه
            await message.answer(text, reply_markup=markup, link_preview_options=NO_PREVIEW)
    elif command.args == "Selin":
        await message.answer(await get_dynamic_text("start_user"))
    else:
        await message.answer("سلام! برای ارسال پیام ناشناس باید از لینک اختصاصی استفاده کنید.")


@dp.callback_query(F.data == "help_btn")
async def process_help_btn(callback: CallbackQuery):
    await cmd_help(callback.message)
    await callback.answer()


# ==========================================
# دریافت پیام از کاربر
# ==========================================
@dp.message(~F.from_user.id.in_(ADMINS), F.chat.type == "private", F.text != "/start")
async def receive_message_from_user(message: Message, bot: Bot):
    user_id = message.from_user.id

    async with conn() as db:
        row = await (await db.execute(
            "SELECT is_blocked, chat_status FROM users WHERE user_id = ?", (user_id,))).fetchone()
        if row and row[0] == 1:
            await message.answer(await get_dynamic_text("user_blocked"))
            return
        if row and row[1] == 'paused':
            await message.answer(await get_dynamic_text("chat_paused"))
            return

        # اگه کاربر ریپلای زده، برای هر ادمین پیام متناظرِ خودش رو پیدا کن
        # (آیدی پیام‌ها بین چت‌ها یکی نیست، پس باید به‌ازای هر ادمین جدا باشه)
        reply_map = {}
        if message.reply_to_message:
            rows = await (await db.execute(
                "SELECT owner_msg_id, admin_id FROM messages WHERE user_msg_id = ? AND user_id = ?",
                (message.reply_to_message.message_id, user_id))).fetchall()
            for owner_msg_id, adm in rows:
                if owner_msg_id:  # پیام‌های ارسالی از مینی‌اپ پیام متناظر توی چت ادمین ندارن
                    reply_map[adm or ADMINS[0]] = owner_msg_id

    text_content, media = message_payload(message)
    if media and not store.get("allow_media"):
        await message.answer(await get_dynamic_text("media_blocked"))
        return

    first_name = message.from_user.first_name or "نامشخص"
    username = message.from_user.username
    profile_link = f"https://t.me/{username}" if username else f"tg://openmessage?user_id={user_id}"
    username_text = f"[@{md(username)}](https://t.me/{username})" if username else "ندارد"
    info_text = (
        f"👤 *فرستنده:* {md(first_name)}\n"
        f"🆔 *آیدی کاربری:* `{user_id}`\n"
        f"🔗 *یوزرنیم:* {username_text}\n"
        f"🌐 *لینک پروفایل:* [باز کردن پیوی]({profile_link})"
    ) if store.get("show_sender") else "📩 *پیام ناشناس جدید*"
    seen_enabled = store.get("seen_receipts")

    delivered = 0
    for admin in ADMINS:
        try:
            copied = await bot.copy_message(
                chat_id=admin, from_chat_id=message.chat.id, message_id=message.message_id,
                reply_to_message_id=reply_map.get(admin), allow_sending_without_reply=True)
        except Exception as e:
            log.warning("copy to admin %s failed: %s", admin, e)
            continue
        delivered += 1
        # اول ذخیره می‌کنیم؛ قبلاً اگه پیام اطلاعات فرستنده ارسال نمی‌شد، پیام هیچ‌وقت ثبت نمی‌شد
        await log_message(message.message_id, copied.message_id, user_id, text_content, "in", admin, media)

        b = InlineKeyboardBuilder()
        if seen_enabled:
            b.button(text="👁 خواندم", callback_data=f"seenu_{user_id}_{message.message_id}_{copied.message_id}")
        b.button(text="📝 پاسخ سریع", callback_data=f"reply_{user_id}")
        if store.get("show_sender"):
            b.button(text="🖤 مدیریت کاربر", callback_data=f"usermenu_{user_id}")
            b.adjust(1, 2)
        else:
            b.adjust(1)
        try:
            await bot.send_message(admin, info_text, reply_to_message_id=copied.message_id,
                                   reply_markup=b.as_markup(), parse_mode="Markdown")
        except Exception as e:
            log.warning("info message to admin %s failed: %s", admin, e)

    if delivered:
        if store.get("send_confirm"):
            await message.reply(await get_dynamic_text("sent_ok"))
    else:
        await message.reply("❌ ارسال پیام ناموفق بود، کمی بعد دوباره تلاش کنید.")


@dp.callback_query(F.data.startswith("seenu_"))
async def mark_seen_by_owner(callback: CallbackQuery, bot: Bot):
    _, user_id, user_msg_id, _owner = callback.data.split("_")
    b = InlineKeyboardBuilder()
    b.button(text="📝 پاسخ سریع", callback_data=f"reply_{user_id}")
    if store.get("show_sender"):
        b.button(text="🖤 مدیریت کاربر", callback_data=f"usermenu_{user_id}")
    b.adjust(2)
    try:
        await callback.message.edit_reply_markup(reply_markup=b.as_markup())
        await bot.send_message(int(user_id), "👁 مالک پیام شما را دید.",
                               reply_to_message_id=int(user_msg_id), allow_sending_without_reply=True)
        await callback.answer("✅ رسید بازدید برای کاربر ارسال شد.")
    except Exception:
        await callback.answer("⚠️ خطا در ارسال رسید (شاید کاربر ربات را بلاک کرده).")


# ==========================================
# پاسخ مدیر (منطق مشترک برای دکمه و ریپلای مستقیم)
# ==========================================
async def deliver_reply(message: Message, bot: Bot, target_user_id: int, target_msg_id):
    admin_id = message.from_user.id
    text, media = message_payload(message)
    seen_enabled, _edit, hidden_mode = get_settings()

    if hidden_mode:
        # اول ثبت می‌کنیم تا شناسه‌ی ردیف توی دکمه بیاد؛ user_msg_id موقع باز شدن پر می‌شه
        row_id = await log_message(None, message.message_id, target_user_id, text, "out", admin_id, media)
        b = InlineKeyboardBuilder()
        b.button(text="👁 مشاهده پیام", callback_data=f"reveal_{row_id}")
        try:
            await bot.send_message(
                target_user_id,
                await get_dynamic_text("hidden_notice"),
                reply_markup=b.as_markup(),
                reply_to_message_id=target_msg_id, allow_sending_without_reply=True)
        except Exception:
            async with conn() as db:
                await db.execute("DELETE FROM messages WHERE id = ?", (row_id,))
                await db.commit()
            raise
        return "✅ اعلان پیام ارسال شد."

    b = InlineKeyboardBuilder()
    if seen_enabled:
        b.button(text="👁 خواندم", callback_data=f"seeno_{admin_id}_{message.message_id}")
    sent = await bot.copy_message(
        chat_id=target_user_id, from_chat_id=message.chat.id, message_id=message.message_id,
        reply_markup=b.as_markup() if seen_enabled else None,
        reply_to_message_id=target_msg_id, allow_sending_without_reply=True)
    await log_message(sent.message_id, message.message_id, target_user_id, text, "out", admin_id, media)
    return "✅ پیام شما مستقیماً ارسال شد."


@dp.callback_query(F.data.startswith("reply_"))
async def process_reply_button(callback: CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split("_")[1])
    target_msg_id = None
    if callback.message.reply_to_message:
        async with conn() as db:
            row = await (await db.execute(
                "SELECT user_msg_id FROM messages WHERE owner_msg_id = ? AND user_id = ? "
                "AND (admin_id = ? OR admin_id IS NULL)",
                (callback.message.reply_to_message.message_id, user_id, callback.from_user.id))).fetchone()
        if row:
            target_msg_id = row[0]
    await state.update_data(target_user_id=user_id, target_msg_id=target_msg_id)
    await state.set_state(OwnerStates.replying_to_user)
    await callback.message.reply("✏️ پیام خود را بنویسید تا ارسال کنم. (لغو: `/cancel`)", parse_mode="Markdown")
    await callback.answer()


@dp.message(F.text == "/cancel", F.from_user.id.in_(ADMINS))
async def cancel_states(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ عملیات جاری لغو شد.")


@dp.message(OwnerStates.replying_to_user, F.from_user.id.in_(ADMINS))
async def send_reply_to_user(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    try:
        await message.reply(await deliver_reply(message, bot, data["target_user_id"], data.get("target_msg_id")))
    except Exception as e:
        await message.reply(f"❌ خطایی رخ داد: {e}")
    await state.clear()


@dp.callback_query(F.data.startswith("reveal_"))
async def reveal_hidden_message(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split("_")
    user_id = callback.from_user.id
    reply_id = callback.message.reply_to_message.message_id if callback.message.reply_to_message else None
    try:
        if len(parts) == 2:  # فرمت جدید: reveal_{row_id}
            async with conn() as db:
                row = await (await db.execute(
                    "SELECT user_id, owner_msg_id, admin_id, media_type, file_id, msg_text, user_msg_id "
                    "FROM messages WHERE id = ?", (int(parts[1]),))).fetchone()
            if not row or row[0] != user_id:
                await callback.answer("این پیام برای شما نیست.", show_alert=True)
                return
            _, owner_msg_id, admin_id, media_type, file_id, text, already = row
            if already:
                await callback.answer("این پیام قبلاً باز شده.", show_alert=True)
                await callback.message.delete()
                return
            sent = None
            if owner_msg_id and admin_id:  # پیامی که از خود تلگرام نوشته شده: کپی با حفظ فرمت
                try:
                    sent = await bot.copy_message(user_id, admin_id, owner_msg_id,
                                                  reply_to_message_id=reply_id, allow_sending_without_reply=True)
                except Exception:
                    sent = None
            if sent is None:  # پیام ارسالی از مینی‌اپ (یا کپی ناموفق): از روی دیتابیس بفرست
                sent = await send_stored(user_id, media_type, file_id, text, reply_id)
            async with conn() as db:
                await db.execute("UPDATE messages SET user_msg_id = ? WHERE id = ?", (sent.message_id, int(parts[1])))
                await db.commit()
            notify_admin, notify_reply = admin_id, owner_msg_id
        else:  # فرمت قدیمی: reveal_{admin}_{msg}_{user}
            _, admin_id, admin_msg_id, target_user = parts
            if user_id != int(target_user):
                await callback.answer("این پیام برای شما نیست.", show_alert=True)
                return
            sent = await bot.copy_message(user_id, int(admin_id), int(admin_msg_id),
                                          reply_to_message_id=reply_id, allow_sending_without_reply=True)
            async with conn() as db:
                cur = await db.execute(
                    "UPDATE messages SET user_msg_id = ? WHERE owner_msg_id = ? AND admin_id = ? "
                    "AND user_id = ? AND direction = 'out' AND user_msg_id IS NULL",
                    (sent.message_id, int(admin_msg_id), int(admin_id), user_id))
                await db.commit()
            if cur.rowcount == 0:
                await log_message(sent.message_id, int(admin_msg_id), user_id, "پیام مخفی باز شد", "out", int(admin_id))
            notify_admin, notify_reply = int(admin_id), int(admin_msg_id)

        await callback.message.delete()
        if notify_admin:
            who = await alias(user_id, callback.from_user.first_name)
            await bot.send_message(notify_admin, f"👀 {who} پیام شما را باز کرد و دید.",
                                   reply_to_message_id=notify_reply or None, allow_sending_without_reply=True)
    except Exception as e:
        log.info("reveal failed: %s", e)
        await callback.answer("⚠️ پیام در دسترس نیست.", show_alert=True)
        try:
            await callback.message.delete()
        except Exception:
            pass


@dp.callback_query(F.data.startswith("seeno_"))
async def mark_seen_by_user(callback: CallbackQuery, bot: Bot):
    _, admin_id, admin_msg_id = callback.data.split("_")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
        who = await alias(callback.from_user.id, callback.from_user.first_name)
        await bot.send_message(int(admin_id), f"👁 {who} پاسخ شما را خواند.",
                               reply_to_message_id=int(admin_msg_id) or None, allow_sending_without_reply=True)
        await callback.answer("✅ رسید بازدید ثبت شد.")
    except Exception:
        await callback.answer("⚠️ خطا در ثبت رسید.")


@dp.message(F.reply_to_message, F.from_user.id.in_(ADMINS), ~F.text.in_(["/del", "/cancel", "/start", "/help"]))
async def direct_admin_reply(message: Message, bot: Bot):
    admin_id = message.from_user.id
    candidates = [message.reply_to_message.message_id]
    # اگه روی پیامِ «اطلاعات فرستنده» ریپلای زد، پیامِ کپی‌شده‌ی بالاییش رو هم امتحان کن
    if message.reply_to_message.reply_to_message:
        candidates.append(message.reply_to_message.reply_to_message.message_id)
    row = None
    async with conn() as db:
        for mid in candidates:
            row = await (await db.execute(
                "SELECT user_msg_id, user_id FROM messages WHERE owner_msg_id = ? "
                "AND (admin_id = ? OR admin_id IS NULL)", (mid, admin_id))).fetchone()
            if row:
                break
    if not row:
        return
    target_msg_id, target_user_id = row
    try:
        await deliver_reply(message, bot, target_user_id, target_msg_id)
    except Exception as e:
        log.warning("direct reply failed: %s", e)
        await message.reply("❌ خطا در ارسال ریپلای مستقیم.")


# ==========================================
# ویرایش دوطرفه
# ==========================================
@dp.edited_message()
async def sync_edited_messages(message: Message, bot: Bot):
    if not store.get("edit_tracking"):
        return
    uid = message.from_user.id
    text = message.text or message.caption
    if text is None:
        return

    async with conn() as db:
        if uid in ADMINS:
            rows = await (await db.execute(
                "SELECT user_id, user_msg_id FROM messages WHERE owner_msg_id = ? AND direction = 'out' "
                "AND (admin_id = ? OR admin_id IS NULL) AND user_msg_id IS NOT NULL", (message.message_id, uid))).fetchall()
            await db.execute("UPDATE messages SET msg_text = ? WHERE owner_msg_id = ? AND direction = 'out' "
                             "AND (admin_id = ? OR admin_id IS NULL)", (text, message.message_id, uid))
            targets = [(r[0], r[1]) for r in rows]
        else:
            rows = await (await db.execute(
                "SELECT admin_id, owner_msg_id FROM messages WHERE user_msg_id = ? AND user_id = ? AND direction = 'in'",
                (message.message_id, uid))).fetchall()
            await db.execute("UPDATE messages SET msg_text = ? WHERE user_msg_id = ? AND user_id = ? AND direction = 'in'",
                             (text, message.message_id, uid))
            targets = [((a or ADMINS[0]), o) for a, o in rows]
        await db.commit()

    mark = f"\n\n(✏️ ویرایش شده در: {datetime.now(TEHRAN).strftime('%Y-%m-%d %H:%M')})"
    for chat_id, msg_id in targets:
        try:  # بدون parse_mode: متن کاربر ممکنه کاراکتر مارک‌داون داشته باشه و ویرایش بی‌صدا خراب می‌شد
            if message.text:
                await bot.edit_message_text(text + mark, chat_id=chat_id, message_id=msg_id)
            else:
                await bot.edit_message_caption(chat_id=chat_id, message_id=msg_id, caption=text + mark)
        except Exception as e:
            log.info("edit sync failed: %s", e)


async def delete_everywhere(row_id: int):
    """پیام رو از چت کاربر، چت هر دو ادمین و دیتابیس حذف می‌کنه.
    None = پیدا نشد؛ True/False = حذف سمت کاربر موفق بود یا نه."""
    async with conn() as db:
        row = await (await db.execute(
            "SELECT user_id, user_msg_id, direction, owner_msg_id, admin_id FROM messages WHERE id = ?",
            (row_id,))).fetchone()
        if not row:
            return None
        user_id, user_msg_id, direction, owner_msg_id, admin_id = row
        if user_msg_id is not None:
            group = await (await db.execute(
                "SELECT id, admin_id, owner_msg_id FROM messages WHERE user_id = ? AND user_msg_id = ? AND direction = ?",
                (user_id, user_msg_id, direction))).fetchall()
        else:
            group = [(row_id, admin_id, owner_msg_id)]
        ok = True
        if user_msg_id is not None:
            try:
                await bot.delete_message(user_id, user_msg_id)
            except Exception:
                ok = False
        for _rid, adm, omid in group:
            if omid:
                try:
                    await bot.delete_message(adm or ADMINS[0], omid)
                except Exception:
                    pass
        ids = [g[0] for g in group]
        await db.execute(f"DELETE FROM messages WHERE id IN ({','.join('?' * len(ids))})", ids)
        await db.commit()
    return ok


@dp.message(F.text == "/del", F.from_user.id.in_(ADMINS))
async def delete_both_sides(message: Message, bot: Bot):
    if not message.reply_to_message:
        await message.reply("⚠️ ریپلای روی پیام و ارسال `/del`.", parse_mode="Markdown")
        return
    admin_id = message.from_user.id
    async with conn() as db:
        row = await (await db.execute(
            "SELECT id FROM messages WHERE owner_msg_id = ? AND (admin_id = ? OR admin_id IS NULL)",
            (message.reply_to_message.message_id, admin_id))).fetchone()
    if not row:
        await message.reply("⚠️ این پیام در دیتابیس یافت نشد.")
        return
    ok = await delete_everywhere(row[0])
    try:
        await message.delete()
    except Exception:
        pass
    if not ok:
        await bot.send_message(admin_id, "⚠️ پیام سمت شما حذف شد ولی حذف از سمت کاربر ممکن نبود (بیش از ۴۸ ساعت یا قبلاً پاک شده).")


@dp.message_reaction()
async def sync_reactions(reaction: MessageReactionUpdated, bot: Bot):
    if not reaction.user:  # ری‌اکشن ناشناس/کانال؛ قبلاً اینجا کرش می‌کرد
        return
    uid, msg_id = reaction.user.id, reaction.message_id
    async with conn() as db:
        if uid in ADMINS:
            rows = await (await db.execute(
                "SELECT user_id, user_msg_id FROM messages WHERE owner_msg_id = ? AND (admin_id = ? OR admin_id IS NULL) "
                "AND user_msg_id IS NOT NULL", (msg_id, uid))).fetchall()
            targets = [(r[0], r[1]) for r in rows]
        else:
            rows = await (await db.execute(
                "SELECT admin_id, owner_msg_id FROM messages WHERE user_msg_id = ? AND user_id = ?",
                (msg_id, uid))).fetchall()
            targets = [((a or ADMINS[0]), o) for a, o in rows]
    new = [ReactionTypeEmoji(emoji=r.emoji) for r in reaction.new_reaction if r.type == "emoji"]
    for chat_id, mid in targets:
        try:
            await bot.set_message_reaction(chat_id=chat_id, message_id=mid, reaction=new)
        except Exception as e:
            log.info("reaction sync failed: %s", e)


# ==========================================
# مدیریت کاربران
# ==========================================
_PH = ",".join("?" * len(ADMINS))


@dp.callback_query(F.data == "users_menu")
async def show_users_menu(callback: CallbackQuery):
    async with conn() as db:
        total = (await (await db.execute(f"SELECT COUNT(*) FROM users WHERE user_id NOT IN ({_PH})", ADMINS)).fetchone())[0]
        blocked = (await (await db.execute("SELECT COUNT(*) FROM users WHERE is_blocked = 1")).fetchone())[0]
    b = InlineKeyboardBuilder()
    b.button(text="📋 لیست تمام چت‌ها", callback_data="users_list_0")
    if store.get("show_sender"):
        b.button(text="🔍 جستجو با آیدی", callback_data="search_user")
    b.adjust(1)
    try:
        await callback.message.edit_text(
            f"👥 *بخش مدیریت کاربران و چت‌ها*\n\n🔹 تعداد کل چت‌ها: `{total}`\n🚫 کاربران مسدود: `{blocked}`\n\n"
            "یکی از گزینه‌های زیر را انتخاب کنید:", reply_markup=b.as_markup(), parse_mode="Markdown")
    except Exception:
        await callback.answer()


@dp.callback_query(F.data.startswith("users_list_"))
async def show_users_list(callback: CallbackQuery):
    page = int(callback.data.split("_")[2])
    limit = 10
    async with conn() as db:
        total = (await (await db.execute(f"SELECT COUNT(*) FROM users WHERE user_id NOT IN ({_PH})", ADMINS)).fetchone())[0]
        users = await (await db.execute(
            f"SELECT user_id, first_name, is_blocked, is_favorite, chat_status, anon_no FROM users "
            f"WHERE user_id NOT IN ({_PH}) LIMIT ? OFFSET ?", (*ADMINS, limit, page * limit))).fetchall()
    b = InlineKeyboardBuilder()
    for u_id, name, blocked, fav, status, anon in users:
        name = name if store.get("show_sender") else f"ناشناس {anon}"
        icon = "🟡" if status == 'paused' else ("🔴" if blocked else ("🌟" if fav else "👤"))
        b.button(text=f"{icon} {name}", callback_data=f"usermenu_{u_id}")
    b.adjust(1)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ قبلی", callback_data=f"users_list_{page - 1}"))
    if (page + 1) * limit < total:
        nav.append(InlineKeyboardButton(text="بعدی ⬅️", callback_data=f"users_list_{page + 1}"))
    if nav:
        b.row(*nav)
    b.row(InlineKeyboardButton(text="🔙 بازگشت به منو", callback_data="users_menu"))
    try:
        await callback.message.edit_text(f"📋 *لیست چت‌ها* (صفحه {page + 1})\nبرای مدیریت روی نام کلیک کنید:",
                                         reply_markup=b.as_markup(), parse_mode="Markdown")
    except Exception:
        await callback.answer()


async def build_user_menu(user_id: int):
    async with conn() as db:
        user = await (await db.execute(
            "SELECT first_name, username, is_blocked, is_favorite, chat_status, anon_no FROM users WHERE user_id = ?",
            (user_id,))).fetchone()
    if not user:
        return None
    first_name, username, is_blocked, is_favorite, chat_status, anon_no = user
    if not store.get("show_sender"):
        first_name = f"ناشناس {anon_no}"
    profile_link = f"https://t.me/{username}" if username else f"tg://openmessage?user_id={user_id}"
    username_text = f"[@{md(username)}](https://t.me/{username})" if username else "ندارد"
    ident = (f"🆔 آیدی عددی: `{user_id}`\n🔗 یوزرنیم: {username_text}\n"
             f"🌐 *لینک پروفایل:* [باز کردن پیوی]({profile_link})\n\n") if store.get("show_sender") else "\n"
    text = (
        f"🖤 *پنل مدیریت کاربر: {md(first_name)}*\n"
        f"ــــــــــــــــــــــــــــــــــــــــ\n"
        f"{ident}"
        f"وضعیت مسدودی: {'بله 🔴' if is_blocked else 'خیر 🟢'}\n"
        f"وضعیت ویژه: {'بله 🌟' if is_favorite else 'خیر ⚪️'}\n"
        f"وضعیت چت: {'متوقف شده 🟡' if chat_status == 'paused' else 'فعال 🔵'}\n")
    b = InlineKeyboardBuilder()
    b.button(text="📝 ارسال پیام", callback_data=f"reply_{user_id}")
    b.button(text="🗂 تاریخچه پیام‌ها", callback_data=f"history_{user_id}")
    b.button(text="🔵 ادامه چت" if chat_status == 'paused' else "🟡 متوقف کردن چت", callback_data=f"pause_{user_id}")
    b.button(text="🟢 آنبلاک کردن" if is_blocked else "🔴 بلاک کردن", callback_data=f"block_{user_id}")
    b.button(text="⚪️ حذف از ویژه‌ها" if is_favorite else "🌟 افزودن به ویژه‌ها", callback_data=f"fav_{user_id}")
    b.button(text="🔙 بازگشت به لیست", callback_data="users_list_0")
    b.adjust(2, 1, 2, 1)
    return text, b.as_markup()


@dp.callback_query(F.data.startswith("usermenu_"))
async def specific_user_menu(callback: CallbackQuery):
    built = await build_user_menu(int(callback.data.split("_")[1]))
    if not built:
        await callback.answer("⚠️ کاربر یافت نشد!", show_alert=True)
        return
    try:
        await callback.message.edit_text(built[0], reply_markup=built[1], parse_mode="Markdown")
    except Exception:
        await callback.answer()


@dp.callback_query(F.data.startswith("history_"))
async def view_user_history(callback: CallbackQuery):
    user_id = int(callback.data.split("_")[1])
    async with conn() as db:
        rows = await (await db.execute(
            f"SELECT msg_text, direction, media_type FROM messages WHERE user_id = ? AND {DEDUP_SQL.format(a='')} "
            "ORDER BY id DESC LIMIT 5", (user_id,))).fetchall()
    if not rows:
        await callback.answer("هیچ تاریخچه‌ای ثبت نشده است.", show_alert=True)
        return
    out = "🗂 *۵ پیام آخر تبادل شده در این چت:*\n\n"
    for i, (t, d, mt) in enumerate(rows, 1):
        t = str(t or MEDIA_LABEL.get(mt, "") or "")
        out += f"{i}. {'📥' if d == 'in' else '📤'} {md(t[:50] + '...' if len(t) > 50 else t)}\n"
    await callback.message.reply(out, parse_mode="Markdown")
    await callback.answer()


async def _toggle(callback: CallbackQuery, sql_select: str, sql_update: str):
    user_id = int(callback.data.split("_")[1])
    async with conn() as db:
        row = await (await db.execute(sql_select, (user_id,))).fetchone()
        if row:
            await db.execute(sql_update, (row[0], user_id))
            await db.commit()
    await specific_user_menu(callback)


@dp.callback_query(F.data.startswith("pause_"))
async def toggle_pause_user(callback: CallbackQuery):
    await _toggle(callback, "SELECT chat_status FROM users WHERE user_id = ?",
                  "UPDATE users SET chat_status = CASE ? WHEN 'paused' THEN 'active' ELSE 'paused' END WHERE user_id = ?")


@dp.callback_query(F.data.startswith("block_"))
async def toggle_block_user(callback: CallbackQuery):
    await _toggle(callback, "SELECT is_blocked FROM users WHERE user_id = ?",
                  "UPDATE users SET is_blocked = 1 - ? WHERE user_id = ?")


@dp.callback_query(F.data.startswith("fav_"))
async def toggle_fav_user(callback: CallbackQuery):
    await _toggle(callback, "SELECT is_favorite FROM users WHERE user_id = ?",
                  "UPDATE users SET is_favorite = 1 - ? WHERE user_id = ?")


@dp.callback_query(F.data == "search_user")
async def prompt_search_user(callback: CallbackQuery, state: FSMContext):
    await state.set_state(OwnerStates.waiting_for_search_id)
    await callback.message.reply("🔍 آیدی عددی کاربر مورد نظر را بفرستید:\n(لغو: `/cancel`)", parse_mode="Markdown")
    await callback.answer()


@dp.message(OwnerStates.waiting_for_search_id, F.from_user.id.in_(ADMINS))
async def process_search_user(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.reply("⚠️ فقط اعداد مجاز است.")
        return
    built = await build_user_menu(int(text))
    if not built:
        await message.reply("❌ یافت نشد.")
        return
    # قبلاً با FakeCallback می‌خواست پیامِ خودِ ادمین رو ویرایش کنه که غیرممکنه (جستجو همیشه خطا می‌داد)
    await message.answer(built[0], reply_markup=built[1], parse_mode="Markdown")
    await state.clear()


# ==========================================
# تنظیمات
# ==========================================
def settings_markup():
    b = InlineKeyboardBuilder()
    for key, (_d, title, _desc) in store.RULES.items():
        b.button(text=f"{title}: {'روشن 🟢' if store.get(key) else 'خاموش 🔴'}", callback_data=f"tg_{key}")
    b.button(text="📝 ویرایش متون ربات", callback_data="edit_texts_menu")
    b.button(text="🔙 بازگشت", callback_data="owner_home")
    b.adjust(1)
    return b.as_markup()


SETTINGS_TEXT = ("🖤 *پنل قوانین ربات*\nــــــــــــــــــــــــــــــــــــــــ\n"
                 "هر قانون با یک کلیک روشن/خاموش می‌شود و همان لحظه اعمال می‌شود.\n\n"
                 + "\n".join(f"• {t}: {d}" for _k, (_x, t, d) in store.RULES.items()))


@dp.callback_query(F.data == "settings_menu")
async def show_settings_menu(callback: CallbackQuery):
    try:
        await callback.message.edit_text(SETTINGS_TEXT, reply_markup=settings_markup(), parse_mode="Markdown")
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data.startswith("tg_"))
async def toggle_settings(callback: CallbackQuery):
    key = callback.data[3:]
    if key not in store.RULES:
        await callback.answer()
        return
    await store.toggle(key)
    await show_settings_menu(callback)


@dp.callback_query(F.data == "owner_home")
async def owner_home(callback: CallbackQuery):
    text, markup = await owner_home_content()
    try:
        await callback.message.edit_text(text, reply_markup=markup, parse_mode="Markdown",
                                         link_preview_options=NO_PREVIEW)
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data == "edit_texts_menu")
async def show_edit_texts_menu(callback: CallbackQuery):
    b = InlineKeyboardBuilder()
    b.button(text="متن استارت مالک 👑", callback_data="edittxt_start_owner")
    b.button(text="متن استارت کاربر 👤", callback_data="edittxt_start_user")
    b.button(text="اخطار مسدود شدن 🚫", callback_data="edittxt_user_blocked")
    b.button(text="اخطار توقف چت ⏸", callback_data="edittxt_chat_paused")
    b.button(text="تأیید ارسال پیام ✅", callback_data="edittxt_sent_ok")
    b.button(text="اخطار رسانه ممنوع 🖼", callback_data="edittxt_media_blocked")
    b.button(text="اعلان پیام مخفی 📩", callback_data="edittxt_hidden_notice")
    b.button(text="🔙 بازگشت", callback_data="settings_menu")
    b.adjust(1)
    await callback.message.edit_text("📝 *کدام متن را می‌خواهید تغییر دهید؟*", reply_markup=b.as_markup(), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("edittxt_"))
async def prompt_edit_text(callback: CallbackQuery, state: FSMContext):
    key = callback.data.split("edittxt_", 1)[1]
    if key not in DEFAULT_TEXTS:
        await callback.answer()
        return
    current = await get_dynamic_text(key)
    await state.update_data(editing_key=key)
    await state.set_state(OwnerStates.waiting_for_new_text)
    guide = "\n\n💡 راهنما: از {link} برای نمایش لینک استفاده کنید." if key == "start_owner" else ""
    # بدون parse_mode: متن فعلی خودش بک‌تیک و * داره و قبلاً این پیام با خطای تلگرام می‌ترکید
    await callback.message.reply(f"متن فعلی شما:\n\n{current}\n\n✏️ متن جدید را ارسال کنید: (لغو: /cancel){guide}")
    await callback.answer()


@dp.message(OwnerStates.waiting_for_new_text, F.from_user.id.in_(ADMINS))
async def save_new_text(message: Message, state: FSMContext):
    if not message.text:
        await message.reply("⚠️ فقط متن بفرستید.")
        return
    key = (await state.get_data()).get("editing_key")
    async with conn() as db:
        await db.execute("UPDATE bot_texts SET value = ? WHERE key = ?", (message.text, key))
        await db.commit()
    await message.reply("🌟 *متن با موفقیت ذخیره شد!*", parse_mode="Markdown")
    await state.clear()


# ==========================================
# اجرا: ربات + مینی‌اپ در یک پروسس
# ==========================================
async def app_send(admin_id, user_id, text, upload, reply_row_id):
    """ارسال پیام از مینی‌اپ (همون منطق پیام مخفی / رسید سین). شناسه‌ی ردیف ثبت‌شده رو برمی‌گردونه."""
    text = (text or "").strip()
    target_msg_id = None
    async with conn() as db:
        if not await (await db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))).fetchone():
            raise ValueError("کاربر یافت نشد")
        if reply_row_id:
            r = await (await db.execute("SELECT user_msg_id FROM messages WHERE id = ? AND user_id = ?",
                                        (reply_row_id, user_id))).fetchone()
            if r:
                target_msg_id = r[0]
    seen, _edit, hidden = get_settings()

    seen_kb = None
    if seen and not hidden:
        kb = InlineKeyboardBuilder()
        kb.button(text="👁 خواندم", callback_data=f"seeno_{admin_id}_0")
        seen_kb = kb.as_markup()

    media = None
    sent = None
    if upload:
        data, fname, ctype = upload
        mtype = pick_type(ctype, len(data))
        f = BufferedInputFile(data, filename=fname)
        if hidden:
            # برای گرفتن file_id، فایل رو بی‌صدا برای خود ادمین می‌فرستیم و بلافاصله پاک می‌کنیم
            tmp = await send_stored(admin_id, mtype, f, text, silent=True)
            media = extract_media(tmp)
            try:
                await bot.delete_message(admin_id, tmp.message_id)
            except Exception:
                pass
        else:
            sent = await send_stored(user_id, mtype, f, text, target_msg_id, seen_kb)
            media = extract_media(sent)

    if hidden:
        row_id = await log_message(None, None, user_id, text, "out", admin_id, media)
        kb = InlineKeyboardBuilder()
        kb.button(text="👁 مشاهده پیام", callback_data=f"reveal_{row_id}")
        try:
            await bot.send_message(
                user_id, await get_dynamic_text("hidden_notice"),
                reply_markup=kb.as_markup(),
                reply_to_message_id=target_msg_id, allow_sending_without_reply=True)
        except Exception:
            async with conn() as db:
                await db.execute("DELETE FROM messages WHERE id = ?", (row_id,))
                await db.commit()
            raise
        return row_id

    if sent is None:
        sent = await send_stored(user_id, None, None, text, target_msg_id, seen_kb)
    return await log_message(sent.message_id, None, user_id, text, "out", admin_id, media)


async def app_delete(row_id):
    return await delete_everywhere(row_id)


async def set_admin_menu_buttons():
    if not MINI_APP_URL:
        log.warning("MINI_APP_URL خالیه؛ دکمه‌ی مینی‌اپ تنظیم نشد.")
        return
    for admin_id in ADMINS:
        try:
            await bot.set_chat_menu_button(chat_id=admin_id, menu_button=MenuButtonWebApp(
                text="🗂 پنل مینی‌اپ", web_app=WebAppInfo(url=MINI_APP_URL)))
        except Exception as e:
            log.warning("menu button for admin %s failed: %s", admin_id, e)


async def main():
    await init_db()
    await bot.delete_webhook(drop_pending_updates=False)  # اگه قبلاً وبهوک ست شده بود، polling کار کنه
    miniserver.hooks.send = app_send
    miniserver.hooks.delete = app_delete
    await set_admin_menu_buttons()

    server = uvicorn.Server(uvicorn.Config(web_app, host="0.0.0.0", port=PORT, log_level="info"))
    web = asyncio.create_task(server.serve())  # با SIGTERM ریلوی تمیز بسته می‌شه
    poll = asyncio.create_task(dp.start_polling(
        bot, handle_signals=False,
        allowed_updates=["message", "edited_message", "callback_query", "message_reaction"]))
    log.info("🚀 ربات و مینی‌اپ روشن شد (port %s)", PORT)

    done, pending = await asyncio.wait({web, poll}, return_when=asyncio.FIRST_COMPLETED)
    server.should_exit = True
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    await bot.session.close()
    for t in done:
        t.result()  # اگه یکی با خطا مرده، خطاش رو نشون بده


if __name__ == "__main__":
    asyncio.run(main())
