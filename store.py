"""قوانین ربات که مدیر از داخل ربات/مینی‌اپ تعیین می‌کنه (ذخیره در دیتابیس، کش در حافظه)."""
import sqlite3

import aiosqlite

from config import DB_PATH

# key: (پیش‌فرض، عنوان فارسی، توضیح)
RULES = {
    "show_sender": (1, "👤 نمایش هویت فرستنده", "روشن: نام/آیدی/یوزرنیم فرستنده به مدیر نشان داده می‌شود. خاموش: کاملاً ناشناس (ناشناس N)."),
    "seen_receipts": (1, "👁 رسید بازدید", "ارسال «خواندم» بین مدیر و کاربر."),
    "edit_tracking": (1, "✏️ همگام‌سازی ویرایش", "ویرایش پیام در یک طرف، در طرف دیگر هم اعمال شود."),
    "hidden_replies": (1, "📩 پاسخ مخفی", "پاسخ مدیر ابتدا به‌صورت اعلان می‌رسد و با دکمه باز می‌شود."),
    "send_confirm": (1, "✅ تأیید ارسال به کاربر", "بعد از هر پیام کاربر، «پیام ارسال شد» نشان داده شود."),
    "allow_media": (1, "🖼 دریافت رسانه از کاربران", "خاموش: فقط پیام متنی پذیرفته می‌شود."),
}

_cache: dict = {k: v[0] for k, v in RULES.items()}


def get(key: str) -> bool:
    return bool(_cache.get(key, RULES[key][0]))


def snapshot() -> dict:
    return {k: bool(_cache.get(k, v[0])) for k, v in RULES.items()}


async def load():
    """ساخت جدول، انتقال تنظیمات جدول قدیمی (settings) و بارگذاری کش."""
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value INTEGER)")
        have = {r[0] for r in await (await db.execute("SELECT key FROM bot_settings")).fetchall()}
        legacy = {}
        try:  # دیتابیس نسخه‌ی قبلی
            row = await (await db.execute(
                "SELECT seen_receipts, edit_tracking, hidden_replies FROM settings LIMIT 1")).fetchone()
            if row:
                legacy = dict(zip(("seen_receipts", "edit_tracking", "hidden_replies"), row))
        except sqlite3.OperationalError:
            pass
        for k, (default, _t, _d) in RULES.items():
            if k not in have:
                await db.execute("INSERT INTO bot_settings (key, value) VALUES (?, ?)", (k, legacy.get(k, default)))
        await db.commit()
        for k, v in await (await db.execute("SELECT key, value FROM bot_settings")).fetchall():
            if k in RULES:
                _cache[k] = int(v)


async def toggle(key: str) -> bool:
    if key not in RULES:
        raise KeyError(key)
    new = 0 if get(key) else 1
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        await db.execute("INSERT INTO bot_settings (key, value) VALUES (?, ?) "
                         "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, new))
        await db.commit()
    _cache[key] = new
    return bool(new)
