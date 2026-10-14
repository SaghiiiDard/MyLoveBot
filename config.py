"""تنظیمات پایه — فقط چیزهایی که برای بالا آمدن ربات لازمه.

قوانین ربات (ناشناس بودن، رسید بازدید، پیام مخفی و ...) اینجا نیست؛
مدیر از داخل خود ربات (⚙️ تنظیمات) یا مینی‌اپ عوض می‌کنه و توی دیتابیس ذخیره می‌شه.

روی Railway فقط یک متغیر لازمه: BOT_TOKEN
بقیه‌ی چیزها (دامنه، پورت، مسیر دیتابیس) خودکار از Railway خونده می‌شن.
"""
import os

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN تنظیم نشده. در Railway → Variables یک متغیر BOT_TOKEN بساز و توکن BotFather رو بذار.")

# آیدی عددی مدیران (آیدی عددی عمومی و غیرمحرمانه‌ست). اگه خواستی با متغیر ADMINS هم می‌شه عوضش کرد.
_DEFAULT_ADMINS = "8545602593,7153137815"
ADMINS = [int(x) for x in os.environ.get("ADMINS", _DEFAULT_ADMINS).replace(" ", "").split(",") if x]

# مسیر دیتابیس: اگه روی Railway یک Volume وصل کرده باشی، خودش RAILWAY_VOLUME_MOUNT_PATH رو ست می‌کنه.
_vol = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
DB_PATH = os.environ.get("DB_PATH") or (os.path.join(_vol, "selin_bot.db") if _vol else "selin_bot.db")

# آدرس مینی‌اپ: از دامنه‌ی عمومی Railway خودکار ساخته می‌شه.
MINI_APP_URL = os.environ.get("MINI_APP_URL", "").strip()
_rw = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
if not MINI_APP_URL and _rw:
    MINI_APP_URL = f"https://{_rw}/"
if MINI_APP_URL and not MINI_APP_URL.endswith("/"):
    MINI_APP_URL += "/"

PORT = int(os.environ.get("PORT", "8000"))

# هر پیام ورودی برای هر ادمین یک ردیف داره؛ این شرط برای نمایش تاریخچه، ردیف‌های تکراری رو حذف می‌کنه
DEDUP_SQL = ("{a}id IN (SELECT MIN(id) FROM messages "
             "GROUP BY user_id, COALESCE(user_msg_id, -id), direction)")
