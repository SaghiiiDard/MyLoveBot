"""
بک‌اند مینی‌اپ چت: مشاهده‌ی کامل پیام‌ها و رسانه‌ها + ارسال/حذف پیام + مدیریت کاربر.
همراه ربات داخل یک پروسس اجرا می‌شه؛ ربات توابع ارسال/حذف رو از طریق `hooks` تزریق می‌کنه.
"""
import asyncio
import hashlib
import hmac
import json
import mimetypes
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from urllib.parse import parse_qsl, quote

import aiohttp
import aiosqlite
from fastapi import FastAPI, File, Form, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import store
from config import ADMINS, BOT_TOKEN, DB_PATH, DEDUP_SQL

MAX_INIT_DATA_AGE = 86400         # ثانیه (۲۴ ساعت؛ مینی‌اپ باز می‌مونه و نباید زود منقضی بشه)
MEDIA_URL_TTL = 2 * 3600          # اعتبار لینک امضاشده‌ی رسانه
MAX_UPLOAD = 25 * 1024 * 1024     # حداکثر حجم آپلود از مینی‌اپ
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Selin Bot Mini App API")
hooks = SimpleNamespace(send=None, delete=None)   # توسط main.py مقداردهی می‌شه

MEDIA_LABEL = {"photo": "🖼 عکس", "video": "🎬 ویدیو", "animation": "🎞 گیف", "voice": "🎤 ویس",
               "audio": "🎵 موزیک", "video_note": "⏺ ویدیو پیام", "document": "📎 فایل",
               "sticker": "استیکر", "sticker_video": "استیکر", "sticker_anim": "استیکر"}
DEFAULT_MIME = {"photo": "image/jpeg", "video": "video/mp4", "animation": "video/mp4", "voice": "audio/ogg",
                "audio": "audio/mpeg", "video_note": "video/mp4", "sticker": "image/webp",
                "sticker_video": "video/webm", "document": "application/octet-stream"}


# ---------------------------------------------------------------- احراز هویت
def validate_init_data(init_data: str) -> dict:
    if not init_data:
        raise HTTPException(401, "initData ارسال نشده")
    try:
        parsed = dict(parse_qsl(init_data, strict_parsing=True))
    except ValueError:
        raise HTTPException(401, "initData نامعتبر")
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        raise HTTPException(401, "initData نامعتبر (بدون hash)")
    check = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    if not hmac.compare_digest(hmac.new(secret, check.encode(), hashlib.sha256).hexdigest(), received_hash):
        raise HTTPException(401, "initData نامعتبر (امضا مطابقت نداره)")
    try:
        auth_date = int(parsed.get("auth_date", 0))
    except ValueError:
        raise HTTPException(401, "initData نامعتبر")
    if time.time() - auth_date > MAX_INIT_DATA_AGE:
        raise HTTPException(401, "نشست منقضی شده — مینی‌اپ رو دوباره باز کن")
    return parsed


def require_admin(init_data: str) -> int:
    parsed = validate_init_data(init_data)
    try:
        user_id = int(json.loads(parsed.get("user", "{}")).get("id"))
    except (ValueError, TypeError, json.JSONDecodeError):
        raise HTTPException(401, "اطلاعات کاربر در initData نامعتبره")
    if user_id not in ADMINS:
        raise HTTPException(403, "فقط ادمین‌های ربات به این مینی‌اپ دسترسی دارن")
    return user_id


# ---------------------------------------------------------------- کمکی‌ها
async def resolve_user(db, ext_id: int) -> int:
    """در حالت ناشناس (store.get("show_sender")=0) به‌جای آیدی تلگرام یک شماره‌ی داخلی (anon_no) به فرانت داده می‌شه."""
    if store.get("show_sender"):
        return ext_id
    row = await (await db.execute("SELECT user_id FROM users WHERE anon_no = ?", (ext_id,))).fetchone()
    if not row:
        raise HTTPException(404, "کاربر یافت نشد")
    return row[0]


def _sign(row_id: int, exp: int) -> str:
    key = hashlib.sha256(b"media:" + BOT_TOKEN.encode()).digest()
    return hmac.new(key, f"{row_id}:{exp}".encode(), hashlib.sha256).hexdigest()[:32]


def media_url(row_id: int) -> str:
    exp = int(time.time()) + MEDIA_URL_TTL
    return f"/api/media/{row_id}?exp={exp}&sig={_sign(row_id, exp)}"


MSG_COLS = ("m.id, m.msg_text, m.direction, m.created_at, m.media_type, m.file_id, m.file_name, "
            "m.admin_id, a.first_name AS admin_name")
MSG_FROM = "FROM messages m LEFT JOIN users a ON a.user_id = m.admin_id"


def fmt_msg(r) -> dict:
    d = dict(r)
    if d.pop("file_id", None):
        d["media_url"] = media_url(d["id"])
    d["media_label"] = MEDIA_LABEL.get(d.get("media_type"))
    return d


async def fetch_message(db, row_id: int) -> dict:
    db.row_factory = aiosqlite.Row
    row = await (await db.execute(f"SELECT {MSG_COLS} {MSG_FROM} WHERE m.id = ?", (row_id,))).fetchone()
    return fmt_msg(row)


async def get_profile_dict(db, uid: int) -> dict:
    db.row_factory = aiosqlite.Row
    row = await (await db.execute(
        "SELECT user_id, first_name, username, is_blocked, is_favorite, chat_status, anon_no FROM users WHERE user_id = ?",
        (uid,))).fetchone()
    if not row:
        raise HTTPException(404, "کاربر یافت نشد")
    d = dict(row)
    if not store.get("show_sender"):
        d.update(user_id=d["anon_no"], first_name=f"ناشناس {d['anon_no']}", username=None)
    d["anonymous"] = not store.get("show_sender")
    d.pop("anon_no", None)
    return d


# ---------------------------------------------------------------- API
@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/api/chats")
async def list_chats(x_init_data: str = Header(None, alias="X-Init-Data")):
    require_admin(x_init_data)
    ph = ",".join("?" * len(ADMINS))
    d = DEDUP_SQL.format(a="m.")
    show = int(store.get("show_sender"))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(f"""
            SELECT CASE WHEN ? THEN u.user_id ELSE u.anon_no END AS user_id,
              CASE WHEN ? THEN u.first_name ELSE 'ناشناس ' || u.anon_no END AS first_name,
              CASE WHEN ? THEN u.username END AS username,
              u.is_blocked, u.is_favorite, u.chat_status,
              (SELECT msg_text FROM messages m WHERE m.user_id = u.user_id AND {d} ORDER BY m.id DESC LIMIT 1) AS last_msg,
              (SELECT media_type FROM messages m WHERE m.user_id = u.user_id AND {d} ORDER BY m.id DESC LIMIT 1) AS last_media,
              (SELECT created_at FROM messages m WHERE m.user_id = u.user_id AND {d} ORDER BY m.id DESC LIMIT 1) AS last_time,
              (SELECT COUNT(*) FROM messages m WHERE m.user_id = u.user_id AND {d}) AS msg_count
            FROM users u
            WHERE u.user_id NOT IN ({ph})
            ORDER BY last_time DESC
        """, [show, show, show, *ADMINS])
        rows = await cur.fetchall()
    out = []
    for r in rows:
        x = dict(r)
        if not x["last_msg"] and x["last_media"]:
            x["last_msg"] = MEDIA_LABEL.get(x["last_media"], "")
        out.append(x)
    return out


@app.get("/api/chats/{ext_id}/messages")
async def get_messages(
    ext_id: int,
    before_id: Optional[int] = Query(default=None),
    after_id: Optional[int] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    x_init_data: str = Header(None, alias="X-Init-Data"),
):
    require_admin(x_init_data)
    d = DEDUP_SQL.format(a="m.")
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        uid = await resolve_user(db, ext_id)
        sql = f"SELECT {MSG_COLS} {MSG_FROM} WHERE m.user_id = ? AND {d} "
        args = [uid]
        if after_id is not None:                       # پولینگ پیام‌های جدید
            sql += "AND m.id > ? ORDER BY m.id ASC LIMIT ?"
            args += [after_id, limit]
            rows = await (await db.execute(sql, args)).fetchall()
            return [fmt_msg(r) for r in rows]
        if before_id:
            sql += "AND m.id < ? "
            args.append(before_id)
        sql += "ORDER BY m.id DESC LIMIT ?"
        args.append(limit)
        rows = await (await db.execute(sql, args)).fetchall()
    return [fmt_msg(r) for r in reversed(rows)]


@app.post("/api/chats/{ext_id}/send")
async def send_message(
    ext_id: int,
    text: str = Form(""),
    reply_to: Optional[int] = Form(None),
    file: Optional[UploadFile] = File(None),
    x_init_data: str = Header(None, alias="X-Init-Data"),
):
    admin_id = require_admin(x_init_data)
    if hooks.send is None:
        raise HTTPException(503, "ربات هنوز آماده نیست")
    upload = None
    if file is not None and file.filename:
        data = await file.read(MAX_UPLOAD + 1)
        if len(data) > MAX_UPLOAD:
            raise HTTPException(413, f"حداکثر حجم فایل {MAX_UPLOAD // 1024 // 1024} مگابایته")
        ctype = file.content_type or mimetypes.guess_type(file.filename)[0] or "application/octet-stream"
        upload = (data, file.filename, ctype)
    if not text.strip() and not upload:
        raise HTTPException(400, "پیام خالیه")
    async with aiosqlite.connect(DB_PATH) as db:
        uid = await resolve_user(db, ext_id)
    try:
        row_id = await hooks.send(admin_id, uid, text, upload, reply_to)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"ارسال ناموفق بود: {e}")
    async with aiosqlite.connect(DB_PATH) as db:
        return await fetch_message(db, row_id)


@app.delete("/api/messages/{row_id}")
async def delete_message(row_id: int, x_init_data: str = Header(None, alias="X-Init-Data")):
    require_admin(x_init_data)
    if hooks.delete is None:
        raise HTTPException(503, "ربات هنوز آماده نیست")
    result = await hooks.delete(row_id)
    if result is None:
        raise HTTPException(404, "پیام پیدا نشد")
    return {"deleted": True, "user_side": result}


class ToggleBody(BaseModel):
    field: str


@app.post("/api/chats/{ext_id}/toggle")
async def toggle_user(ext_id: int, body: ToggleBody, x_init_data: str = Header(None, alias="X-Init-Data")):
    require_admin(x_init_data)
    sql = {
        "blocked": "UPDATE users SET is_blocked = 1 - COALESCE(is_blocked, 0) WHERE user_id = ?",
        "favorite": "UPDATE users SET is_favorite = 1 - COALESCE(is_favorite, 0) WHERE user_id = ?",
        "paused": "UPDATE users SET chat_status = CASE chat_status WHEN 'paused' THEN 'active' ELSE 'paused' END WHERE user_id = ?",
    }.get(body.field)
    if not sql:
        raise HTTPException(400, "فیلد نامعتبر")
    async with aiosqlite.connect(DB_PATH) as db:
        uid = await resolve_user(db, ext_id)
        await db.execute(sql, (uid,))
        await db.commit()
        return await get_profile_dict(db, uid)


@app.get("/api/settings")
async def get_rules(x_init_data: str = Header(None, alias="X-Init-Data")):
    require_admin(x_init_data)
    snap = store.snapshot()
    return [{"key": k, "title": v[1], "desc": v[2], "value": snap[k]} for k, v in store.RULES.items()]


@app.post("/api/settings/{key}/toggle")
async def toggle_rule(key: str, x_init_data: str = Header(None, alias="X-Init-Data")):
    require_admin(x_init_data)
    if key not in store.RULES:
        raise HTTPException(400, "تنظیم نامعتبر")
    await store.toggle(key)
    snap = store.snapshot()
    return [{"key": k, "title": v[1], "desc": v[2], "value": snap[k]} for k, v in store.RULES.items()]


@app.get("/api/profile/{ext_id}")
async def get_profile(ext_id: int, x_init_data: str = Header(None, alias="X-Init-Data")):
    require_admin(x_init_data)
    async with aiosqlite.connect(DB_PATH) as db:
        uid = await resolve_user(db, ext_id)
        return await get_profile_dict(db, uid)


# ---------------------------------------------------------------- رسانه (پروکسی به تلگرام)
_path_cache: dict = {}


async def _tg_file_path(file_id: str) -> str:
    hit = _path_cache.get(file_id)
    if hit and time.time() - hit[1] < 1800:
        return hit[0]
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile", data={"file_id": file_id}) as r:
            data = await r.json()
    if not data.get("ok"):
        raise HTTPException(404, "فایل در دسترس نیست (حجم بیش از ۲۰ مگابایت یا منقضی)")
    path = data["result"]["file_path"]
    _path_cache[file_id] = (path, time.time())
    if len(_path_cache) > 2000:
        _path_cache.clear()
    return path


@app.get("/api/media/{row_id}")
async def get_media(row_id: int, request: Request, exp: int, sig: str):
    # لینک امضاشده است تا <img>/<video> بدون هدر هم کار کنن
    if exp < time.time() or not hmac.compare_digest(sig, _sign(row_id, exp)):
        raise HTTPException(403, "لینک نامعتبر یا منقضی")
    async with aiosqlite.connect(DB_PATH) as db:
        row = await (await db.execute(
            "SELECT file_id, media_type, mime, file_name FROM messages WHERE id = ?", (row_id,))).fetchone()
    if not row or not row[0]:
        raise HTTPException(404, "رسانه پیدا نشد")
    file_id, media_type, mime, file_name = row
    path = await _tg_file_path(file_id)

    headers = {"Range": request.headers["range"]} if "range" in request.headers else {}
    session = aiohttp.ClientSession()
    try:
        resp = await session.get(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}", headers=headers)
    except Exception:
        await session.close()
        raise HTTPException(502, "خطا در دریافت فایل از تلگرام")
    if resp.status >= 400:
        resp.release()
        await session.close()
        raise HTTPException(404, "فایل در دسترس نیست")

    out = {k: resp.headers[k] for k in ("Content-Length", "Content-Range", "Accept-Ranges") if k in resp.headers}
    out["Cache-Control"] = "private, max-age=3600"
    if media_type == "document" and file_name:
        out["Content-Disposition"] = f"inline; filename*=UTF-8''{quote(file_name)}"

    async def body():
        try:
            async for chunk in resp.content.iter_chunked(64 * 1024):
                yield chunk
        finally:
            resp.release()
            await session.close()

    return StreamingResponse(body(), status_code=resp.status,
                             media_type=mime or DEFAULT_MIME.get(media_type, "application/octet-stream"), headers=out)


app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
