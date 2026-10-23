import time
import asyncio
import io
import json
import math
import os
import re
import threading
import urllib.parse
import urllib.request
import uuid
import wave
from array import array
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from zoneinfo import ZoneInfo

import edge_tts
import miniaudio
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from groq import Groq

app = FastAPI()
groq_client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY")
JAMENDO_CLIENT_ID = os.environ.get("JAMENDO_CLIENT_ID")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "").strip()
UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip()
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
HOME_LAT = float(os.environ.get("HOME_LAT", "0") or 0)
HOME_LNG = float(os.environ.get("HOME_LNG", "0") or 0)
HOME_RADIUS_M = float(os.environ.get("HOME_RADIUS_M", "500") or 500)
WELCOME_MUSIC_URL = os.environ.get("WELCOME_MUSIC_URL", "").strip()

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
SAMPLE_RATE = 16000
MIN_BYTES = SAMPLE_RATE * 2 // 2
STT_MODEL = "whisper-large-v3"
MAX_HISTORY = 10
LEAVE_TIMEOUT_S = 300
SCHEDULE_KEY = "xiaozhi:schedules"
OFFLINE_MSG = "Thiết bị đang offline, kiểm tra ESP32 đã bật và có WiFi."

ALARM_SECONDS = 5.0                    # thời lượng chuông reo trên loa (phải khớp với ESP32)

TTS_VOICE = "vi-VN-HoaiMyNeural"
TTS_CHUNK = 4096                       # ~128 ms âm thanh mỗi gói
SEND_AHEAD_S = 1.5                     # server gửi trước 1,5 giây để ESP32 có đệm
TTS_PEAK = int(0.6 * 32767)
MUSIC_PEAK = int(0.35 * 32767)
DEFAULT_VOLUME = 70

# Thiết bị điều khiển được, khớp với ESP32 (mỗi thiết bị một màu LED)
DEVICES = {
    "den":     {"label": "đèn",      "color": "đỏ"},
    "maylanh": {"label": "máy lạnh", "color": "xanh lá"},
    "tivi":    {"label": "tivi",     "color": "xanh dương"},
}

PREFERRED_LLM = [
    "openai/gpt-oss-120b",
    "openai/gpt-oss-20b",
    "qwen/qwen3.8-27b",
    "allam-2-7b",
]
LLM_CANDIDATES = None

MY_SONGS = {
    # "xuân này con không về": "https://raw.githubusercontent.com/USER/REPO/main/music/xuan-nay-con-khong-ve.mp3",
}
ARCHIVE_SEARCH_URL = "https://archive.org/advancedsearch.php"
ARCHIVE_MIN_BYTES = 1 * 1024 * 1024
ARCHIVE_MAX_BYTES = 15 * 1024 * 1024

STATE = {
    "volume": DEFAULT_VOLUME,
    "recent": [],
    "history": [],
    "music_task": None,
    "devices": {k: False for k in DEVICES},   # trạng thái thật do ESP32 báo lên
    "at_home": None,
    "leave_pending": False,
    "leave_task": None,
}
DEVICE = {"ws": None}
SCHED_LOCK = threading.Lock()


def build_menu() -> dict:
    d = STATE["devices"]

    def lab(k, icon):
        return f"{icon} {DEVICES[k]['label'].capitalize()}: {'BẬT' if d.get(k) else 'TẮT'}"

    return {"inline_keyboard": [
        [{"text": lab("den", "💡"), "callback_data": "dev:den:toggle"}],
        [{"text": lab("maylanh", "❄️"), "callback_data": "dev:maylanh:toggle"}],
        [{"text": lab("tivi", "📺"), "callback_data": "dev:tivi:toggle"}],
        [{"text": "Tắt tất cả", "callback_data": "dev:ALL:off"},
         {"text": "Trạng thái", "callback_data": "status"}],
        [{"text": "Bật micro", "callback_data": "start"},
         {"text": "Dừng micro", "callback_data": "stop"}],
        [{"text": "Xem lịch", "callback_data": "list"},
         {"text": "Tạm biệt", "callback_data": "bye"}],
    ]}


MENU_KEYBOARD = {"inline_keyboard": [
    [{"text": "Bật đèn", "callback_data": "dev:den:on"}],
]}  # đánh dấu để tg_text biết cần dựng menu mới

LEAVE_KEYBOARD = {"inline_keyboard": [
    [{"text": "Tắt tất cả", "callback_data": "leave_off"},
     {"text": "Giữ bật", "callback_data": "leave_keep"}],
]}

DEVICE_RULES = (
    "Thiết bị có thể điều khiển: đèn (đỏ), máy lạnh (xanh lá), tivi (xanh dương). "
    "Khi người dùng nói bật hoặc tắt một thiết bị, gọi device_control với device và action. "
    "Khi người dùng nói tắt tất cả, gọi device_control với device là tatca và action là off. "
    "Khi người dùng đặt hẹn hoặc nhắc việc theo giờ, gọi add_schedule. "
    "Khi cần thông tin mới (thời tiết, tin tức, giá cả, sự kiện) hãy gọi công cụ trước khi trả lời. "
    "Khi người dùng nhờ phát nhạc hoặc hát, gọi play_music. Bạn không tự hát được bài mới. "
    "Khi người dùng nói tạm biệt hoặc tắt bot, gọi shutdown_bot. "
    "Bạn chưa có quyền truy cập lịch Google, email hay thiết bị khác; nếu được hỏi, hãy nói thật."
)

VOICE_PROMPT = (
    "Bạn là trợ lý giọng nói chạy trên thiết bị ESP32, người dùng nghe câu trả lời qua loa. "
    "Luôn trả lời bằng tiếng Việt, tự nhiên, ngắn gọn, tối đa 3 câu. "
    "Không dùng markdown, emoji, danh sách hay đường link."
)

TEXT_PROMPT = (
    "Bạn là trợ lý nhà thông minh đang trò chuyện với chủ nhân qua Telegram, giống như một người bạn. "
    "Trả lời tự nhiên, thân thiện, rõ ràng, có thể dài 2 đến 5 câu khi cần. "
    "Không dùng markdown hay emoji. "
    "Khi thực hiện một việc như bật tắt thiết bị, đặt lịch, phát nhạc hay tra cứu, hãy gọi công cụ rồi viết lại kết quả bằng câu tự nhiên. "
    "Không chỉ sao chép thông báo của công cụ. "
    "Khi đặt lịch, xác nhận đầy đủ: việc gì, lúc mấy giờ, ngày nào, và mã lịch. "
    "Nếu thiếu thông tin như giờ hoặc thiết bị, hỏi lại một câu ngắn gọn."
)


def system_prompt(mode: str = "voice") -> str:
    now = datetime.now(TZ).strftime("%H:%M, ngày %d/%m/%Y")
    base = TEXT_PROMPT if mode == "text" else VOICE_PROMPT
    status = "Trạng thái thiết bị hiện tại: " + "; ".join(
        f"{v['label']} {'đang bật' if STATE['devices'].get(k) else 'đang tắt'}"
        for k, v in DEVICES.items())
    return f"{base} {DEVICE_RULES} {status}. Thời gian hiện tại ở Việt Nam: {now}."


def normalize(s: str) -> str:
    return re.sub(r"[^\w\s]", "", s.lower()).strip()


def distance_m(lat1, lon1, lat2, lon2) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def http_json(url: str, data=None, timeout: int = 10):
    body = json.dumps(data).encode("utf-8") if data is not None else None
    req = urllib.request.Request(url, data=body, headers={"User-Agent": "xiaozhi-bot/1.0"})
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


# ---------- Upstash: lưu lịch bền vững ----------
def upstash_ready() -> bool:
    return bool(UPSTASH_URL and UPSTASH_TOKEN)


def redis_cmd(*args):
    if not upstash_ready():
        raise RuntimeError("Chưa cấu hình Upstash")
    req = urllib.request.Request(
        UPSTASH_URL,
        data=json.dumps(list(args)).encode("utf-8"),
        headers={"Authorization": f"Bearer {UPSTASH_TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        data = json.loads(r.read().decode("utf-8"))
    if "error" in data:
        raise RuntimeError(data["error"])
    return data.get("result")


def load_schedules():
    raw = redis_cmd("GET", SCHEDULE_KEY)
    return json.loads(raw) if raw else []


def save_schedules(items):
    redis_cmd("SET", SCHEDULE_KEY, json.dumps(items, ensure_ascii=False))


def sched_read():
    with SCHED_LOCK:
        return load_schedules()


def sched_add(item):
    with SCHED_LOCK:
        items = load_schedules()
        items.append(item)
        save_schedules(items)


def sched_update(item):
    with SCHED_LOCK:
        items = load_schedules()
        items = [item if i["id"] == item["id"] else i for i in items]
        save_schedules(items)


def sched_remove(ids):
    with SCHED_LOCK:
        items = load_schedules()
        save_schedules([i for i in items if i["id"] not in ids])


def sched_cancel(sid: str) -> bool:
    with SCHED_LOCK:
        items = load_schedules()
        new = [i for i in items if i["id"] != sid]
        if len(new) == len(items):
            return False
        save_schedules(new)
        return True


def parse_when(s: str) -> datetime:
    s = (s or "").strip()
    now = datetime.now(TZ)
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})", s)
    if m:
        y, mo, d, h, mi = map(int, m.groups())
        return datetime(y, mo, d, h, mi, tzinfo=TZ)
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", s)
    if m:
        h, mi = map(int, m.groups())
        t = now.replace(hour=h, minute=mi, second=0, microsecond=0)
        if t <= now:
            t += timedelta(days=1)
        return t
    raise ValueError("Thời gian phải dạng HH:MM hoặc YYYY-MM-DD HH:MM")


def schedule_selftest() -> str:
    if not upstash_ready():
        return (f"Thiếu biến Upstash: URL {'có' if UPSTASH_URL else 'THIẾU'}, "
                f"TOKEN {'có' if UPSTASH_TOKEN else 'THIẾU'}")
    try:
        test_item = {"id": "test01", "at": "2099-01-01T00:00:00+07:00",
                     "action": "remind", "note": "test"}
        sched_add(test_item)
        found = any(i.get("id") == "test01" for i in sched_read())
        sched_remove(["test01"])
        return ("Upstash hoạt động: ghi và đọc lịch OK" if found
                else "Ghi được nhưng không đọc lại được lịch")
    except Exception as e:
        return f"Lỗi Upstash: {e}"


# ---------- Điều khiển thiết bị ----------
async def send_raw(text: str) -> bool:
    ws = DEVICE["ws"]
    if ws is None:
        return False
    try:
        await ws.send_text(text)
        return True
    except Exception as e:
        print("Lỗi gửi tới thiết bị:", e, flush=True)
        return False


async def send_device(name: str, on: bool) -> bool:
    return await send_raw(f"DEV:{name}:{'ON' if on else 'OFF'}")


async def send_all_off() -> bool:
    return await send_raw("DEV:ALL:OFF")


def any_device_on() -> bool:
    return any(STATE["devices"].values())


def devices_text() -> str:
    lines = []
    for k, info in DEVICES.items():
        state = "BẬT" if STATE["devices"].get(k) else "TẮT"
        lines.append(f"{info['label']} ({info['color']}): {state}")
    return "\n".join(lines)


# ---------- Công cụ ----------
def tool_get_time(ctx) -> str:
    return datetime.now(TZ).strftime("%H giờ %M phút, ngày %d tháng %m năm %Y")


def tool_web_search(ctx, query: str) -> str:
    if TAVILY_API_KEY:
        data = http_json(
            "https://api.tavily.com/search",
            data={"api_key": TAVILY_API_KEY, "query": query,
                  "max_results": 3, "search_depth": "basic"},
        )
        parts = [f"{r.get('title', '')}: {r.get('content', '')}" for r in data.get("results", [])]
        return "\n".join(parts)[:2000] or "Không có kết quả"

    q = urllib.parse.quote(query)
    s = http_json(f"https://vi.wikipedia.org/w/api.php?action=query&list=search"
                  f"&srsearch={q}&format=json&srlimit=2")
    hits = s.get("query", {}).get("search", [])
    if not hits:
        return "Không có kết quả"
    titles = urllib.parse.quote("|".join(h["title"] for h in hits))
    ex = http_json(f"https://vi.wikipedia.org/w/api.php?action=query&prop=extracts"
                   f"&exintro=1&explaintext=1&format=json&titles={titles}")
    pages = ex.get("query", {}).get("pages", {}).values()
    return "\n".join(f"{p.get('title')}: {p.get('extract', '')}" for p in pages)[:2000]


def tool_get_weather(ctx, city: str) -> str:
    geo = http_json("https://geocoding-api.open-meteo.com/v1/search?" +
                    urllib.parse.urlencode({"name": city, "count": 1, "language": "vi"}))
    res = geo.get("results") or []
    if not res:
        return "Không tìm thấy địa điểm này"
    p = res[0]
    fc = http_json(
        "https://api.open-meteo.com/v1/forecast?"
        f"latitude={p['latitude']}&longitude={p['longitude']}"
        "&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"
        "&timezone=Asia%2FBangkok"
    )
    c = fc["current"]
    return (f"{p['name']}: {c['temperature_2m']} độ C, độ ẩm {c['relative_humidity_2m']} phần trăm, "
            f"gió {c['wind_speed_10m']} km/h, lượng mưa {c['precipitation']} mm")


def tool_set_volume(ctx, level: int = None, change: str = None) -> str:
    cur = ctx["volume"]
    if level is not None:
        new = int(level)
    elif change == "up":
        new = cur + 20
    elif change == "down":
        new = cur - 20
    elif change == "mute":
        new = 0
    elif change == "max":
        new = 100
    else:
        return "Thiếu tham số âm lượng"
    new = max(0, min(100, new))
    ctx["volume"] = new
    return f"Âm lượng hiện là {new} phần trăm"


def tool_send_telegram(ctx, message: str) -> str:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return "Chưa cấu hình Telegram trên server"
    text = (message or "").strip()[:4000]
    if not text:
        return "Nội dung tin nhắn trống"
    data = http_json(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=15)
    return "Đã gửi tin nhắn Telegram" if data.get("ok") else "Gửi Telegram thất bại"


def tool_device_control(ctx, device: str, action: str) -> str:
    if action not in ("on", "off"):
        return "Hành động không hợp lệ"
    if device == "tatca":
        if action != "off":
            return "Chỉ có thể tắt tất cả"
        ctx["devcmds"].append(("ALL", False))
        return "Sẽ tắt tất cả thiết bị"
    if device not in DEVICES:
        return "Không có thiết bị này"
    ctx["devcmds"].append((device, action == "on"))
    return f"Sẽ {'bật' if action == 'on' else 'tắt'} {DEVICES[device]['label']}"


def tool_shutdown_bot(ctx) -> str:
    ctx["standby"] = True
    return "Sẽ chuyển về chế độ chờ sau lời tạm biệt"


def tool_add_schedule(ctx, when: str, action: str, device: str = "", note: str = "") -> str:
    if action not in ("device_on", "device_off", "remind"):
        return "Hành động không hợp lệ"
    if action != "remind" and device not in DEVICES:
        return "Cần chọn thiết bị: đèn, máy lạnh hoặc tivi"
    try:
        t = parse_when(when)
    except ValueError as e:
        return str(e)
    if t <= datetime.now(TZ):
        return "Thời gian này đã qua, hãy chọn giờ khác"
    item = {"id": uuid.uuid4().hex[:6], "at": t.isoformat(), "action": action,
            "device": device, "note": note[:200], "tries": 0}
    try:
        sched_add(item)
    except Exception as e:
        print("Lỗi ghi lịch:", e, flush=True)
        return f"Không lưu được lịch: {e}"
    if action == "remind":
        label = "nhắc"
    else:
        label = ("bật " if action == "device_on" else "tắt ") + DEVICES[device]["label"]
    return f"Đã đặt lịch {label} lúc {t.strftime('%H giờ %M, ngày %d/%m/%Y')}, mã {item['id']}"


def tool_list_schedules(ctx) -> str:
    items = sched_read()
    if not items:
        return "Hiện không có lịch nào"
    lines = []
    for it in sorted(items, key=lambda x: x["at"]):
        t = datetime.fromisoformat(it["at"]).astimezone(TZ)
        if it["action"] == "remind":
            what = f"nhắc: {it.get('note', '')}"
        else:
            name = DEVICES.get(it.get("device", ""), {}).get("label", "?")
            what = ("bật " if it["action"] == "device_on" else "tắt ") + name
        lines.append(f"{it['id']}: {t.strftime('%H:%M %d/%m')} {what}")
    return "; ".join(lines)


def tool_cancel_schedule(ctx, schedule_id: str) -> str:
    return "Đã hủy lịch" if sched_cancel(schedule_id) else "Không tìm thấy mã lịch này"


# ---------- Nhạc ----------
def archive_search_track(query: str):
    clean = re.sub(r"[^\w\s]", " ", query, flags=re.UNICODE).strip()
    if not clean:
        return None
    q = f"({clean}) AND mediatype:audio AND collection:opensource_audio"
    params = urllib.parse.urlencode([
        ("q", q), ("fl[]", "identifier"), ("fl[]", "title"), ("fl[]", "creator"),
        ("rows", "5"), ("output", "json"),
    ])
    data = http_json(f"{ARCHIVE_SEARCH_URL}?{params}", timeout=20)
    for doc in data.get("response", {}).get("docs", []):
        ident = doc.get("identifier")
        if not ident:
            continue
        meta = http_json(f"https://archive.org/metadata/{ident}", timeout=20)
        mp3s = []
        for f in meta.get("files", []):
            name = f.get("name", "")
            size = int(f.get("size", 0) or 0)
            if name.lower().endswith(".mp3") and ARCHIVE_MIN_BYTES <= size <= ARCHIVE_MAX_BYTES:
                mp3s.append((size, name))
        if not mp3s:
            continue
        mp3s.sort(reverse=True)
        _, name = mp3s[0]
        creator = doc.get("creator") or ""
        if isinstance(creator, list):
            creator = ", ".join(creator)
        license_url = (meta.get("metadata", {}) or {}).get("licenseurl", "")
        return {
            "url": f"https://archive.org/download/{ident}/{urllib.parse.quote(name)}",
            "title": doc.get("title") or ident,
            "artist": creator,
            "license": license_url or "không rõ",
        }
    return None


def tool_play_music(ctx, query: str) -> str:
    q = normalize(query)
    for title, url in MY_SONGS.items():
        t = normalize(title)
        if q and (q in t or t in q):
            ctx["music"] = {"url": url, "title": title, "artist": "", "license": "kho riêng"}
            return f"Sẽ phát bài {title}"
    try:
        track = archive_search_track(query)
    except Exception as e:
        print("Lỗi Internet Archive:", e, flush=True)
        track = None
    if track:
        ctx["music"] = track
        return f"Sẽ phát bài {track['title']} của {track['artist'] or 'không rõ'}"
    if not JAMENDO_CLIENT_ID:
        return "Không tìm thấy bài này"
    url = "https://api.jamendo.com/v3.0/tracks/?" + urllib.parse.urlencode({
        "client_id": JAMENDO_CLIENT_ID, "format": "json", "limit": 1,
        "search": query, "audioformat": "mp32",
    })
    res = http_json(url, timeout=15).get("results") or []
    if not res or not res[0].get("audio"):
        return "Không tìm thấy bài phù hợp"
    t = res[0]
    ctx["music"] = {"url": t["audio"], "title": t.get("name", ""),
                    "artist": t.get("artist_name", ""), "license": "Jamendo CC"}
    return f"Sẽ phát bài {t.get('name')} của {t.get('artist_name')}"


TOOL_FUNCS = {
    "get_time": tool_get_time,
    "web_search": tool_web_search,
    "get_weather": tool_get_weather,
    "set_volume": tool_set_volume,
    "play_music": tool_play_music,
    "send_telegram": tool_send_telegram,
    "device_control": tool_device_control,
    "shutdown_bot": tool_shutdown_bot,
    "add_schedule": tool_add_schedule,
    "list_schedules": tool_list_schedules,
    "cancel_schedule": tool_cancel_schedule,
}

TOOLS = [
    {"type": "function", "function": {"name": "get_time",
        "description": "Lấy ngày và giờ hiện tại ở Việt Nam",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {"name": "web_search",
        "description": "Tìm thông tin mới trên web",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"}}, "required": ["query"]}}},
    {"type": "function", "function": {"name": "get_weather",
        "description": "Lấy thời tiết hiện tại của một thành phố",
        "parameters": {"type": "object", "properties": {
            "city": {"type": "string"}}, "required": ["city"]}}},
    {"type": "function", "function": {"name": "set_volume",
        "description": "Chỉnh âm lượng loa. Dùng level hoặc change.",
        "parameters": {"type": "object", "properties": {
            "level": {"type": "integer", "minimum": 0, "maximum": 100},
            "change": {"type": "string", "enum": ["up", "down", "mute", "max"]}}}}},
    {"type": "function", "function": {"name": "play_music",
        "description": "Tìm và phát một bài nhạc có sẵn theo tên bài, ca sĩ hoặc thể loại",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"}}, "required": ["query"]}}},
    {"type": "function", "function": {"name": "send_telegram",
        "description": "Gửi một tin nhắn Telegram đến chủ nhân",
        "parameters": {"type": "object", "properties": {
            "message": {"type": "string"}}, "required": ["message"]}}},
    {"type": "function", "function": {"name": "device_control",
        "description": "Bật hoặc tắt đèn, máy lạnh, tivi. Dùng device tatca với action off để tắt tất cả.",
        "parameters": {"type": "object", "properties": {
            "device": {"type": "string", "enum": ["den", "maylanh", "tivi", "tatca"]},
            "action": {"type": "string", "enum": ["on", "off"]}},
            "required": ["device", "action"]}}},
    {"type": "function", "function": {"name": "shutdown_bot",
        "description": "Chuyển bot về chế độ chờ khi người dùng nói tạm biệt hoặc tắt bot",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {"name": "add_schedule",
        "description": "Đặt lịch bật hoặc tắt một thiết bị, hoặc nhắc việc theo giờ. Với remind, note là nội dung cần nhắc, ví dụ 'đi giặt đồ'.",
        "parameters": {"type": "object", "properties": {
            "when": {"type": "string", "description": "HH:MM hoặc YYYY-MM-DD HH:MM, giờ Việt Nam"},
            "action": {"type": "string", "enum": ["device_on", "device_off", "remind"]},
            "device": {"type": "string", "enum": ["den", "maylanh", "tivi"]},
            "note": {"type": "string", "description": "nội dung cần nhắc (chỉ dùng khi action là remind)"}},
            "required": ["when", "action"]}}},
    {"type": "function", "function": {"name": "list_schedules",
        "description": "Xem danh sách lịch đã đặt",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {"name": "cancel_schedule",
        "description": "Hủy một lịch theo mã",
        "parameters": {"type": "object", "properties": {
            "schedule_id": {"type": "string"}}, "required": ["schedule_id"]}}},
]


# ---------- Hỏi AI ----------
def llm_candidates() -> list:
    global LLM_CANDIDATES
    if LLM_CANDIDATES is None:
        ids = {m.id for m in groq_client.models.list().data}
        LLM_CANDIDATES = [name for name in PREFERRED_LLM if name in ids]
        print("Model chat sẽ dùng:", LLM_CANDIDATES, flush=True)
        if not LLM_CANDIDATES:
            LLM_CANDIDATES = None
            raise RuntimeError("Key Groq không có model chat nào trong danh sách ưu tiên")
    return LLM_CANDIDATES


def llm_chat(messages: list, tools=None, max_tokens: int = 300):
    last_err = None
    for model in llm_candidates():
        try:
            kwargs = dict(model=model, messages=messages, max_tokens=max_tokens, temperature=0.6)
            if tools:
                kwargs["tools"] = tools
                kwargs["tool_choice"] = "auto"
            return groq_client.chat.completions.create(**kwargs).choices[0].message
        except Exception as e:
            print(f"Model {model} lỗi, thử model khác: {e}", flush=True)
            last_err = e
    raise last_err


def run_agent(history: list, user_text: str, ctx: dict, mode: str = "voice") -> str:
    messages = ([{"role": "system", "content": system_prompt(mode)}]
                + history + [{"role": "user", "content": user_text}])
    for _ in range(4):
        msg = llm_chat(messages, TOOLS)
        if not msg.tool_calls:
            return (msg.content or "").strip()
        messages.append({
            "role": "assistant",
            "content": msg.content or "",
            "tool_calls": [{"id": tc.id, "type": "function",
                            "function": {"name": tc.function.name,
                                         "arguments": tc.function.arguments}}
                           for tc in msg.tool_calls],
        })
        for tc in msg.tool_calls:
            fn = TOOL_FUNCS.get(tc.function.name)
            try:
                args = json.loads(tc.function.arguments or "{}")
                print("Công cụ:", tc.function.name, args, flush=True)
                result = fn(ctx, **args) if fn else "Công cụ không tồn tại"
            except Exception as e:
                result = f"Lỗi: {e}"
                print("Lỗi công cụ:", e, flush=True)
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": str(result)[:2000]})
    return "Mình chưa xử lý xong yêu cầu, bạn thử lại nhé."


# ---------- Giọng nói ----------
def pcm_to_wav(pcm: bytes) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(pcm)
    return buf.getvalue()


def speech_to_text(pcm: bytes) -> str:
    result = groq_client.audio.transcriptions.create(
        file=("audio.wav", pcm_to_wav(pcm)), model=STT_MODEL, language="vi")
    return result.text or ""


def is_echo(text: str, recent: list) -> bool:
    t = normalize(text)
    if len(t) < 6:
        return False
    for r in recent:
        rr = normalize(r)
        if rr and (t in rr or SequenceMatcher(None, t, rr).ratio() > 0.6):
            return True
    return False


def normalize_pcm(pcm: bytes, peak_target: int) -> bytes:
    samples = array("h")
    samples.frombytes(pcm)
    peak = max((abs(x) for x in samples), default=0)
    if peak == 0:
        return pcm
    gain = peak_target / peak
    out = array("h", (max(-32768, min(32767, int(x * gain))) for x in samples))
    return out.tobytes()


async def text_to_speech_pcm(text: str) -> bytes:
    communicate = edge_tts.Communicate(text, TTS_VOICE)
    mp3 = bytearray()
    async for chunk in communicate.stream():
        if chunk["type"] == "audio":
            mp3.extend(chunk["data"])
    if not mp3:
        raise RuntimeError("TTS không trả về audio")
    decoded = miniaudio.decode(bytes(mp3), output_format=miniaudio.SampleFormat.SIGNED16,
                               nchannels=1, sample_rate=SAMPLE_RATE)
    return await asyncio.to_thread(normalize_pcm, decoded.samples.tobytes(), TTS_PEAK)


def download_track_pcm(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "xiaozhi-bot/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        mp3 = r.read()
    decoded = miniaudio.decode(mp3, output_format=miniaudio.SampleFormat.SIGNED16,
                               nchannels=1, sample_rate=SAMPLE_RATE)
    return normalize_pcm(decoded.samples.tobytes(), MUSIC_PEAK)


async def send_audio(ws: WebSocket, pcm: bytes):
    bytes_per_s = SAMPLE_RATE * 2
    start = time.monotonic()
    sent = 0
    for i in range(0, len(pcm), TTS_CHUNK):
        chunk = pcm[i:i + TTS_CHUNK]
        await ws.send_bytes(chunk)
        sent += len(chunk)
        delay = (start + sent / bytes_per_s - SEND_AHEAD_S) - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
    print(f"Đã gửi xong audio: {sent}/{len(pcm)} bytes", flush=True)


async def speak_and_end(ws: WebSocket, text: str):
    try:
        await ws.send_text(text)
        pcm = await text_to_speech_pcm(text)
        await send_audio(ws, pcm)
    except Exception as e:
        print("Lỗi nói:", e, flush=True)
    try:
        await ws.send_text("TTS_END")
    except Exception:
        pass


# ---------- Telegram ----------
def tg_api(method: str, payload: dict):
    return http_json(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
                     data=payload, timeout=15)


async def tg_text(text: str, keyboard=None):
    if keyboard is MENU_KEYBOARD:
        keyboard = build_menu()          # menu luôn hiện trạng thái mới nhất
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000]}
    if keyboard:
        payload["reply_markup"] = keyboard
    try:
        res = await asyncio.to_thread(tg_api, "sendMessage", payload)
        print("Đã gửi Telegram, ok =", res.get("ok"), flush=True)
    except Exception as e:
        print("Lỗi gửi Telegram:", e, flush=True)


# ---------- Phát nhạc ----------
def stop_music():
    task = STATE.get("music_task")
    if task and not task.done():
        task.cancel()
    STATE["music_task"] = None


async def play_music(ws: WebSocket, track: dict):
    try:
        print("Phát nhạc:", track["title"], "-", track["artist"],
              "| license:", track.get("license", ""), flush=True)
        pcm = await asyncio.to_thread(download_track_pcm, track["url"])
        await send_audio(ws, pcm)
    except asyncio.CancelledError:
        print("Đã dừng nhạc", flush=True)
        raise
    except Exception as e:
        print("Lỗi nhạc:", e, flush=True)
    finally:
        STATE["music_task"] = None
        try:
            await ws.send_text("TTS_END")
        except Exception:
            pass


def start_music(ws: WebSocket, track: dict):
    stop_music()
    STATE["music_task"] = asyncio.create_task(play_music(ws, track))


# ---------- Trả lời chung: giọng nói và Telegram ----------
async def answer(user_text: str, voice: bool) -> bool:
    """voice=True: trả lời bằng loa ESP32. voice=False: trả lời bằng chữ Telegram và đọc qua loa."""
    mode = "voice" if voice else "text"
    ctx = {"volume": STATE["volume"], "music": None, "devcmds": [], "standby": False}
    try:
        reply = await asyncio.to_thread(run_agent, STATE["history"], user_text, ctx, mode)
    except Exception as e:
        print("Lỗi AI:", e, flush=True)
        reply = f"Lỗi: {e}"
    else:
        STATE["history"].append({"role": "user", "content": user_text})
        STATE["history"].append({"role": "assistant", "content": reply})
        del STATE["history"][:-MAX_HISTORY]

    if ctx["volume"] != STATE["volume"]:
        STATE["volume"] = ctx["volume"]
        await send_raw(f"VOL:{STATE['volume']}")

    offline = False
    for name, on in ctx["devcmds"]:
        ok = await (send_all_off() if name == "ALL" else send_device(name, on))
        offline = offline or not ok
        if ok and name != "ALL":
            STATE["devices"][name] = on
        elif ok and name == "ALL":
            for k in STATE["devices"]:
                STATE["devices"][k] = False
    if offline:
        reply = (reply + " " if reply else "") + OFFLINE_MSG

    if ctx["music"] and not reply:
        reply = f"Đang phát {ctx['music']['title']}."
    if not reply:
        reply = "Đã thực hiện yêu cầu." if ctx["devcmds"] else "Mình chưa có câu trả lời, bạn nói lại nhé."

    STATE["recent"] = (STATE["recent"] + [reply])[-3:]
    print("Trả lời:", reply, flush=True)

    music_started = False
    if voice:
        ws = DEVICE["ws"]
        if ws is None:
            return False
        try:
            await ws.send_text(reply)
            pcm = await text_to_speech_pcm(reply)
            print(f"TTS: {len(pcm)} bytes, đang phát", flush=True)
            await send_audio(ws, pcm)
        except Exception as e:
            print("Lỗi TTS:", e, flush=True)
    else:
        await tg_text(reply)
        if DEVICE["ws"] is not None:
            await speak_and_end(DEVICE["ws"], reply)

    if ctx["standby"]:
        await send_raw("CMD:STANDBY")

    if ctx["music"]:
        if DEVICE["ws"] is not None:
            start_music(DEVICE["ws"], ctx["music"])
            music_started = True
        else:
            await tg_text("Thiết bị chưa kết nối nên chưa phát được nhạc.")
    return music_started


async def handle_turn(ws: WebSocket, pcm: bytes):
    music_started = False
    try:
        if len(pcm) < MIN_BYTES:
            print("Âm thanh quá ngắn, bỏ qua", flush=True)
            return
        user_text = (await asyncio.to_thread(speech_to_text, pcm)).strip()
        print("Người dùng nói:", user_text, flush=True)
        if len(normalize(user_text)) < 2:
            return
        if is_echo(user_text, STATE["recent"]):
            print("Bỏ qua: tiếng vọng từ loa", flush=True)
            return
        await ws.send_text("USER:" + user_text)
        music_started = await answer(user_text, voice=True)
    except Exception as e:
        print("Lỗi lượt nói:", e, flush=True)
    finally:
        if not music_started:
            try:
                await ws.send_text("TTS_END")
            except Exception:
                pass


# ---------- Chào khi thiết bị khởi động ----------
async def welcome(ws: WebSocket):
    greeting = "Kính chào chủ nhân."
    try:
        await ws.send_text(greeting)
        pcm = await text_to_speech_pcm(greeting)
        await send_audio(ws, pcm)
    except Exception as e:
        print("Lỗi chào:", e, flush=True)
    if WELCOME_MUSIC_URL:
        start_music(ws, {"url": WELCOME_MUSIC_URL, "title": "nhạc chào mừng",
                         "artist": "", "license": "tự cấu hình"})
    else:
        try:
            await ws.send_text("TTS_END")
        except Exception:
            pass


# ---------- Lịch hẹn ----------
async def ring_alarm() -> bool:
    """Gửi lệnh reo chuông tới ESP32 và đợi chuông reo xong. Trả về False nếu offline."""
    if DEVICE["ws"] is None:
        return False
    await send_raw("CMD:ALARM")
    await asyncio.sleep(ALARM_SECONDS + 0.5)
    return True


async def run_schedule(item: dict) -> bool:
    """
    Trả về True nếu đã xử lý xong lịch (kể cả khi loa offline, Telegram vẫn nhận tin).
    Trả về False chỉ với lịch điều khiển thiết bị khi ESP32 offline, để thử lại sau.
    """
    action = item["action"]

    if action in ("device_on", "device_off"):
        name = item.get("device", "")
        on = action == "device_on"
        label = DEVICES.get(name, {}).get("label", name)
        if DEVICE["ws"] is None:
            return False                       # thiết bị offline: thử lại sau
        await ring_alarm()
        ok = await send_device(name, on)
        if ok:
            STATE["devices"][name] = on
        verb = "bật" if on else "tắt"
        msg = (f"Đã {verb} {label} theo lịch." if ok
               else f"Không {verb} được {label} vì thiết bị offline.")

    else:
        note = (item.get("note") or "").strip()
        msg = f"Đến giờ rồi, bạn nhớ: {note}" if note else "Đến giờ rồi."
        if DEVICE["ws"] is not None:
            await ring_alarm()

    # Luôn nhắn Telegram để điện thoại có thông báo, dù loa có online hay không
    await tg_text("🔔 " + msg)

    # Nếu loa online thì đọc nội dung sau khi chuông reo
    if DEVICE["ws"] is not None:
        await speak_and_end(DEVICE["ws"], msg)
    return True


async def scheduler_loop():
    print("Bộ hẹn giờ đã khởi động", flush=True)
    while True:
        await asyncio.sleep(20)
        if not upstash_ready():
            continue
        try:
            items = await asyncio.to_thread(sched_read)
            now = datetime.now(TZ)
            due = [i for i in items if datetime.fromisoformat(i["at"]) <= now]
            for it in due:
                print("Đến giờ lịch:", it["id"], it["action"], it.get("device", ""), flush=True)
                ok = await run_schedule(it)
                if ok or it.get("tries", 0) >= 30:
                    await asyncio.to_thread(sched_remove, [it["id"]])
                else:
                    it["tries"] = it.get("tries", 0) + 1
                    it["at"] = (now + timedelta(minutes=1)).isoformat()
                    await asyncio.to_thread(sched_update, it)
        except Exception as e:
            print("Lỗi lịch:", e, flush=True)


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(scheduler_loop())


# ---------- Vị trí và rời nhà ----------
def cancel_leave():
    STATE["leave_pending"] = False
    task = STATE.get("leave_task")
    if task and not task.done():
        task.cancel()
    STATE["leave_task"] = None


async def leave_timeout():
    try:
        await asyncio.sleep(LEAVE_TIMEOUT_S)
    except asyncio.CancelledError:
        return
    if STATE["leave_pending"]:
        STATE["leave_pending"] = False
        ok = await send_all_off()
        if ok:
            for k in STATE["devices"]:
                STATE["devices"][k] = False
            await tg_text("Hết 5 phút không có phản hồi, mình đã tắt các thiết bị để an toàn.")
        else:
            await tg_text("Hết 5 phút, nhưng thiết bị đang offline nên chưa tắt được.")


async def on_location(lat: float, lng: float):
    if HOME_LAT == 0 and HOME_LNG == 0:
        await tg_text("Chưa cấu hình tọa độ nhà (HOME_LAT, HOME_LNG) trên Render.")
        return
    d = distance_m(lat, lng, HOME_LAT, HOME_LNG)
    prev = STATE["at_home"]
    if d <= HOME_RADIUS_M:
        now_home = True
    elif d > HOME_RADIUS_M + 50:
        now_home = False
    else:
        now_home = prev if prev is not None else False
    STATE["at_home"] = now_home

    if now_home:
        if STATE["leave_pending"]:
            cancel_leave()
            await tg_text("Bạn đã về nhà, mình đã hủy việc tự tắt thiết bị.")
        elif prev is False:
            await tg_text("Chào mừng bạn về nhà.")
        return

    if prev is True and any_device_on() and not STATE["leave_pending"]:
        STATE["leave_pending"] = True
        STATE["leave_task"] = asyncio.create_task(leave_timeout())
        await tg_text(
            f"Bạn đã ra khỏi nhà (cách khoảng {int(d)} m). Đang có thiết bị bật:\n"
            f"{devices_text()}\nBạn có muốn tắt tất cả không? Nếu không trả lời trong 5 phút, mình sẽ tự tắt.",
            LEAVE_KEYBOARD)


# ---------- Xử lý cập nhật từ Telegram ----------
def status_text() -> str:
    online = "online" if DEVICE["ws"] is not None else "offline"
    if STATE["at_home"] is None:
        place = "chưa rõ"
    else:
        place = "ở nhà" if STATE["at_home"] else "ngoài nhà"
    return f"Thiết bị ESP32: {online}\n{devices_text()}\nVị trí: {place}"


async def run_button(data: str):
    if data.startswith("dev:"):
        parts = data.split(":")
        if len(parts) == 3:
            _, name, val = parts
            if name == "ALL":
                ok = await send_all_off()
                if ok:
                    for k in STATE["devices"]:
                        STATE["devices"][k] = False
                text = "Đã tắt tất cả thiết bị."
            else:
                cur = STATE["devices"].get(name, False)
                on = (not cur) if val == "toggle" else (val == "on")
                ok = await send_device(name, on)
                if ok:
                    STATE["devices"][name] = on     # cập nhật ngay, không chờ ESP32
                text = f"Đã {'bật' if on else 'tắt'} {DEVICES[name]['label']}."
            await tg_text(text if ok else OFFLINE_MSG, MENU_KEYBOARD)
        return
    if data == "start":
        ok = await send_raw("CMD:START")
        await tg_text("Đã bật micro, bot đang nghe bạn." if ok else OFFLINE_MSG, MENU_KEYBOARD)
    elif data == "stop":
        ok = await send_raw("CMD:STOP")
        await tg_text("Đã dừng micro." if ok else OFFLINE_MSG, MENU_KEYBOARD)
    elif data == "status":
        await tg_text(status_text(), MENU_KEYBOARD)
    elif data == "list":
        try:
            await tg_text("Lịch: " + await asyncio.to_thread(tool_list_schedules, {}), MENU_KEYBOARD)
        except Exception as e:
            await tg_text(f"Không đọc được lịch: {e}", MENU_KEYBOARD)
    elif data == "bye":
        ws = DEVICE["ws"]
        if ws is not None:
            await speak_and_end(ws, "Tạm biệt chủ nhân.")
            await send_raw("CMD:STANDBY")
            await tg_text("Thiết bị đã chuyển về chế độ chờ.", MENU_KEYBOARD)
        else:
            await tg_text(OFFLINE_MSG, MENU_KEYBOARD)
    elif data == "leave_off":
        cancel_leave()
        ok = await send_all_off()
        if ok:
            for k in STATE["devices"]:
                STATE["devices"][k] = False
        await tg_text("Đã tắt tất cả thiết bị." if ok else OFFLINE_MSG)
    elif data == "leave_keep":
        cancel_leave()
        await tg_text("Đã giữ thiết bị bật.")


async def handle_telegram_update(update: dict):
    if "callback_query" in update:
        cq = update["callback_query"]
        chat = str(cq.get("message", {}).get("chat", {}).get("id", ""))
        if chat != TELEGRAM_CHAT_ID:
            print("Bỏ qua callback: chat ID không khớp", flush=True)
            return
        await asyncio.to_thread(tg_api, "answerCallbackQuery", {"callback_query_id": cq["id"]})
        await run_button(cq.get("data", ""))
        return

    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return
    chat_id = str(msg.get("chat", {}).get("id", ""))
    if chat_id != TELEGRAM_CHAT_ID:
        print(f"Bỏ qua tin: chat {chat_id} không khớp cấu hình", flush=True)
        return

    loc = msg.get("location")
    if loc:
        await on_location(loc["latitude"], loc["longitude"])
        return

    text = (msg.get("text") or "").strip()
    print("Nội dung Telegram:", text, flush=True)
    if not text:
        return
    if text.lower() in ("/start", "/menu", "menu"):
        await tg_text("Chọn thao tác cho bot:", MENU_KEYBOARD)
        return
    if text.lower() == "/lichtest":
        result = await asyncio.to_thread(schedule_selftest)
        await tg_text(result, MENU_KEYBOARD)
        return
    if text.lower() == "/reotest":
        if DEVICE["ws"] is None:
            await tg_text("ESP32 chưa kết nối, không reo được.", MENU_KEYBOARD)
        else:
            await send_raw("CMD:ALARM")
            await tg_text("Đã gửi lệnh reo chuông tới loa.", MENU_KEYBOARD)
        return

    # Hiện tin nhắn Telegram lên trang web của thiết bị
    if DEVICE["ws"] is not None:
        try:
            await DEVICE["ws"].send_text("USER:" + text)
        except Exception:
            pass
    await answer(text, voice=False)


async def safe_handle(update: dict):
    try:
        await handle_telegram_update(update)
    except Exception as e:
        print("Lỗi xử lý Telegram:", e, flush=True)


@app.post("/telegram/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if not TELEGRAM_WEBHOOK_SECRET or secret != TELEGRAM_WEBHOOK_SECRET:
        raise HTTPException(status_code=404)
    update = await request.json()
    asyncio.create_task(safe_handle(update))
    return {"ok": True}


@app.get("/")
def root():
    return {"status": "ok"}


# ---------- WebSocket với ESP32 ----------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    DEVICE["ws"] = websocket
    print("ESP32 đã kết nối", flush=True)
    await websocket.send_text(f"VOL:{STATE['volume']}")

    audio_buffer = bytearray()
    recording = False

    try:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                break

            if message.get("bytes") is not None:
                if recording:
                    audio_buffer.extend(message["bytes"])
                continue

            text = message.get("text")
            if text is None:
                continue
            print("TEXT:", text, flush=True)

            if text == "START_RECORDING":
                audio_buffer.clear()
                recording = True

            elif text == "CANCEL_RECORDING":
                recording = False
                audio_buffer.clear()

            elif text == "END_SESSION":
                recording = False
                audio_buffer.clear()
                stop_music()
                STATE["history"].clear()
                STATE["recent"].clear()
                print("Kết thúc hội thoại, xóa lịch sử", flush=True)

            elif text.startswith("SETVOL:"):
                try:
                    STATE["volume"] = max(0, min(100, int(text[7:])))
                except ValueError:
                    pass

            elif text.startswith("STATE:"):
                parts = text.split(":")
                if len(parts) == 3 and parts[1] in DEVICES:
                    STATE["devices"][parts[1]] = (parts[2] == "ON")

            elif text == "HELLO":
                asyncio.create_task(welcome(websocket))

            elif text == "STOP_RECORDING":
                recording = False
                pcm = bytes(audio_buffer)
                audio_buffer.clear()
                await handle_turn(websocket, pcm)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("Lỗi kết nối:", e, flush=True)
    finally:
        stop_music()
        if DEVICE["ws"] is websocket:
            DEVICE["ws"] = None

    print("ESP32 ngắt kết nối", flush=True)
