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

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
SAMPLE_RATE = 16000
MIN_BYTES = SAMPLE_RATE * 2 // 2
STT_MODEL = "whisper-large-v3"
MAX_HISTORY = 10
LEAVE_TIMEOUT_S = 300
SCHEDULE_KEY = "xiaozhi:schedules"
MEMORY_KEY = "xiaozhi:memory"
MAX_MEMORY = 100
MEMORY_IN_PROMPT = 40
OFFLINE_MSG = "Thiết bị đang offline, kiểm tra ESP32 đã bật và có WiFi."

ALARM_SECONDS = 5.0                    # phải khớp ALARM_MS trên ESP32
ACK_REPEAT_S = 120                     # nhắc lại mỗi 2 phút nếu chưa xác nhận
MAX_ACK_REPEAT = 15                    # nhắc tối đa 15 lần rồi tạm dừng lịch
OFFLINE_RETRY_S = 60                   # lịch điều khiển khi ESP32 offline: thử lại mỗi phút
MAX_OFFLINE_TRIES = 30
LISTEN_AFTER_REMIND_S = 4.0            # chờ loa đọc xong rồi mới mở micro để nghe "đã nhớ"

TTS_VOICE = "vi-VN-HoaiMyNeural"
TTS_CHUNK = 4096
SEND_AHEAD_S = 1.5
TTS_PEAK = int(0.6 * 32767)
MUSIC_PEAK = int(0.35 * 32767)
DEFAULT_VOLUME = 70

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
    "devices": {k: False for k in DEVICES},
    "at_home": None,
    "leave_pending": False,
    "leave_task": None,
}
DEVICE = {"ws": None}
SCHED_LOCK = threading.Lock()
MEM_LOCK = threading.Lock()


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
]}  # đánh dấu: tg_text sẽ dựng menu mới theo trạng thái

LEAVE_KEYBOARD = {"inline_keyboard": [
    [{"text": "Tắt tất cả", "callback_data": "leave_off"},
     {"text": "Giữ bật", "callback_data": "leave_keep"}],
]}

ACK_KEYBOARD = {"inline_keyboard": [
    [{"text": "Đã nhớ", "callback_data": "ack"}],
]}

DEVICE_RULES = (
    "Thiết bị có thể điều khiển: đèn (đỏ), máy lạnh (xanh lá), tivi (xanh dương). "
    "Khi người dùng nói bật hoặc tắt một thiết bị, gọi device_control với device và action. "
    "Khi người dùng nói tắt tất cả, gọi device_control với device là tatca và action là off. "
    "Khi người dùng đặt hẹn hoặc nhắc việc theo giờ, gọi add_schedule. "
    "Khi người dùng nhờ ghi nhớ điều gì (ví dụ 'nhớ giúp tôi', 'hãy nhớ'), gọi remember với note là nội dung cần nhớ, "
    "rồi trả lời ngắn gọn 'Ok, đã nhớ.' "
    "Khi người dùng nhờ quên điều gì, gọi forget_memory với keyword là từ khóa liên quan. "
    "Khi người dùng hỏi về điều bạn đã nhớ, trả lời dựa trên danh sách ghi nhớ. "
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


def mem_prompt() -> str:
    try:
        items = mem_read()
    except Exception:
        return ""
    if not items:
        return ""
    recent = items[-MEMORY_IN_PROMPT:]
    return "Những điều bạn đã ghi nhớ về chủ nhân: " + " | ".join(i["text"] for i in recent) + "."


def system_prompt(mode: str = "voice") -> str:
    now = datetime.now(TZ).strftime("%H:%M, ngày %d/%m/%Y")
    base = TEXT_PROMPT if mode == "text" else VOICE_PROMPT
    status = "Trạng thái thiết bị hiện tại: " + "; ".join(
        f"{v['label']} {'đang bật' if STATE['devices'].get(k) else 'đang tắt'}"
        for k, v in DEVICES.items())
    mem = mem_prompt()
    return f"{base} {DEVICE_RULES} {status}. {mem} Thời gian hiện tại ở Việt Nam: {now}."


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


# ---------- Upstash: lưu lịch và ghi nhớ bền vững ----------
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


def load_json(key: str) -> list:
    raw = redis_cmd("GET", key)
    return json.loads(raw) if raw else []


def save_json(key: str, items: list):
    redis_cmd("SET", key, json.dumps(items, ensure_ascii=False))


def sched_read():
    with SCHED_LOCK:
        return load_json(SCHEDULE_KEY)


def sched_add(item):
    with SCHED_LOCK:
        items = load_json(SCHEDULE_KEY)
        items.append(item)
        save_json(SCHEDULE_KEY, items)


def sched_update(item):
    with SCHED_LOCK:
        items = load_json(SCHEDULE_KEY)
        items = [item if i["id"] == item["id"] else i for i in items]
        save_json(SCHEDULE_KEY, items)


def sched_remove(ids):
    with SCHED_LOCK:
        items = load_json(SCHEDULE_KEY)
        save_json(SCHEDULE_KEY, [i for i in items if i["id"] not in ids])


def sched_cancel(sid: str) -> bool:
    with SCHED_LOCK:
        items = load_json(SCHEDULE_KEY)
        new = [i for i in items if i["id"] != sid]
        if len(new) == len(items):
            return False
        save_json(SCHEDULE_KEY, new)
        return True


def has_pending() -> bool:
    """Có lịch nào đã nhắc và đang chờ xác nhận 'đã nhớ' không."""
    try:
        return any(i.get("fired") for i in sched_read())
    except Exception as e:
        print("Lỗi kiểm tra lịch chờ:", e, flush=True)
        return False


def ack_all_pending() -> int:
    """Đã nhớ: xóa hẳn các lịch đã nhắc khỏi Upstash. Trả về số lịch đã xóa."""
    with SCHED_LOCK:
        items = load_json(SCHEDULE_KEY)
        done = [i for i in items if i.get("fired")]
        if not done:
            return 0
        keep = [i for i in items if not i.get("fired")]
        save_json(SCHEDULE_KEY, keep)
    for i in done:
        print("Đã xóa lịch trên Upstash:", i["id"], i.get("action"), flush=True)
    return len(done)


def is_ack_text(text: str) -> bool:
    t = normalize(text)
    keys = ["đã nhớ", "nhớ rồi", "đã biết", "biết rồi", "rõ rồi", "đã rõ", "ghi nhớ rồi", "ok nhớ"]
    if any(k in t for k in keys):
        return True
    return t in ("ok", "oke", "ừ", "rồi", "được", "ok rồi")


def mem_read():
    return load_json(MEMORY_KEY)


def mem_add(text: str) -> dict:
    with MEM_LOCK:
        items = load_json(MEMORY_KEY)
        item = {"id": uuid.uuid4().hex[:6], "text": text,
                "at": datetime.now(TZ).isoformat()}
        items.append(item)
        save_json(MEMORY_KEY, items[-MAX_MEMORY:])
        return item


def mem_forget(keyword: str) -> int:
    kw = normalize(keyword)
    if not kw:
        return 0
    with MEM_LOCK:
        items = load_json(MEMORY_KEY)
        keep = [i for i in items if kw not in normalize(i["text"])]
        removed = len(items) - len(keep)
        if removed:
            save_json(MEMORY_KEY, keep)
        return removed


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


async def confirm_done() -> str:
    """Đã nhớ: dừng chuông trên loa rồi xóa lịch đã nhắc khỏi Upstash.
    Trả về thông báo để đọc/hiển thị."""
    await send_raw("CMD:ALARM_STOP")
    try:
        n = await asyncio.to_thread(ack_all_pending)
    except Exception as e:
        print("Lỗi xóa lịch khi đã nhớ:", e, flush=True)
        return f"Không xóa được lịch trên Upstash: {e}"
    return ("Ok, mình đã ghi nhận. Lịch hẹn đã xóa khỏi Upstash." if n
            else "Hiện không có lịch nào đang chờ xác nhận.")


async def open_mic_later(delay: float):
    """Sau khi loa đọc xong lời nhắc, mở micro để nghe câu 'đã nhớ'."""
    await asyncio.sleep(delay)
    if DEVICE["ws"] is not None:
        await send_raw("CMD:START")


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
            "device": device, "note": note[:200], "tries": 0, "fired": False}
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
        if it.get("fired"):
            what += " (chờ xác nhận)"
        lines.append(f"{it['id']}: {t.strftime('%H:%M %d/%m')} {what}")
    return "; ".join(lines)


def tool_cancel_schedule(ctx, schedule_id: str) -> str:
    return "Đã hủy lịch" if sched_cancel(schedule_id) else "Không tìm thấy mã lịch này"


def tool_remember(ctx, note: str) -> str:
    note = (note or "").strip()[:200]
    if not note:
        return "Nội dung cần nhớ đang trống"
    try:
        mem_add(note)
    except Exception as e:
        print("Lỗi ghi nhớ:", e, flush=True)
        return f"Không lưu được ghi nhớ: {e}"
    return f"Đã nhớ: {note}"


def tool_forget_memory(ctx, keyword: str) -> str:
    try:
        n = mem_forget(keyword)
    except Exception as e:
        return f"Không xóa được: {e}"
    return f"Đã quên {n} điều liên quan" if n else "Không tìm thấy điều nào để quên"


def tool_list_memory(ctx) -> str:
    try:
        items = mem_read()
    except Exception as e:
        return f"Không đọc được ghi nhớ: {e}"
    if not items:
        return "Hiện chưa nhớ điều gì"
    return "; ".join(f"{i['id']}: {i['text']}" for i in items)


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
    "remember": tool_remember,
    "forget_memory": tool_forget_memory,
    "list_memory": tool_list_memory,
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
        "description": "Đặt lịch bật hoặc tắt một thiết bị, hoặc nhắc việc theo giờ. Với remind, note là nội dung cần nhắc.",
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
    {"type": "function", "function": {"name": "remember",
        "description": "Ghi nhớ một điều người dùng nhờ nhớ, ví dụ sở thích, thông tin cá nhân, việc hay làm",
        "parameters": {"type": "object", "properties": {
            "note": {"type": "string", "description": "nội dung cần nhớ, viết ngắn gọn"}},
            "required": ["note"]}}},
    {"type": "function", "function": {"name": "forget_memory",
        "description": "Quên các điều đã nhớ có chứa từ khóa",
        "parameters": {"type": "object", "properties": {
            "keyword": {"type": "string"}}, "required": ["keyword"]}}},
    {"type": "function", "function": {"name": "list_memory",
        "description": "Liệt kê những điều đã ghi nhớ",
        "parameters": {"type": "object", "properties": {}}}},
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


def pcm_peak(pcm: bytes) -> int:
    samples = array("h")
    samples.frombytes(pcm[: len(pcm) - (len(pcm) % 2)])
    return max((abs(x) for x in samples), default=0)


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
    pcm = await asyncio.to_thread(normalize_pcm, decoded.samples.tobytes(), TTS_PEAK)
    peak = pcm_peak(pcm)
    print(f"TTS: mp3 {len(mp3)} bytes, pcm {len(pcm)} bytes, peak {peak}", flush=True)
    if peak == 0:
        raise RuntimeError("Âm thanh TTS bị trống (peak = 0)")
    return pcm


def download_track_pcm(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "xiaozhi-bot/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        mp3 = r.read()
    decoded = miniaudio.decode(mp3, output_format=miniaudio.SampleFormat.SIGNED16,
                               nchannels=1, sample_rate=SAMPLE_RATE)
    return normalize_pcm(decoded.samples.tobytes(), MUSIC_PEAK)


async def send_audio(ws: WebSocket, pcm: bytes):
    print(f"Chuẩn bị gửi audio: {len(pcm)} bytes, peak toàn câu = {pcm_peak(pcm)}", flush=True)
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
        keyboard = build_menu()
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
    ended = False
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

        # Đang có lời nhắc chờ xác nhận và người dùng nói "đã nhớ"
        if is_ack_text(user_text) and await asyncio.to_thread(has_pending):
            reply = await confirm_done()
            await speak_and_end(ws, reply)
            ended = True
            return

        ended = await answer(user_text, voice=True)
    except Exception as e:
        print("Lỗi lượt nói:", e, flush=True)
    finally:
        if not ended:
            try:
                await ws.send_text("TTS_END")
            except Exception:
                pass


# ---------- Lịch hẹn ----------
async def ring_alarm() -> bool:
    if DEVICE["ws"] is None:
        return False
    await send_raw("CMD:ALARM")
    await asyncio.sleep(ALARM_SECONDS + 0.5)
    return True


async def run_schedule(item: dict, first: bool) -> bool:
    """
    False chỉ khi lần đầu chạy lịch điều khiển thiết bị mà ESP32 offline (để thử lại).
    Chuông chỉ reo ở lần đầu. Các lần nhắc lại chỉ đọc lời nhắc.
    """
    action = item["action"]
    note = (item.get("note") or "").strip()

    if action in ("device_on", "device_off"):
        name = item.get("device", "")
        on = action == "device_on"
        label = DEVICES.get(name, {}).get("label", name)
        verb = "bật" if on else "tắt"
        if first and DEVICE["ws"] is None:
            return False
        if first:
            await ring_alarm()
            ok = await send_device(name, on)
            if ok:
                STATE["devices"][name] = on
            base = (f"Đã {verb} {label} theo lịch." if ok
                    else f"Không {verb} được {label} vì thiết bị offline.")
        else:
            base = f"Nhắc lại: lịch {verb} {label} đã thực hiện."
    else:
        if first:
            base = f"Đến giờ rồi, bạn nhớ: {note}" if note else "Đến giờ rồi."
            await ring_alarm()
        else:
            base = f"Nhắc lại: bạn nhớ {note}" if note else "Nhắc lại việc đã hẹn."

    text = base + " Bạn đã nhớ chưa? Nói đã nhớ hoặc bấm nút để kết thúc."
    await tg_text("🔔 " + text, ACK_KEYBOARD)
    if DEVICE["ws"] is not None:
        await speak_and_end(DEVICE["ws"], text)
        # Mở micro sau khi loa đọc xong để nghe câu "đã nhớ"
        asyncio.create_task(open_mic_later(LISTEN_AFTER_REMIND_S))
    return True


async def scheduler_loop():
    print("Bộ hẹn giờ đã khởi động", flush=True)
    while True:
        await asyncio.sleep(20)
        if not upstash_ready():
            continue
        try:
            items = await asyncio.to_thread(sched_read)
        except Exception as e:
            print("Lỗi đọc lịch:", e, flush=True)
            continue
        now = datetime.now(TZ)
        due = [i for i in items if datetime.fromisoformat(i["at"]) <= now]
        for it in due:
            first = not it.get("fired", False)
            print("Lịch đến hạn:", it["id"], it.get("action"),
                  "lần đầu" if first else "nhắc lại", flush=True)
            try:
                ok = await run_schedule(it, first)
            except Exception as e:
                print("Lỗi chạy lịch:", e, flush=True)
                ok = True

            try:
                if not ok:
                    it["tries"] = it.get("tries", 0) + 1
                    if it["tries"] >= MAX_OFFLINE_TRIES:
                        await asyncio.to_thread(sched_remove, [it["id"]])
                        print("Bỏ lịch do ESP32 offline quá lâu:", it["id"], flush=True)
                    else:
                        it["at"] = (now + timedelta(seconds=OFFLINE_RETRY_S)).isoformat()
                        await asyncio.to_thread(sched_update, it)
                    continue

                if first:
                    it["fired"] = True
                    it["tries"] = 0
                    it["at"] = (now + timedelta(seconds=ACK_REPEAT_S)).isoformat()
                    await asyncio.to_thread(sched_update, it)
                else:
                    it["tries"] = it.get("tries", 0) + 1
                    if it["tries"] >= MAX_ACK_REPEAT:
                        await asyncio.to_thread(sched_remove, [it["id"]])
                        await tg_text("Mình đã nhắc nhiều lần nhưng chưa thấy xác nhận, tạm dừng lịch này.")
                    else:
                        it["at"] = (now + timedelta(seconds=ACK_REPEAT_S)).isoformat()
                        await asyncio.to_thread(sched_update, it)
            except Exception as e:
                print("Lỗi cập nhật lịch:", e, flush=True)


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
                    STATE["devices"][name] = on
                text = f"Đã {'bật' if on else 'tắt'} {DEVICES[name]['label']}."
            await tg_text(text if ok else OFFLINE_MSG, MENU_KEYBOARD)
        return
    if data == "ack":
        text = await confirm_done()
        await tg_text(text, MENU_KEYBOARD)
        if DEVICE["ws"] is not None and text.startswith("Ok"):
            await speak_and_end(DEVICE["ws"], "Ok, mình đã ghi nhận.")
    elif data == "start":
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
    low = text.lower()
    if low in ("/start", "/menu", "menu"):
        await tg_text("Chọn thao tác cho bot:", MENU_KEYBOARD)
        return
    if low == "/lichtest":
        result = await asyncio.to_thread(schedule_selftest)
        await tg_text(result, MENU_KEYBOARD)
        return
    if low == "/reotest":
        if DEVICE["ws"] is None:
            await tg_text("ESP32 chưa kết nối, không reo được.", MENU_KEYBOARD)
        else:
            await send_raw("CMD:ALARM")
            await tg_text("Đã gửi lệnh reo chuông tới loa.", MENU_KEYBOARD)
        return
    if low == "/stopalarm":
        ok = await send_raw("CMD:ALARM_STOP")
        await tg_text("Đã tắt chuông." if ok else OFFLINE_MSG, MENU_KEYBOARD)
        return
    if low == "/nho":
        try:
            result = await asyncio.to_thread(tool_list_memory, {})
        except Exception as e:
            result = f"Không đọc được ghi nhớ: {e}"
        await tg_text("Mình đang nhớ:\n" + result, MENU_KEYBOARD)
        return
    if low.startswith("/quen "):
        try:
            n = await asyncio.to_thread(mem_forget, text[6:])
            result = f"Đã quên {n} điều liên quan." if n else "Không tìm thấy điều nào để quên."
        except Exception as e:
            result = f"Không xóa được: {e}"
        await tg_text(result, MENU_KEYBOARD)
        return

    # Đang có lời nhắc chờ xác nhận và người dùng nhắn "đã nhớ"
    if is_ack_text(text) and await asyncio.to_thread(has_pending):
        reply = await confirm_done()
        await tg_text(reply, MENU_KEYBOARD)
        if DEVICE["ws"] is not None and reply.startswith("Ok"):
            await speak_and_end(DEVICE["ws"], "Ok, mình đã ghi nhận.")
        return

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
                print("Kết thúc hội thoại, xóa lịch sử tạm (ghi nhớ vẫn giữ)", flush=True)

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
                print("ESP32 chào: phát lời chào", flush=True)
                asyncio.create_task(speak_and_end(websocket, "Kính chào chủ nhân."))

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
