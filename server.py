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
WELCOME_MUSIC_URL = os.environ.get("WELCOME_MUSIC_URL", "")

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
SAMPLE_RATE = 16000
MIN_BYTES = SAMPLE_RATE * 2 // 2
STT_MODEL = "whisper-large-v3"
MAX_HISTORY = 10
LEAVE_TIMEOUT_S = 300
SCHEDULE_KEY = "xiaozhi:schedules"

TTS_VOICE = "vi-VN-HoaiMyNeural"
TTS_CHUNK = 1024
TTS_PEAK = int(0.6 * 32767)
MUSIC_PEAK = int(0.35 * 32767)
DEFAULT_VOLUME = 70

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

# Trạng thái toàn hệ thống (một thiết bị)
STATE = {
    "volume": DEFAULT_VOLUME,
    "recent": [],
    "history": [],
    "music_task": None,
    "device_on": False,
    "at_home": None,
    "leave_pending": False,
    "leave_task": None,
}
DEVICE = {"ws": None}
SCHED_LOCK = threading.Lock()

MENU_KEYBOARD = {"inline_keyboard": [
    [{"text": "Bật thiết bị", "callback_data": "dev_on"},
     {"text": "Tắt thiết bị", "callback_data": "dev_off"}],
    [{"text": "Bật micro", "callback_data": "start"},
     {"text": "Dừng micro", "callback_data": "stop"}],
    [{"text": "Trạng thái", "callback_data": "status"},
     {"text": "Xem lịch", "callback_data": "list"}],
    [{"text": "Tạm biệt (chờ)", "callback_data": "bye"}],
]}
LEAVE_KEYBOARD = {"inline_keyboard": [
    [{"text": "Tắt thiết bị", "callback_data": "leave_off"},
     {"text": "Giữ bật", "callback_data": "leave_keep"}],
]}

BASE_PROMPT = (
    "Bạn là trợ lý giọng nói chạy trên thiết bị ESP32, người dùng nghe câu trả lời qua loa. "
    "Luôn trả lời bằng tiếng Việt, tự nhiên, ngắn gọn, tối đa 3 câu. "
    "Không dùng markdown, emoji, danh sách hay đường link. "
    "Bạn có các công cụ: xem giờ, tìm kiếm web, xem thời tiết, chỉnh âm lượng, phát nhạc, "
    "gửi tin nhắn Telegram, bật hoặc tắt thiết bị (đèn, quạt), đặt lịch hẹn, xem và hủy lịch, "
    "và chuyển về chế độ chờ. "
    "Khi người dùng nói bật hoặc tắt đèn, quạt hoặc thiết bị, gọi device_control. "
    "Khi người dùng nói tạm biệt hoặc tắt bot, gọi shutdown_bot. "
    "Khi người dùng đặt hẹn hoặc nhắc việc theo giờ, gọi add_schedule với when dạng HH:MM hoặc YYYY-MM-DD HH:MM theo giờ Việt Nam, "
    "và xác nhận lại rõ thời gian. "
    "Khi cần thông tin mới (thời tiết, tin tức, giá cả, sự kiện) hãy gọi công cụ trước khi trả lời. "
    "Khi người dùng nhờ phát nhạc hoặc hát, gọi play_music. Bạn không tự hát được bài mới, chỉ phát được bản thu có sẵn. "
    "Bạn chưa có quyền truy cập lịch Google, email hay thiết bị khác; nếu được hỏi, hãy nói thật."
)


def system_prompt() -> str:
    now = datetime.now(TZ).strftime("%H:%M, ngày %d/%m/%Y")
    return f"{BASE_PROMPT} Thời gian hiện tại ở Việt Nam: {now}."


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
