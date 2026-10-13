import asyncio
import io
import json
import os
import re
import urllib.parse
import urllib.request
import wave
from array import array
from datetime import datetime
from difflib import SequenceMatcher
from zoneinfo import ZoneInfo

import edge_tts
import miniaudio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from groq import Groq

app = FastAPI()
groq_client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY")        # tùy chọn
JAMENDO_CLIENT_ID = os.environ.get("JAMENDO_CLIENT_ID")  # tùy chọn

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
SAMPLE_RATE = 16000
MIN_BYTES = SAMPLE_RATE * 2 // 2
STT_MODEL = "whisper-large-v3"
MAX_HISTORY = 10

TTS_VOICE = "vi-VN-HoaiMyNeural"
TTS_CHUNK = 1024                       # bytes mỗi gói (~32 ms audio)
TTS_PEAK = int(0.6 * 32767)            # mức đỉnh giọng nói, tránh rè
MUSIC_PEAK = int(0.35 * 32767)         # nhạc nhỏ hơn giọng nói
DEFAULT_VOLUME = 70

PREFERRED_LLM = [
    "openai/gpt-oss-120b",
    "openai/gpt-oss-20b",
    "qwen/qwen3.8-27b",
    "allam-2-7b",
]
LLM_CANDIDATES = None

# Kho nhạc riêng: tên bài -> link mp3 trực tiếp (ưu tiên cao nhất)
MY_SONGS = {
    # "xuân này con không về": "https://raw.githubusercontent.com/USER/REPO/main/music/xuan-nay-con-khong-ve.mp3",
}

# Internet Archive
ARCHIVE_SEARCH_URL = "https://archive.org/advancedsearch.php"
ARCHIVE_MIN_BYTES = 1 * 1024 * 1024    # bỏ file quá nhỏ (thường là đoạn trích)
ARCHIVE_MAX_BYTES = 15 * 1024 * 1024   # bỏ file quá lớn

BASE_PROMPT = (
    "Bạn là trợ lý giọng nói chạy trên thiết bị ESP32, người dùng nghe câu trả lời qua loa. "
    "Luôn trả lời bằng tiếng Việt, tự nhiên, ngắn gọn, tối đa 3 câu. "
    "Không dùng markdown, emoji, danh sách hay đường link. "
    "Bạn có các công cụ: xem giờ, tìm kiếm web, xem thời tiết, chỉnh âm lượng và phát nhạc. "
    "Khi cần thông tin mới (thời tiết, tin tức, giá cả, sự kiện) hãy gọi công cụ trước khi trả lời. "
    "Khi người dùng nhờ phát nhạc hoặc hát, gọi play_music. Bạn không tự hát được bài mới, chỉ phát được bản thu có sẵn. "
    "Bạn chưa có quyền truy cập lịch cá nhân, email hay thiết bị khác; nếu được hỏi, hãy nói thật."
)


def system_prompt() -> str:
    now = datetime.now(TZ).strftime("%H:%M, ngày %d/%m/%Y")
    return f"{BASE_PROMPT} Thời gian hiện tại ở Việt Nam: {now}."


def normalize(s: str) -> str:
    return re.sub(r"[^\w\s]", "", s.lower()).strip()


# ---------- Tiện ích mạng ----------
def http_json(url: str, data=None, timeout: int = 10):
    body = json.dumps(data).encode("utf-8") if data is not None else None
    req = urllib.request.Request(url, data=body, headers={"User-Agent": "xiaozhi-bot/1.0"})
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


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
        parts = [f"{r.get('title', '')}: {r.get('content', '')}"
                 for r in data.get("results", [])]
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


# ---------- Nhạc: Internet Archive ----------
def archive_search_track(query: str):
    """Tìm bài nhạc trên Internet Archive, trả về dict hoặc None."""
    clean = re.sub(r"[^\w\s]", " ", query, flags=re.UNICODE).strip()
    if not clean:
        return None

    q = f"({clean}) AND mediatype:audio AND collection:opensource_audio"
    params = urllib.parse.urlencode([
        ("q", q),
        ("fl[]", "identifier"), ("fl[]", "title"), ("fl[]", "creator"),
        ("rows", "5"), ("output", "json"),
    ])
    data = http_json(f"{ARCHIVE_SEARCH_URL}?{params}", timeout=20)
    docs = data.get("response", {}).get("docs", [])

    for doc in docs:
        ident = doc.get("identifier")
        if not ident:
            continue
        meta = http_json(f"https://archive.org/metadata/{ident}", timeout=20)
        files = meta.get("files", [])

        mp3s = []
        for f in files:
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


# ---------- Nhạc: công cụ phát nhạc ----------
def tool_play_music(ctx, query: str) -> str:
    q = normalize(query)

    # 1. Kho nhạc riêng
    for title, url in MY_SONGS.items():
        t = normalize(title)
        if q and (q in t or t in q):
            ctx["music"] = {"url": url, "title": title, "artist": "", "license": "kho riêng"}
            return f"Sẽ phát bài {title}"

    # 2. Internet Archive
    try:
        track = archive_search_track(query)
    except Exception as e:
        print("Lỗi Internet Archive:", e, flush=True)
        track = None
    if track:
        ctx["music"] = track
        return f"Sẽ phát bài {track['title']} của {track['artist'] or 'không rõ'}"

    # 3. Jamendo (dự phòng)
    if not JAMENDO_CLIENT_ID:
        return "Không tìm thấy bài này"
    url = "https://api.jamendo.com/v3.0/tracks/?" + urllib.parse.urlencode({
        "client_id": JAMENDO_CLIENT_ID, "format": "json", "limit": 1,
        "search": query, "audioformat": "mp32",
    })
    data = http_json(url, timeout=15)
    res = data.get("results") or []
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
}

TOOLS = [
    {"type": "function", "function": {
        "name": "get_time",
        "description": "Lấy ngày và giờ hiện tại ở Việt Nam",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {
        "name": "web_search",
        "description": "Tìm thông tin mới trên web",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string", "description": "từ khóa tìm kiếm"}},
            "required": ["query"]}}},
    {"type": "function", "function": {
        "name": "get_weather",
        "description": "Lấy thời tiết hiện tại của một thành phố",
        "parameters": {"type": "object", "properties": {
            "city": {"type": "string"}}, "required": ["city"]}}},
    {"type": "function", "function": {
        "name": "set_volume",
        "description": "Chỉnh âm lượng loa. Dùng level hoặc change.",
        "parameters": {"type": "object", "properties": {
            "level": {"type": "integer", "minimum": 0, "maximum": 100},
            "change": {"type": "string", "enum": ["up", "down", "mute", "max"]}}}}},
    {"type": "function", "function": {
        "name": "play_music",
        "description": "Tìm và phát một bài nhạc có sẵn theo tên bài, ca sĩ hoặc thể loại",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"}}, "required": ["query"]}}},
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


def run_agent(history: list, user_text: str, ctx: dict) -> str:
    messages = ([{"role": "system", "content": system_prompt()}]
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


# ---------- Nhận dạng giọng nói ----------
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
        file=("audio.wav", pcm_to_wav(pcm)),
        model=STT_MODEL,
        language="vi",
    )
    return result.text or ""


# ---------- Chống nhận lại tiếng loa ----------
def is_echo(text: str, recent: list) -> bool:
    t = normalize(text)
    if len(t) < 6:
        return False
    for r in recent:
        rr = normalize(r)
        if rr and (t in rr or SequenceMatcher(None, t, rr).ratio() > 0.6):
            return True
    return False


# ---------- Âm thanh ----------
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
    sent = 0
    for i in range(0, len(pcm), TTS_CHUNK):
        chunk = pcm[i:i + TTS_CHUNK]
        await ws.send_bytes(chunk)
        sent += len(chunk)
        await asyncio.sleep(len(chunk) / (SAMPLE_RATE * 2))
    print(f"Đã gửi xong audio: {sent}/{len(pcm)} bytes", flush=True)


async def play_music(ws: WebSocket, state: dict, track: dict):
    """Phát nhạc nền. Luôn kết thúc bằng TTS_END để ESP32 quay lại chế độ nghe."""
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
        state["music_task"] = None
        try:
            await ws.send_text("TTS_END")
        except Exception:
            pass


# ---------- Một lượt hội thoại ----------
async def handle_turn(ws: WebSocket, pcm: bytes, state: dict):
    ctx = {"volume": state["volume"], "music": None}
    music_started = False
    try:
        if len(pcm) < MIN_BYTES:
            print("Âm thanh quá ngắn, bỏ qua", flush=True)
            return

        user_text = (await asyncio.to_thread(speech_to_text, pcm)).strip()
        print("Người dùng nói:", user_text, flush=True)
        if len(normalize(user_text)) < 2:
            return
        if is_echo(user_text, state["recent"]):
            print("Bỏ qua: tiếng vọng từ loa", flush=True)
            return

        try:
            reply = await asyncio.to_thread(run_agent, state["history"], user_text, ctx)
            history = state["history"]
            history.append({"role": "user", "content": user_text})
            history.append({"role": "assistant", "content": reply})
            del history[:-MAX_HISTORY]
        except Exception as e:
            print("Lỗi AI:", e, flush=True)
            reply = f"Lỗi: {e}"

        if ctx["volume"] != state["volume"]:
            state["volume"] = ctx["volume"]
            await ws.send_text(f"VOL:{state['volume']}")

        if ctx["music"] and not reply:
            reply = f"Đang phát {ctx['music']['title']}."
        if not reply:
            return

        state["recent"] = (state["recent"] + [reply])[-3:]
        print("Trả lời:", reply, flush=True)
        await ws.send_text(reply)

        try:
            pcm_out = await text_to_speech_pcm(reply)
            print(f"TTS: {len(pcm_out)} bytes, đang phát", flush=True)
            await send_audio(ws, pcm_out)
        except Exception as e:
            print("Lỗi TTS:", e, flush=True)

        if ctx["music"]:
            music_started = True
            state["music_task"] = asyncio.create_task(play_music(ws, state, ctx["music"]))
    finally:
        if not music_started:
            try:
                await ws.send_text("TTS_END")
            except Exception:
                pass


@app.get("/")
def root():
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("ESP32 đã kết nối", flush=True)

    state = {"volume": DEFAULT_VOLUME, "recent": [], "history": [], "music_task": None}
    await websocket.send_text(f"VOL:{state['volume']}")

    audio_buffer = bytearray()
    recording = False

    def stop_music():
        task = state.get("music_task")
        if task and not task.done():
            task.cancel()
        state["music_task"] = None

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
                state["history"].clear()
                state["recent"].clear()
                print("Kết thúc hội thoại, xóa lịch sử", flush=True)

            elif text == "STOP_RECORDING":
                recording = False
                pcm = bytes(audio_buffer)
                audio_buffer.clear()
                await handle_turn(websocket, pcm, state)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("Lỗi kết nối:", e, flush=True)
    finally:
        stop_music()

    print("ESP32 ngắt kết nối", flush=True)
