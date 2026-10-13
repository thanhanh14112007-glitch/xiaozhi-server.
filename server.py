import asyncio
import io
import os
import wave
from datetime import datetime
from zoneinfo import ZoneInfo

import edge_tts
import miniaudio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from groq import Groq

app = FastAPI()
groq_client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

SAMPLE_RATE = 16000
MIN_BYTES = SAMPLE_RATE * 2 // 2          # tối thiểu 0.5 giây audio (16-bit mono)
STT_MODEL = "whisper-large-v3"
MAX_HISTORY = 10                           # giữ 10 tin nhắn gần nhất (5 lượt)

TTS_VOICE = "vi-VN-HoaiMyNeural"
TTS_CHUNK = 1024                           # bytes mỗi gói (~32 ms audio)

# Model chat theo thứ tự ưu tiên (120b thông minh hơn, Groq vẫn nhanh)
PREFERRED_LLM = [
    "openai/gpt-oss-120b",
    "openai/gpt-oss-20b",
    "qwen/qwen3.8-27b",
    "allam-2-7b",
]
LLM_CANDIDATES = None

BASE_PROMPT = (
    "Bạn là trợ lý giọng nói chạy trên thiết bị ESP32. "
    "Luôn trả lời bằng tiếng Việt, tự nhiên, ngắn gọn, tối đa 3 câu. "
    "Không dùng markdown, emoji hay danh sách. "
    "Bạn không truy cập được internet, lịch cá nhân hay dịch vụ nhạc. "
    "Nếu người dùng hỏi những việc đó, hãy nói thật là bạn chưa có quyền truy cập."
)


def system_prompt() -> str:
    now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%H:%M, ngày %d/%m/%Y")
    return f"{BASE_PROMPT} Thời gian hiện tại ở Việt Nam: {now}."


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


def llm_candidates() -> list:
    global LLM_CANDIDATES
    if LLM_CANDIDATES is None:
        ids = {m.id for m in groq_client.models.list().data}
        LLM_CANDIDATES = [name for name in PREFERRED_LLM if name in ids]
        print("Model chat sẽ dùng (theo thứ tự):", LLM_CANDIDATES, flush=True)
        if not LLM_CANDIDATES:
            LLM_CANDIDATES = None
            raise RuntimeError("Key Groq không có model chat nào trong danh sách ưu tiên")
    return LLM_CANDIDATES


def ask_llm(history: list, user_text: str) -> str:
    messages = [{"role": "system", "content": system_prompt()}]
    messages += history
    messages.append({"role": "user", "content": user_text})

    last_err = None
    for model in llm_candidates():
        try:
            resp = groq_client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=200,
                temperature=0.7,
            )
            print("Dùng model:", model, flush=True)
            return (resp.choices[0].message.content or "").strip()
        except Exception as e:
            print(f"Model {model} lỗi, thử model khác: {e}", flush=True)
            last_err = e
    raise last_err


async def text_to_speech_pcm(text: str) -> bytes:
    """Đổi văn bản thành PCM 16-bit, mono, 16 kHz."""
    communicate = edge_tts.Communicate(text, TTS_VOICE)
    mp3 = bytearray()
    async for chunk in communicate.stream():
        if chunk["type"] == "audio":
            mp3.extend(chunk["data"])
    if not mp3:
        raise RuntimeError("TTS không trả về audio")

    decoded = miniaudio.decode(
        bytes(mp3),
        output_format=miniaudio.SampleFormat.SIGNED16,
        nchannels=1,
        sample_rate=SAMPLE_RATE,
    )
    return decoded.samples.tobytes()


async def send_audio(ws: WebSocket, pcm: bytes):
    """Gửi audio theo nhịp thời lượng thực."""
    sent = 0
    for i in range(0, len(pcm), TTS_CHUNK):
        chunk = pcm[i:i + TTS_CHUNK]
        await ws.send_bytes(chunk)
        sent += len(chunk)
        await asyncio.sleep(len(chunk) / (SAMPLE_RATE * 2))
    print(f"Đã gửi xong audio: {sent}/{len(pcm)} bytes", flush=True)


async def handle_turn(ws: WebSocket, pcm: bytes, history: list):
    """Xử lý một lượt nói: nghe -> hiểu -> trả lời -> đọc. Luôn kết thúc bằng TTS_END."""
    speak = False
    if len(pcm) < MIN_BYTES:
        reply = "Âm thanh quá ngắn, hãy nói lâu hơn."
    else:
        try:
            user_text = await asyncio.to_thread(speech_to_text, pcm)
            print("Người dùng nói:", user_text, flush=True)
            if not user_text.strip():
                reply = "Mình không nghe rõ, bạn nói lại nhé."
            else:
                reply = await asyncio.to_thread(ask_llm, history, user_text)
                history.append({"role": "user", "content": user_text})
                history.append({"role": "assistant", "content": reply})
                del history[:-MAX_HISTORY]
                speak = True
        except Exception as e:
            reply = f"Lỗi: {e}"
            print(reply, flush=True)

    print("Trả lời:", reply, flush=True)
    await ws.send_text(reply)

    if speak:
        try:
            pcm_out = await text_to_speech_pcm(reply)
            print(f"TTS: {len(pcm_out)} bytes, đang phát", flush=True)
            await send_audio(ws, pcm_out)
        except Exception as e:
            print("Lỗi TTS:", e, flush=True)

    await ws.send_text("TTS_END")


@app.get("/")
def root():
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("ESP32 đã kết nối", flush=True)

    audio_buffer = bytearray()
    recording = False
    history: list = []

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
                history.clear()
                print("Kết thúc hội thoại, xóa lịch sử", flush=True)

            elif text == "STOP_RECORDING":
                recording = False
                pcm = bytes(audio_buffer)
                audio_buffer.clear()
                print(f"Nhận {len(pcm)} bytes, bắt đầu xử lý", flush=True)
                await handle_turn(websocket, pcm, history)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("Lỗi kết nối:", e, flush=True)

    print("ESP32 ngắt kết nối", flush=True)
