import asyncio
import io
import os
import wave

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from groq import Groq

app = FastAPI()
groq_client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

SAMPLE_RATE = 16000
MIN_BYTES = SAMPLE_RATE * 2 // 2          # tối thiểu 0.5 giây audio (16-bit mono)
STT_MODEL = "whisper-large-v3"
MAX_HISTORY = 10                           # giữ 10 tin nhắn gần nhất (5 lượt hỏi đáp)

# Model chat theo thứ tự ưu tiên. Server chỉ dùng model mà key có trong danh sách.
PREFERRED_LLM = [
    "openai/gpt-oss-20b",
    "openai/gpt-oss-120b",
    "qwen/qwen3.8-27b",
    "allam-2-7b",
]
LLM_CANDIDATES = None                      # cache danh sách model khả dụng

SYSTEM_PROMPT = (
    "Bạn là trợ lý giọng nói chạy trên thiết bị ESP32. "
    "Luôn trả lời bằng tiếng Việt, ngắn gọn, tối đa 2 đến 3 câu. "
    "Không dùng markdown, emoji hay danh sách."
)


def pcm_to_wav(pcm: bytes) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(pcm)
    return buf.getvalue()


def speech_to_text(pcm: bytes) -> str:
    wav = pcm_to_wav(pcm)
    result = groq_client.audio.transcriptions.create(
        file=("audio.wav", wav),
        model=STT_MODEL,
        language="vi",
    )
    return result.text or ""


def llm_candidates() -> list:
    global LLM_CANDIDATES
    if LLM_CANDIDATES is None:
        ids = {m.id for m in groq_client.models.list().data}
        print("Model khả dụng:", sorted(ids), flush=True)
        LLM_CANDIDATES = [name for name in PREFERRED_LLM if name in ids]
        print("Model chat sẽ dùng (theo thứ tự):", LLM_CANDIDATES, flush=True)
        if not LLM_CANDIDATES:
            LLM_CANDIDATES = None
            raise RuntimeError("Key Groq không có model chat nào trong danh sách ưu tiên")
    return LLM_CANDIDATES


def ask_llm(history: list, user_text: str) -> str:
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
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

            # Dữ liệu audio từ mic
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

            elif text == "STOP_RECORDING":
                recording = False
                pcm = bytes(audio_buffer)
                audio_buffer.clear()

                if len(pcm) < MIN_BYTES:
                    await websocket.send_text("Âm thanh quá ngắn, hãy nói lâu hơn.")
                    continue

                print(f"Nhận {len(pcm)} bytes, bắt đầu nhận dạng giọng nói", flush=True)
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
                except Exception as e:
                    reply = f"Lỗi: {e}"
                    print(reply, flush=True)

                print("Trả lời:", reply, flush=True)
                await websocket.send_text(reply)

    except WebSocketDisconnect:
        pass

    print("ESP32 ngắt kết nối", flush=True)
