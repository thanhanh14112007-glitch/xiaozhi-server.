import asyncio
import io
import os
import wave

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from google import genai
from google.genai import types

app = FastAPI()
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

SAMPLE_RATE = 16000
MIN_BYTES = SAMPLE_RATE * 2 // 2  # tối thiểu 0.5 giây audio (16-bit mono)


def pcm_to_wav(pcm: bytes) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(pcm)
    return buf.getvalue()


def ask_gemini(pcm: bytes) -> str:
    wav = pcm_to_wav(pcm)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            types.Part.from_bytes(data=wav, mime_type="audio/wav"),
            "Hãy nghe đoạn âm thanh này và trả lời ngắn gọn bằng tiếng Việt.",
        ],
    )
    return response.text or ""


@app.get("/")
def root():
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("ESP32 đã kết nối", flush=True)

    audio_buffer = bytearray()
    recording = False

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

                print(f"Nhận {len(pcm)} bytes, gửi Gemini", flush=True)
                try:
                    reply = await asyncio.to_thread(ask_gemini, pcm)
                except Exception as e:
                    reply = f"Lỗi: {e}"
                    print(reply, flush=True)

                print("Trả lời:", reply, flush=True)
                await websocket.send_text(reply)

    except WebSocketDisconnect:
        pass

    print("ESP32 ngắt kết nối", flush=True)
