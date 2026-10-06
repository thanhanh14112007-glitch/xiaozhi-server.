import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from google import genai

app = FastAPI()
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    audio_buffer = bytearray()
    try:
        while True:
            message = await websocket.receive()
            if "bytes" in message:
                audio_buffer.extend(message["bytes"])
            elif "text" in message:
                if message["text"] == "STOP":
                    if len(audio_buffer) > 0:
                        try:
                            response = client.models.generate_content(
                                model='gemini-2.5-flash',
                                contents=[
                                    {"mime_type": "audio/pcm", "data": bytes(audio_buffer)},
                                    "Hãy nghe đoạn âm thanh này và trả lời ngắn gọn bằng tiếng Việt."
                                ]
                            )
                            await websocket.send_text(response.text)
                        except Exception as e:
                            await websocket.send_text(f"Lỗi: {str(e)}")
                    audio_buffer.clear()
    except WebSocketDisconnect:
        pass

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get('PORT', 5000)))
