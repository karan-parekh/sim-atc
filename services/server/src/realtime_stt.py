"""
Realtime STT Transform

Connects to the local Realtime STT server running in a separate docker container

Input: PCM 16-bit audio buffer (bytes)
Output: STT events (stt_chunk for partials, stt_output for final transcripts)
"""

import asyncio
import struct
import contextlib
import json
from typing import AsyncIterator, Optional
from urllib.parse import urlencode

import websockets
from websockets.client import WebSocketClientProtocol

from events import STTChunkEvent, STTEvent, STTOutputEvent


class ReatimeSTT:
    def __init__(
        self,
        sample_rate: int = 16000,
    ):
        self.sample_rate = sample_rate
        self._ws: Optional[WebSocketClientProtocol] = None
        self._connection_signal = asyncio.Event()
        self._close_signal = asyncio.Event()

    async def receive_events(self) -> AsyncIterator[STTEvent]:
        while not self._close_signal.is_set():
            _, pending = await asyncio.wait(
                [
                    asyncio.create_task(self._close_signal.wait()),
                    asyncio.create_task(self._connection_signal.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            with contextlib.suppress(asyncio.CancelledError):
                for task in pending:
                    task.cancel()

            if self._close_signal.is_set():
                break

            if self._ws and self._ws.close_code is None:
                self._connection_signal.clear()
                try:
                    async for raw_message in self._ws:
                        try:
                            message = json.loads(raw_message)
                            message_type = message.get("type")

                            if message_type == 'recording_start':
                                pass
                            elif message_type == 'fullSentence':
                                transcript = message['text']
                                yield STTOutputEvent.create(transcript)
                            elif message_type == 'realtime':
                                transcript = message['text']
                                yield STTChunkEvent.create(transcript)

                            elif message_type == 'recording_stop':
                                # no-op
                                pass
                            else:
                                print(f"WARNING: Unhandled websocket message type {message_type}")
                        except json.JSONDecodeError as e:
                            print(f"[DEBUG] RealtimeSTT JSON decode error: {e}")
                            continue
                except websockets.exceptions.ConnectionClosed:
                    print("RealtimeSTT: WebSocket connection closed")

    async def send_audio(self, audio_chunk: bytes) -> None:
        metadata = {"sampleRate": self.sample_rate}
        metadata_json = json.dumps(metadata)
        metadata_length = len(metadata_json)
        message = struct.pack('<I', metadata_length) + metadata_json.encode('utf-8') + audio_chunk

        ws = await self._ensure_connection()
        await ws.send(message)

    async def close(self) -> None:
        if self._ws and self._ws.close_code is None:
            await self._ws.close()
        self._ws = None
        self._close_signal.set()

    async def _ensure_connection(self) -> WebSocketClientProtocol:
        if self._close_signal.is_set():
            raise RuntimeError("RealtimeSTT tried establishing a connection after it was closed")
        if self._ws and self._ws.close_code is None:
            return self._ws

        url = f"ws://localhost:8012"
        self._ws = await websockets.connect(url)

        self._connection_signal.set()
        return self._ws
