"""Native :mod:`asyncio` implementation of the FCM MCS receiver."""

import asyncio
import inspect
import logging
import ssl
import struct
import time
from contextlib import suppress
from typing import Any, Callable, Dict, Iterable, Optional, Union

from .mcs import (
    Close,
    DataMessageStanza,
    HeartbeatAck,
    HeartbeatPing,
    IqStanza,
    LoginRequest,
    LoginResponse,
    Setting,
    StreamErrorStanza,
)


log = logging.getLogger("push_receiver")


class AsyncPushReceiver:
    """Receive FCM MCS messages without blocking the event loop.

    ``listen`` accepts both regular and coroutine callbacks.  Call ``stop``
    from another task or from an async callback to close the active connection
    and make ``listen`` return promptly.
    """

    HOST = "mtalk.google.com"
    PORT = 5228
    READ_TIMEOUT_SECS = 60 * 60
    MIN_RESET_INTERVAL_SECS = 5 * 60
    MAX_SILENT_INTERVAL_SECS = 60 * 60
    MCS_VERSION = 41

    PACKET_BY_TAG = [
        HeartbeatPing,
        HeartbeatAck,
        LoginRequest,
        LoginResponse,
        Close,
        "MessageStanza",
        "PresenceStanza",
        IqStanza,
        DataMessageStanza,
        "BatchPresenceStanza",
        StreamErrorStanza,
        "HttpRequest",
        "HttpResponse",
        "BindAccountRequest",
        "BindAccountResponse",
        "TalkMetadata",
    ]

    def __init__(
        self,
        credentials: Dict[str, Any],
        received_persistent_ids: Optional[Iterable[str]] = None,
        host: str = HOST,
        port: int = PORT,
        ssl_context: Union[ssl.SSLContext, bool] = True,
    ) -> None:
        self.credentials = credentials
        self.persistent_ids = list(received_persistent_ids or [])
        self.host = host
        self.port = port
        if ssl_context is True:
            self.ssl_context = ssl.create_default_context()
        elif ssl_context is False:
            self.ssl_context = None
        else:
            self.ssl_context = ssl_context
        self.time_last_message_received = time.monotonic()

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._stop_event = asyncio.Event()
        self._status_task: Optional[asyncio.Task] = None
        self._last_reset = 0.0
        self._listening = False

    @property
    def is_running(self) -> bool:
        """Whether ``listen`` currently owns an active MCS connection."""
        return self._listening

    async def __aenter__(self) -> "AsyncPushReceiver":
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.stop()

    @staticmethod
    def _encode_varint32(value: int) -> bytes:
        if value < 0:
            raise ValueError("varint values must be non-negative")

        encoded = bytearray()
        while True:
            byte = value & 0x7F
            value >>= 7
            if value:
                encoded.append(byte | 0x80)
            else:
                encoded.append(byte)
                return bytes(encoded)

    async def _read_varint32(self) -> int:
        value = 0
        for shift in range(0, 35, 7):
            byte = (await self._read_exactly(1))[0]
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return value
        raise ValueError("invalid MCS varint")

    async def _read_exactly(self, size: int) -> bytes:
        if self._reader is None:
            raise ConnectionError("MCS connection is not open")
        return await self._reader.readexactly(size)

    async def _open(self) -> None:
        kwargs: Dict[str, Any] = {"ssl": self.ssl_context}
        if self.ssl_context is not None:
            kwargs["server_hostname"] = self.host

        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port, **kwargs
        )
        log.debug("connected to MCS socket")

    async def _close_connection(self) -> None:
        writer = self._writer
        self._reader = None
        self._writer = None

        if writer is None:
            return

        writer.close()
        with suppress(ConnectionError, OSError):
            await writer.wait_closed()

    async def _send(self, packet: Any) -> None:
        if self._writer is None:
            raise ConnectionError("MCS connection is not open")

        try:
            tag = self.PACKET_BY_TAG.index(type(packet))
        except ValueError as exc:
            raise ValueError("unsupported MCS packet type") from exc

        payload = packet.SerializeToString()
        frame = (
            bytes([self.MCS_VERSION, tag])
            + self._encode_varint32(len(payload))
            + payload
        )
        self._writer.write(frame)
        await self._writer.drain()

    async def _recv_packet(self, first: bool = False) -> Any:
        if first:
            version, tag = struct.unpack("BB", await self._read_exactly(2))
            if version < self.MCS_VERSION and version != 38:
                raise RuntimeError("protocol version {} unsupported".format(version))
        else:
            tag = (await self._read_exactly(1))[0]

        if tag >= len(self.PACKET_BY_TAG):
            raise RuntimeError("unknown MCS packet tag {}".format(tag))

        packet_type = self.PACKET_BY_TAG[tag]
        if isinstance(packet_type, str):
            raise RuntimeError("unsupported MCS packet tag {}".format(tag))

        size = await self._read_varint32()
        payload = packet_type()
        payload.parse(await self._read_exactly(size))
        self.time_last_message_received = time.monotonic()
        log.debug("received MCS packet %s (%s bytes)", packet_type.__name__, size)
        return payload

    async def _recv(self, first: bool = False) -> Any:
        try:
            return await asyncio.wait_for(
                self._recv_packet(first), timeout=self.READ_TIMEOUT_SECS
            )
        except asyncio.TimeoutError:
            log.debug("MCS read timed out")
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as exc:
            log.debug("MCS read failed: %s", exc)
        return None

    async def _login(self) -> None:
        await self._open()

        android_id = self.credentials["gcm"]["androidId"]
        request = LoginRequest()
        request.adaptive_heartbeat = False
        request.auth_service = 2
        request.auth_token = self.credentials["gcm"]["securityToken"]
        request.id = "chrome-63.0.3234.0"
        request.domain = "mcs.android.com"
        request.device_id = "android-%x" % int(android_id)
        request.network_type = 1
        request.resource = android_id
        request.user = android_id
        request.use_rmq2 = True
        request.setting.append(Setting(name="new_vc", value="1"))
        request.received_persistent_id.extend(self.persistent_ids)

        await self._send(request)
        response = await self._recv(first=True)
        if response is None:
            raise ConnectionError("MCS login connection closed")
        log.info("received MCS login response: %s", response)

    async def _reset(self) -> None:
        if self._stop_event.is_set():
            return

        remaining = self.MIN_RESET_INTERVAL_SECS - (
            time.monotonic() - self._last_reset
        )
        if remaining > 0:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=remaining)
                return
            except asyncio.TimeoutError:
                pass

        self._last_reset = time.monotonic()
        log.debug("reestablishing MCS connection")
        await self._cancel_status_task()
        await self._close_connection()
        if not self._stop_event.is_set():
            await self._login()
            self._start_status_task()

    async def _status_loop(self) -> None:
        while not self._stop_event.is_set():
            remaining = self.MAX_SILENT_INTERVAL_SECS - (
                time.monotonic() - self.time_last_message_received
            )
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=max(remaining, 0)
                )
                return
            except asyncio.TimeoutError:
                if self._stop_event.is_set():
                    return
                log.info("MCS connection was silent for too long; resetting it")
                await self._close_connection()
                return

    def _start_status_task(self) -> None:
        if self._status_task is None or self._status_task.done():
            self._status_task = asyncio.create_task(self._status_loop())

    async def _cancel_status_task(self) -> None:
        task = self._status_task
        self._status_task = None
        if task is None or task is asyncio.current_task():
            return
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _handle_data_message(
        self, packet: DataMessageStanza, callback: Callable[..., Any], obj: Any
    ) -> str:
        notification = {item.key: item.value for item in packet.app_data}
        log.info("received data message %s: %s", packet.persistent_id, notification)

        callback_result = callback(obj, notification, packet)
        if inspect.isawaitable(callback_result):
            await callback_result
        return packet.persistent_id

    async def _handle_ping(self, packet: HeartbeatPing) -> None:
        response = HeartbeatAck()
        response.stream_id = packet.stream_id + 1
        response.last_stream_id_received = packet.stream_id
        response.status = packet.status
        await self._send(response)

    async def listen(self, callback: Callable[..., Any], obj: Any = None) -> None:
        """Listen until :meth:`stop` is called.

        ``callback(obj, notification, data_message)`` may be a normal function
        or an ``async def`` coroutine function.
        """
        if self._listening:
            raise RuntimeError("the receiver is already listening")

        self._stop_event.clear()
        self._listening = True
        try:
            await self._login()
            self._start_status_task()

            while not self._stop_event.is_set():
                packet = await self._recv()
                if self._stop_event.is_set():
                    break
                if isinstance(packet, DataMessageStanza):
                    self.persistent_ids.append(
                        await self._handle_data_message(packet, callback, obj)
                    )
                elif isinstance(packet, HeartbeatPing):
                    await self._handle_ping(packet)
                elif packet is None or isinstance(packet, Close):
                    await self._reset()
                else:
                    log.debug("unexpected MCS packet type %s", type(packet).__name__)
        finally:
            self._stop_event.set()
            await self._cancel_status_task()
            await self._close_connection()
            self._listening = False

    async def stop(self) -> None:
        """Stop listening and close the MCS connection.

        Closing the writer wakes a pending stream read, so shutdown does not
        wait for ``READ_TIMEOUT_SECS``.
        """
        self._stop_event.set()
        await self._cancel_status_task()
        await self._close_connection()
