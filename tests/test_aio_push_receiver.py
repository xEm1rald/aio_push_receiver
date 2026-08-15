import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from push_receiver import AsyncPushReceiver
from push_receiver.mcs import AppData, DataMessageStanza, LoginResponse


def encode_varint(value):
    encoded = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        encoded.append(byte | 0x80 if value else byte)
        if not value:
            return bytes(encoded)


def frame(tag, packet, first=False):
    payload = packet.SerializeToString()
    header = bytes([AsyncPushReceiver.MCS_VERSION, tag]) if first else bytes([tag])
    return header + encode_varint(len(payload)) + payload


class FakeReader:
    def __init__(self, data):
        self.data = bytearray(data)

    async def readexactly(self, size):
        if len(self.data) < size:
            raise asyncio.IncompleteReadError(bytes(self.data), size)
        value = bytes(self.data[:size])
        del self.data[:size]
        return value


class FakeWriter:
    def __init__(self):
        self.writes = []
        self.closed = False

    def write(self, data):
        self.writes.append(data)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


class AsyncPushReceiverTests(unittest.IsolatedAsyncioTestCase):
    CREDENTIALS = {
        "gcm": {"androidId": "123", "securityToken": "secret"}
    }

    def test_encodes_zero_as_a_valid_varint(self):
        self.assertEqual(AsyncPushReceiver._encode_varint32(0), b"\x00")

    async def test_async_callback_receives_message_and_can_stop_listener(self):
        message = DataMessageStanza(
            persistent_id="message-id",
            app_data=[AppData(key="channelId", value="pairing")],
        )
        reader = FakeReader(
            frame(3, LoginResponse(), first=True) + frame(8, message)
        )
        writer = FakeWriter()
        receiver = AsyncPushReceiver(
            self.CREDENTIALS, host="localhost", port=12345, ssl_context=False
        )
        received = []

        async def callback(obj, notification, data_message):
            received.append((obj, notification, data_message.persistent_id))
            await receiver.stop()

        with patch(
            "push_receiver.aio_push_receiver.asyncio.open_connection",
            new=AsyncMock(return_value=(reader, writer)),
        ):
            await receiver.listen(callback, obj="context")

        self.assertEqual(
            received,
            [("context", {"channelId": "pairing"}, "message-id")],
        )
        self.assertEqual(receiver.persistent_ids, ["message-id"])
        self.assertTrue(writer.closed)
        self.assertEqual(writer.writes[0][0:2], bytes([41, 2]))


if __name__ == "__main__":
    unittest.main()
