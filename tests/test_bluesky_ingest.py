"""Testes do loop de ingestão e reconexão do Jetstream v2."""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from bluesky_streaming import ingest


def make_frame(seq):
    """Cria um evento mínimo compatível com o contrato v2."""
    return json.dumps({
        "$type": "message",
        "payload": {
            "$type": "network.bsky.jetstream.subscribeEvents#commit",
            "seq": seq,
            "collection": "app.bsky.feed.post",
            "operation": "create",
            "time": "2026-01-01T00:00:00Z",
            "witnessedAt": "2026-01-01T00:00:01Z",
        },
    })


class FakeWebSocket:
    """Simula eventos e interrupções do WebSocket."""

    def __init__(self, messages):
        self.messages = iter(messages)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def recv(self):
        item = next(self.messages)

        if isinstance(item, Exception):
            raise item

        return item


class FakePublisher:
    """Simula confirmação imediata de entrega no Kafka."""

    def __init__(self):
        self.published = []

    def publish(self, message):
        self.published.append(json.loads(message))


class TestBlueskyIngest(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.checkpoint_path = (
            Path(self.temp_dir.name) / "cursor.json"
        )
        self.publisher = FakePublisher()

    async def test_reconecta_utilizando_cursor_confirmado(self):
        urls = []
        connections = [
            [make_frame(100), ConnectionResetError("simulated")],
            [make_frame(101)],
        ]

        def fake_connect(url, **kwargs):
            urls.append(url)
            return FakeWebSocket(connections.pop(0))

        with (
            patch.object(ingest, "CHECKPOINT_PATH", self.checkpoint_path),
            patch.object(ingest, "MAX_EVENTS", 2),
            patch.object(ingest, "connect", side_effect=fake_connect),
            patch.object(ingest, "KafkaPublisher", return_value=self.publisher),
            patch.object(ingest.asyncio, "sleep", new_callable=lambda: self.fake_sleep),
        ):
            await ingest.ingest_events()

        self.assertEqual(len(urls), 2)

        first_cursor = parse_qs(urlparse(urls[0]).query).get("cursor")
        second_cursor = parse_qs(urlparse(urls[1]).query).get("cursor")

        self.assertIsNone(first_cursor)
        self.assertEqual(second_cursor, ["100"])
        self.assertEqual(len(self.publisher.published), 2)

    async def test_limite_de_reconexoes(self):
        attempts = 0

        def fake_connect(url, **kwargs):
            nonlocal attempts
            attempts += 1
            raise ConnectionRefusedError("simulated")

        with (
            patch.object(ingest, "CHECKPOINT_PATH", self.checkpoint_path),
            patch.object(ingest, "MAX_EVENTS", 1),
            patch.object(ingest, "connect", side_effect=fake_connect),
            patch.object(ingest, "KafkaPublisher", return_value=self.publisher),
            patch.object(ingest.asyncio, "sleep", new_callable=lambda: self.fake_sleep),
        ):
            with self.assertRaises(RuntimeError):
                await ingest.ingest_events()

        self.assertEqual(attempts, 4)

    async def fake_sleep(self, delay):
        """Evita espera real durante os testes."""
        return None


    async def test_falha_kafka_nao_reconecta(self):
        """Falha de publicação é fatal e preserva o checkpoint."""
        connections = 0

        def fake_connect(url, **kwargs):
            nonlocal connections
            connections += 1
            return FakeWebSocket([make_frame(100)])

        def fail_publish(message):
            raise OSError("Simulated Kafka failure")

        self.publisher.publish = fail_publish

        with (
            patch.object(ingest, "CHECKPOINT_PATH", self.checkpoint_path),
            patch.object(ingest, "MAX_EVENTS", 1),
            patch.object(ingest, "connect", side_effect=fake_connect),
            patch.object(ingest, "KafkaPublisher", return_value=self.publisher),
        ):
            with self.assertRaises(RuntimeError):
                await ingest.ingest_events()

        self.assertEqual(connections, 1)
        self.assertFalse(self.checkpoint_path.exists())

    async def test_falha_checkpoint_nao_reconecta(self):
        """Falha de persistência não pode disparar reconexão."""
        connections = 0

        def fake_connect(url, **kwargs):
            nonlocal connections
            connections += 1
            return FakeWebSocket([make_frame(100)])

        def fail_save(self, cursor):
            raise OSError("Simulated disk failure")

        with (
            patch.object(ingest, "CHECKPOINT_PATH", self.checkpoint_path),
            patch.object(ingest, "MAX_EVENTS", 1),
            patch.object(ingest, "connect", side_effect=fake_connect),
            patch.object(ingest, "KafkaPublisher", return_value=self.publisher),
            patch(
                "bluesky_streaming.checkpoint.CheckpointStore.save",
                fail_save,
            ),
        ):
            with self.assertRaises(RuntimeError):
                await ingest.ingest_events()

        self.assertEqual(connections, 1)
        self.assertEqual(len(self.publisher.published), 1)
        self.assertFalse(self.checkpoint_path.exists())


    async def test_replay_inclusivo_preserva_progresso(self):
        """Replay republica evento, sem retroceder o checkpoint."""
        from bluesky_streaming.checkpoint import CheckpointStore

        urls = []
        connections = [
            [make_frame(100), ConnectionResetError("simulated")],
            [make_frame(100), make_frame(101)],
        ]

        def fake_connect(url, **kwargs):
            urls.append(url)
            return FakeWebSocket(connections.pop(0))

        with (
            patch.object(ingest, "CHECKPOINT_PATH", self.checkpoint_path),
            patch.object(ingest, "MAX_EVENTS", 3),
            patch.object(ingest, "connect", side_effect=fake_connect),
            patch.object(ingest, "KafkaPublisher", return_value=self.publisher),
            patch.object(ingest.asyncio, "sleep", new_callable=lambda: self.fake_sleep),
        ):
            await ingest.ingest_events()

        sequences = [
            event["source_seq"]
            for event in self.publisher.published
        ]

        self.assertEqual(sequences, [100, 100, 101])
        self.assertEqual(
            CheckpointStore(self.checkpoint_path).load(),
            101,
        )
        self.assertEqual(len(urls), 2)


if __name__ == "__main__":
    unittest.main()
