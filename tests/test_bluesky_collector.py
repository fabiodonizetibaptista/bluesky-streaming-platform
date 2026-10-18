"""Testes da ordem entre publicação Kafka e checkpoint."""

import json
import tempfile
import unittest
from pathlib import Path

from bluesky_streaming.checkpoint import CheckpointStore
from bluesky_streaming.collector import process_event


class TestBlueskyCollector(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        path = Path(self.temp_dir.name) / "cursor.json"
        self.checkpoint = CheckpointStore(path)

        self.frame = {
            "$type": "message",
            "payload": {
                "$type": "network.bsky.jetstream.subscribeEvents#commit",
                "seq": 100,
                "collection": "app.bsky.feed.post",
                "operation": "create",
                "time": "2026-01-01T00:00:00Z",
                "witnessedAt": "2026-01-01T00:00:01Z",
            },
        }

    def test_publicacao_confirmada_avanca_cursor(self):
        published = []

        def publish(message):
            published.append(json.loads(message))

        result = process_event(
            self.frame,
            publish,
            self.checkpoint,
        )

        self.assertEqual(result, 100)
        self.assertEqual(self.checkpoint.load(), 100)
        self.assertEqual(len(published), 1)

    def test_falha_publicacao_nao_avanca_cursor(self):
        self.checkpoint.save(90)

        def publish(message):
            raise RuntimeError("Simulated Kafka failure")

        with self.assertRaises(RuntimeError):
            process_event(self.frame, publish, self.checkpoint)

        self.assertEqual(self.checkpoint.load(), 90)

    def test_envelope_invalido_nao_publica(self):
        published = []

        with self.assertRaises(ValueError):
            process_event(
                {"payload": {"seq": 100}},
                published.append,
                self.checkpoint,
            )

        self.assertEqual(published, [])
        self.assertIsNone(self.checkpoint.load())

    def test_sequencia_invalida_nao_publica(self):
        self.frame["payload"]["seq"] = True
        published = []

        with self.assertRaises(ValueError):
            process_event(
                self.frame,
                published.append,
                self.checkpoint,
            )

        self.assertEqual(published, [])
        self.assertIsNone(self.checkpoint.load())

    def test_checkpoint_falha_apos_publicacao(self):
        published = []
        self.checkpoint.save(90)

        def publish(message):
            published.append(message)

        def fail_save(cursor):
            raise OSError("Simulated checkpoint failure")

        self.checkpoint.save = fail_save

        with self.assertRaises(OSError):
            process_event(self.frame, publish, self.checkpoint)

        # O Kafka já teria confirmado a mensagem, mas o cursor
        # continuaria anterior. Isso pode provocar replay.
        self.assertEqual(len(published), 1)
        self.assertEqual(self.checkpoint.load(), 90)


    def test_evento_repetido_nao_regride_cursor(self):
        """Um evento anterior não pode reduzir o progresso salvo."""
        self.checkpoint.save(101)
        published = []

        process_event(
            self.frame,
            published.append,
            self.checkpoint,
        )

        self.assertEqual(len(published), 1)
        self.assertEqual(self.checkpoint.load(), 101)


    def test_publicacao_nao_expoe_dados_pessoais(self):
        """O Kafka recebe apenas os metadados autorizados."""
        self.frame["payload"].update({
            "did": "did:example:private",
            "record": {"text": "conteudo-nao-publicavel"},
            "collection": "app.bsky.feed.post",
            "operation": "create",
            "time": "2026-01-01T00:00:00Z",
            "witnessedAt": "2026-01-01T00:00:01Z",
        })

        published = []
        process_event(
            self.frame,
            published.append,
            self.checkpoint,
        )

        event = json.loads(published[0])

        self.assertEqual(event["source_seq"], 100)
        self.assertEqual(
            set(event),
            {
                "source", "source_seq", "collection",
                "operation", "event_time", "witnessed_at",
            },
        )

        self.assertNotIn("did", event)
        self.assertNotIn("record", event)
        self.assertNotIn("conteudo-nao-publicavel", published[0].decode())


    def test_rejeitar_colecao_invalida(self):
        """Eventos de outras coleções não devem ser publicados."""
        self.frame["payload"]["collection"] = "app.bsky.feed.like"
        published = []

        with self.assertRaises(ValueError):
            process_event(self.frame, published.append, self.checkpoint)

        self.assertEqual(published, [])
        self.assertIsNone(self.checkpoint.load())

    def test_rejeitar_operacao_invalida(self):
        """Operações desconhecidas não podem avançar o cursor."""
        self.frame["payload"]["operation"] = "unknown"
        published = []

        with self.assertRaises(ValueError):
            process_event(self.frame, published.append, self.checkpoint)

        self.assertEqual(published, [])
        self.assertIsNone(self.checkpoint.load())

    def test_rejeitar_timestamp_ausente(self):
        """Timestamp ausente deve impedir a publicação."""
        del self.frame["payload"]["time"]
        published = []

        with self.assertRaises(ValueError):
            process_event(self.frame, published.append, self.checkpoint)

        self.assertEqual(published, [])
        self.assertIsNone(self.checkpoint.load())


if __name__ == "__main__":
    unittest.main()
