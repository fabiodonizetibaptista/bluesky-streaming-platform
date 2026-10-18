"""Testes unitários da persistência do cursor do Jetstream."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bluesky_streaming.checkpoint import CheckpointStore


class TestCheckpointStore(unittest.TestCase):
    """Valida gravação, recuperação e integridade do checkpoint."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.path = Path(self.temp_dir.name) / "state" / "cursor.json"
        self.store = CheckpointStore(self.path)

    def test_checkpoint_inexistente(self):
        """Uma primeira execução ainda não possui cursor salvo."""
        self.assertIsNone(self.store.load())

    def test_salvar_e_recuperar(self):
        """O cursor persiste entre instâncias do armazenamento."""
        self.store.save(123456789)

        another_store = CheckpointStore(self.path)

        self.assertEqual(another_store.load(), 123456789)

    def test_atualizar_cursor(self):
        """Uma nova gravação substitui o cursor anterior."""
        self.store.save(100)
        self.store.save(200)

        self.assertEqual(self.store.load(), 200)

    def test_rejeitar_cursor_invalido(self):
        """Valores negativos, booleanos e strings não são válidos."""
        for cursor in (-1, True, "123", None):
            with self.subTest(cursor=cursor):
                with self.assertRaises(ValueError):
                    self.store.save(cursor)

    def test_rejeitar_checkpoint_corrompido(self):
        """Um JSON inválido não deve provocar reinício silencioso."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("{invalid-json", encoding="utf-8")

        with self.assertRaises(RuntimeError):
            self.store.load()

    def test_rejeitar_formato_invalido(self):
        """O cursor salvo precisa ser um inteiro não negativo."""
        self.path.parent.mkdir(parents=True, exist_ok=True)

        for content in ({"version": 2, "cursor": -1}, {"version": 2, "cursor": True}, {"version": 2}):
            with self.subTest(content=content):
                self.path.write_text(json.dumps(content), encoding="utf-8")

                with self.assertRaises(ValueError):
                    self.store.load()

    def test_falha_na_substituicao_preserva_checkpoint(self):
        """Falha antes da substituição mantém o último cursor válido."""
        self.store.save(100)

        with patch(
            "bluesky_streaming.checkpoint.os.replace",
            side_effect=OSError("Simulated filesystem failure"),
        ):
            with self.assertRaises(OSError):
                self.store.save(200)

        self.assertEqual(self.store.load(), 100)

        # O arquivo temporário não deve permanecer abandonado.
        leftovers = list(self.path.parent.glob(".checkpoint-*.tmp"))
        self.assertEqual(leftovers, [])


    def test_checkpoint_utiliza_versao_2(self):
        """O arquivo precisa identificar explicitamente o protocolo."""
        self.store.save(12345)

        data = json.loads(self.path.read_text(encoding="utf-8"))

        self.assertEqual(data["version"], 2)
        self.assertEqual(data["cursor"], 12345)

    def test_rejeitar_versao_incompativel(self):
        """Um cursor de outro protocolo não pode ser reutilizado."""
        self.path.parent.mkdir(parents=True, exist_ok=True)

        for version in (1, 3, None):
            with self.subTest(version=version):
                self.path.write_text(
                    json.dumps({
                        "version": version,
                        "cursor": 12345,
                    }),
                    encoding="utf-8",
                )

                with self.assertRaises(ValueError):
                    self.store.load()


if __name__ == "__main__":
    unittest.main()
