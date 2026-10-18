"""Persistência local do cursor confirmado do Bluesky Jetstream.

O checkpoint armazena o último cursor cuja publicação no Kafka
foi confirmada. A responsabilidade por decidir quando salvar
o cursor pertence ao coletor.
"""

import json
import os
import tempfile
from pathlib import Path


class CheckpointStore:
    """Gerencia o cursor de ingestão em um arquivo JSON local."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> int | None:
        """Recupera o cursor salvo ou None quando ainda não existe."""

        if not self.path.exists():
            return None

        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                "Não foi possível ler o checkpoint"
            ) from exc

        if not isinstance(data, dict) or data.get("version") != 2:
            raise ValueError("Versão do checkpoint incompatível")

        cursor = data.get("cursor")

        # bool é subtipo de int no Python; deve ser rejeitado.
        if type(cursor) is not int or cursor < 0:
            raise ValueError("Checkpoint contém cursor inválido")

        return cursor

    def save(self, cursor: int) -> None:
        """Persiste o cursor usando substituição atômica do arquivo."""

        if type(cursor) is not int or cursor < 0:
            raise ValueError("Cursor deve ser inteiro não negativo")

        self.path.parent.mkdir(parents=True, exist_ok=True)

        # O arquivo temporário fica no mesmo diretório para permitir
        # substituição atômica no mesmo sistema de arquivos.
        fd, temp_path = tempfile.mkstemp(
            prefix=".checkpoint-",
            suffix=".tmp",
            dir=self.path.parent,
        )

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                json.dump({"version": 2, "cursor": cursor}, file)
                file.flush()

                # Garante que os bytes sejam enviados ao sistema de
                # arquivos antes da substituição do checkpoint.
                os.fsync(file.fileno())

            os.replace(temp_path, self.path)

        finally:
            # Evita arquivos temporários abandonados em caso de erro.
            if os.path.exists(temp_path):
                os.unlink(temp_path)
