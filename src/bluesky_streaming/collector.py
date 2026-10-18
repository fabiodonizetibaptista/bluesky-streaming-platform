"""Núcleo de publicação confiável de eventos Jetstream v2."""

import json
from collections.abc import Callable

from bluesky_streaming.checkpoint import CheckpointStore


def process_event(
    frame: dict,
    publish: Callable[[bytes], None],
    checkpoint: CheckpointStore,
) -> int:
    """Publica um evento e salva o cursor após confirmação.

    O callback ``publish`` deve retornar apenas quando a entrega
    no Kafka tiver sido efetivamente confirmada.

    Args:
        frame: Envelope recebido do Jetstream v2.
        publish: Função que publica e confirma uma mensagem.
        checkpoint: Armazenamento local do cursor confirmado.

    Returns:
        Número de sequência do evento processado.

    Raises:
        ValueError: Quando o envelope não respeita o contrato mínimo.
        RuntimeError: Quando a publicação ou checkpoint falhar.
    """

    if not isinstance(frame, dict) or frame.get("$type") != "message":
        raise ValueError("Envelope Jetstream v2 inválido")

    payload = frame.get("payload")

    if not isinstance(payload, dict):
        raise ValueError("Payload Jetstream v2 inválido")

    seq = payload.get("seq")

    # bool também é int no Python, por isso verificamos o tipo exato.
    if type(seq) is not int or seq < 0:
        raise ValueError("Sequência Jetstream inválida")

    # O coletor atual é exclusivo para eventos de publicações.
    if payload.get("collection") != "app.bsky.feed.post":
        raise ValueError("Coleção Jetstream não suportada")

    if payload.get("operation") not in {"create", "update", "delete"}:
        raise ValueError("Operação Jetstream não suportada")

    for field in ("time", "witnessedAt"):
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Timestamp ausente ou inválido: {field}")

    # A identidade do evento será utilizada nas próximas etapas
    # para implementar deduplicação na camada de processamento.
    #
    # Não exibimos conteúdo nem identificadores em logs.
    # Contrato mínimo para métricas agregadas de atividade.
    # Não persistimos texto, DID ou identificadores dos registros.
    metadata = {
        "source": "bluesky_jetstream_v2",
        "source_seq": seq,
        "collection": payload.get("collection"),
        "operation": payload.get("operation"),
        "event_time": payload.get("time"),
        "witnessed_at": payload.get("witnessedAt"),
    }

    message = json.dumps(
        metadata,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    # CONTRATO: publish deve aguardar ACK do Kafka.
    # Uma exceção impede que o cursor seja persistido.
    publish(message)

    # Somente eventos confirmados podem avançar o checkpoint.
    # Um replay ou evento fora de ordem nunca deve retroceder
    # o cursor persistido.
    current_cursor = checkpoint.load()

    if current_cursor is None or seq > current_cursor:
        checkpoint.save(seq)

    return seq
