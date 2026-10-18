"""Ingestão Jetstream v2 com retomada e reconexão limitada.

Execução:
    PYTHONPATH=src python -m bluesky_streaming.ingest

O coletor publica sequencialmente para manter a relação entre
ACK do Kafka e checkpoint simples e verificável.
"""

import asyncio
import json
from urllib.parse import urlencode

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from bluesky_streaming.checkpoint import CheckpointStore
from bluesky_streaming.collector import process_event
from bluesky_streaming.kafka_publisher import KafkaPublisher


JETSTREAM_ENDPOINT = (
    "wss://jetstream.us-east.bsky.network"
    "/xrpc/network.bsky.jetstream.subscribeEvents"
)

CHECKPOINT_PATH = "checkpoints/bluesky_cursor.json"

MAX_EVENTS = 20
MAX_RECONNECT_ATTEMPTS = 3
RECONNECT_BASE_DELAY = 1.0
RECEIVE_TIMEOUT = 30.0


def build_url(cursor: int | None) -> str:
    """Constrói a assinatura v2 usando o último cursor confirmado."""
    parameters = {
        "collections": "app.bsky.feed.post",
        "kinds": "commit",
    }

    # O cursor v2 é inclusivo. Duplicações no replay são
    # esperadas e deverão ser removidas no processamento.
    if cursor is not None:
        parameters["cursor"] = str(cursor)

    return f"{JETSTREAM_ENDPOINT}?{urlencode(parameters)}"




class ProcessingFailure(Exception):
    """Sinaliza falha fatal no processamento de um evento."""

async def ingest_events() -> None:
    """Consome eventos, isolando erros de transporte e processamento."""
    checkpoint = CheckpointStore(CHECKPOINT_PATH)
    publisher = KafkaPublisher()

    confirmed = 0
    reconnect_attempts = 0

    while confirmed < MAX_EVENTS:
        # Qualquer falha na leitura do checkpoint é fatal.
        cursor = checkpoint.load()
        url = build_url(cursor)

        try:
            # A conexão e o recebimento ficam nesta função interna.
            async def receive_events():
                async with connect(
                    url,
                    subprotocols=["xrpc.v1.json"],
                    open_timeout=10,
                    max_size=1_000_000,
                    ping_interval=20,
                ) as websocket:
                    print("JETSTREAM_CONNECTED = True")
                    print("RESUMED_FROM_CHECKPOINT =", cursor is not None)

                    while True:
                        async with asyncio.timeout(RECEIVE_TIMEOUT):
                            yield await websocket.recv()

            async for raw_frame in receive_events():
                # O processamento fica fora da política de reconexão.
                # Entretanto, ainda precisamos proteger explicitamente
                # suas exceções da captura externa.
                frame = json.loads(raw_frame)

                if frame.get("$type") != "message":
                    continue

                try:
                    process_event(
                        frame,
                        publisher.publish,
                        checkpoint,
                    )
                except Exception as exc:
                    # Preserva a causa para diagnóstico interno,
                    # sem imprimir payloads nem valores sensíveis.
                    raise ProcessingFailure() from exc

                confirmed += 1
                reconnect_attempts = 0

                if confirmed >= MAX_EVENTS:
                    break

        except ProcessingFailure:
            raise RuntimeError(
                "Falha de processamento: ingestão interrompida"
            ) from None

        except (WebSocketException, OSError, TimeoutError) as exc:
            reconnect_attempts += 1

            print("TRANSPORT_ERROR_TYPE =", type(exc).__name__)
            print("RECONNECT_ATTEMPT =", reconnect_attempts)

            if reconnect_attempts > MAX_RECONNECT_ATTEMPTS:
                raise RuntimeError(
                    "Limite de reconexões Jetstream excedido"
                ) from exc

            delay = RECONNECT_BASE_DELAY * (
                2 ** (reconnect_attempts - 1)
            )

            print("RECONNECT_DELAY_SECONDS =", delay)
            await asyncio.sleep(delay)

    print("EVENTS_CONFIRMED =", confirmed)
    print("CHECKPOINT_SAVED =", checkpoint.load() is not None)


def main() -> None:
    """Ponto de entrada do coletor."""
    asyncio.run(ingest_events())


if __name__ == "__main__":
    main()
