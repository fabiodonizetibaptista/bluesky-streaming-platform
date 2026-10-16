"""Publicação confirmada de eventos do Bluesky no Apache Kafka."""

import os

from confluent_kafka import Producer


class KafkaPublisher:
    """Adaptador Kafka com confirmação explícita de entrega.

    A implementação é sequencial para manter simples a relação
    entre publicação confirmada e avanço do checkpoint.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        producer_factory=Producer,
    ):
        self.topic = topic or os.getenv(
            "KAFKA_TOPIC", "bluesky.posts.raw"
        )

        self.producer = producer_factory({
            "bootstrap.servers": bootstrap_servers or os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
            ),
            "acks": "all",
            "enable.idempotence": True,
            "delivery.timeout.ms": 10000,
        })

    def publish(self, message: bytes) -> None:
        """Retorna somente depois da confirmação do broker Kafka."""

        result = {"confirmed": False, "failed": False}

        def delivery_callback(error, kafka_message):
            if error is not None:
                result["failed"] = True
            else:
                result["confirmed"] = True

        # Um BufferError não pode avançar o checkpoint.
        # Por enquanto, propagamos a falha para a camada superior.
        self.producer.produce(
            topic=self.topic,
            value=message,
            callback=delivery_callback,
        )

        # Atende os callbacks e aguarda a conclusão das entregas.
        remaining = self.producer.flush(timeout=12)

        if remaining > 0 or result["failed"] or not result["confirmed"]:
            raise RuntimeError("Entrega Kafka não confirmada")
