"""Testes do adaptador Kafka sem conexão com um broker real."""

import unittest

from bluesky_streaming.kafka_publisher import KafkaPublisher


class FakeProducer:
    """Simula confirmações e falhas da biblioteca confluent-kafka."""

    def __init__(self, config, scenario="success"):
        self.config = config
        self.scenario = scenario
        self.messages = []
        self.callback = None

    def produce(self, topic, value, callback):
        if self.scenario == "buffer_full":
            raise BufferError("Simulated full queue")

        self.messages.append((topic, value))
        self.callback = callback

    def flush(self, timeout):
        if self.scenario == "timeout":
            return 1

        if self.scenario == "delivery_failure":
            self.callback(Exception("Simulated delivery failure"), None)
        elif self.scenario == "success":
            self.callback(None, object())

        return 0


class TestKafkaPublisher(unittest.TestCase):

    def make_publisher(self, scenario):
        """Cria o publisher com uma implementação Kafka simulada."""
        return KafkaPublisher(
            bootstrap_servers="127.0.0.1:9092",
            topic="bluesky.posts.raw",
            producer_factory=lambda config: FakeProducer(config, scenario),
        )

    def test_entrega_confirmada(self):
        publisher = self.make_publisher("success")

        publisher.publish(b'{"source_seq": 100}')

        self.assertEqual(len(publisher.producer.messages), 1)
        self.assertEqual(
            publisher.producer.messages[0][0],
            "bluesky.posts.raw",
        )

    def test_falha_de_entrega(self):
        publisher = self.make_publisher("delivery_failure")

        with self.assertRaises(RuntimeError):
            publisher.publish(b'{"source_seq": 100}')

    def test_timeout(self):
        publisher = self.make_publisher("timeout")

        with self.assertRaises(RuntimeError):
            publisher.publish(b'{"source_seq": 100}')

    def test_buffer_cheio(self):
        publisher = self.make_publisher("buffer_full")

        with self.assertRaisesRegex(
            RuntimeError, "buffer do produtor cheio"
        ):
            publisher.publish(b'{"source_seq": 100}')

    def test_sem_confirmacao(self):
        publisher = self.make_publisher("no_callback")

        with self.assertRaises(RuntimeError):
            publisher.publish(b'{"source_seq": 100}')


if __name__ == "__main__":
    unittest.main()
