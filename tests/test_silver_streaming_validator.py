"""Testes locais dos contratos do validador Silver Streaming.

Não exigem Kafka, Flink ou Docker em execução.
"""

import importlib.util
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


SCRIPT = Path(__file__).resolve().parents[1] / (
    "scripts/validate_silver_streaming.py"
)

spec = importlib.util.spec_from_file_location(
    "validate_silver_streaming", SCRIPT
)
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)


class TestSilverStreamingValidator(unittest.TestCase):

    def test_valid_kafka_position(self):
        self.assertEqual(
            validator.position({
                "kafka_partition": 0,
                "kafka_offset": 153,
            }),
            (0, 153),
        )

    def test_invalid_kafka_positions(self):
        invalid = [
            {"kafka_partition": True, "kafka_offset": 1},
            {"kafka_partition": 0, "kafka_offset": -1},
            {"kafka_partition": 0, "kafka_offset": "153"},
            {"kafka_partition": 0},
        ]

        for row in invalid:
            with self.subTest(row=row):
                with self.assertRaises(RuntimeError):
                    validator.position(row)

    def test_only_finalized_files_are_read(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)

            (directory / "part-0").write_text(
                json.dumps({
                    "kafka_partition": 0,
                    "kafka_offset": 153,
                }) + "\n",
                encoding="utf-8",
            )

            # Arquivos abertos pelo Flink não podem entrar
            # na reconciliação de dados finalizados.
            (directory / ".part-1.inprogress.id").write_text(
                "incomplete",
                encoding="utf-8",
            )

            rows = validator.finalized_rows(directory)

            self.assertEqual(len(rows), 1)
            self.assertEqual(
                validator.position(rows[0]),
                (0, 153),
            )

    def test_streaming_sql_is_isolated(self):
        run_id = "unit-test-123"

        sql = validator.make_sql(run_id)

        self.assertIn(
            f"flink-bluesky-silver-validation-{run_id}",
            sql,
        )
        self.assertIn(
            f"/silver/validation_runs/{run_id}/valid",
            sql,
        )
        self.assertIn(
            f"/silver/validation_runs/{run_id}/quarantine",
            sql,
        )
        self.assertIn(
            "'scan.startup.mode' = 'latest-offset'",
            sql,
        )
        self.assertEqual(
            sql.count("'sink.rolling-policy.rollover-interval'"),
            2,
        )

    def test_expected_quality_cases(self):
        cases = validator.make_cases()

        self.assertEqual(len(cases), 5)
        self.assertEqual(
            [expected for _, _, expected in cases],
            [
                "VALID",
                "VALID",
                "INVALID_OPERATION",
                "INVALID_SEQUENCE",
                "UNSUPPORTED_SOURCE",
            ],
        )

        self.assertEqual(
            len({label for label, _, _ in cases}),
            5,
        )


if __name__ == "__main__":
    unittest.main()
