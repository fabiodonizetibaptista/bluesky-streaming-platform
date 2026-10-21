"""Testes de regressão dos contratos SQL Bronze e Silver Paimon.

Estes testes são estáticos: não iniciam Flink, Kafka ou Paimon.
Protegem as decisões arquiteturais durante futuras refatorações.
"""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql"


def read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8")


def quality_rules(sql: str) -> str:
    """Extrai somente os blocos de regras de negócio da Silver.

    A origem e os destinos podem mudar de tecnologia.
    As regras de extração, tipagem e classificação devem permanecer
    equivalentes até que haja uma mudança explícita de contrato.
    """
    start = "-- 2. Extração"
    end = "-- 5. Destino"

    if sql.count(start) != 1 or sql.count(end) != 1:
        raise ValueError("SQL_QUALITY_SECTION_NOT_UNIQUE")

    return sql.split(start, 1)[1].split(end, 1)[0]


class TestPaimonSqlContract(unittest.TestCase):

    def test_silver_preserva_regras_do_sql_original(self):
        original = read_sql("06_kafka_to_silver_streaming.sql")
        paimon = read_sql("08_paimon_bronze_to_silver.sql")

        self.assertEqual(
            quality_rules(paimon),
            quality_rules(original),
            "As regras de qualidade foram alteradas na migração.",
        )

    def test_silver_nao_consome_kafka_diretamente(self):
        sql = read_sql("08_paimon_bronze_to_silver.sql")

        self.assertNotIn("'connector' = 'kafka'", sql)
        self.assertIn(
            "FROM bronze_platform.social.bronze_events",
            sql,
        )

    def test_silver_possui_dois_destinos_paimon(self):
        sql = read_sql("08_paimon_bronze_to_silver.sql")

        self.assertIn(
            "CREATE TABLE IF NOT EXISTS silver_valid_sink",
            sql,
        )
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS silver_quarantine_sink",
            sql,
        )
        self.assertEqual(sql.count("'bucket' = '-1'"), 2)
        self.assertIn("EXECUTE STATEMENT SET", sql)

    def test_bronze_preserva_contrato_kafka(self):
        sql = read_sql("07_kafka_to_paimon_bronze.sql")

        for field in (
            "payload STRING",
            "kafka_partition INT",
            "kafka_offset BIGINT",
            "kafka_timestamp TIMESTAMP_LTZ(3)",
        ):
            with self.subTest(field=field):
                self.assertIn(field, sql)

        self.assertIn("'topic' = 'bluesky.posts.raw'", sql)
        self.assertIn("'format' = 'raw'", sql)
        self.assertIn("INSERT INTO bronze_events", sql)
        self.assertNotIn("'scan.bounded.mode'", sql)


if __name__ == "__main__":
    unittest.main()
