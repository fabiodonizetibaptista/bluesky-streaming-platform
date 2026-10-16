-- Primeiro teste de integração Flink SQL -> Kafka.
-- Não imprime payload, texto de posts nem identificadores pessoais.
-- A consulta é limitada aos offsets disponíveis no início do job.

SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TEMPORARY TABLE bluesky_kafka_probe (
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
    payload STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'bluesky.posts.raw',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flink-bluesky-probe',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset',
    'format' = 'raw'
);

-- COUNT(*) verifica a leitura sem expor os dados.
SELECT COUNT(*) AS records_read
FROM bluesky_kafka_probe;
