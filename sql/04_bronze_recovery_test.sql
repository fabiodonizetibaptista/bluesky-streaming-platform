-- Experimento isolado de recuperação Kafka -> Flink -> Bronze.
-- Não limpa, transforma, filtra nem deduplica os registros.
-- A origem é contínua; o job permanece ativo aguardando eventos.
-- A Bronze V1 validada não será modificada.

SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';

CREATE TEMPORARY TABLE recovery_kafka_source (
    payload STRING,
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
    kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = 'bluesky.posts.raw',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flink-bluesky-recovery-test',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'raw'
);

CREATE TEMPORARY TABLE recovery_bronze_sink (
    payload STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///opt/flink/runtime/bronze/recovery_test',
    'format' = 'json',
    'sink.rolling-policy.rollover-interval' = '30s',
    'sink.rolling-policy.check-interval' = '10s'
);

INSERT INTO recovery_bronze_sink
SELECT
    payload,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
FROM recovery_kafka_source;
