-- Bronze V1: persistência do valor recebido do Kafka.
-- Sem validações de negócio, filtros ou deduplicação.
-- Execução limitada aos offsets existentes no início do job.

SET 'parallelism.default' = '1';

CREATE TEMPORARY TABLE kafka_bronze_source (
    payload STRING,
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
    kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = 'bluesky.posts.raw',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flink-bluesky-bronze-v1',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset',
    'format' = 'raw'
);

CREATE TEMPORARY TABLE bronze_events (
    payload STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///opt/flink/runtime/bronze/events_v1',
    'format' = 'json'
);

INSERT INTO bronze_events
SELECT
    payload,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
FROM kafka_bronze_source;
