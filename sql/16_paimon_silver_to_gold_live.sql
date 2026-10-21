-- Template SQL: placeholders preenchidos pelo executor E2E.
-- Gold Streaming V1: Silver Paimon -> Flink -> Gold Paimon.
--
-- Event time em TIMESTAMP(3), preservando a Silver TIMESTAMP(6).
-- Watermark: 5 segundos de tolerância experimental.
-- Janelas: 10 segundos, com agregação stateful.
--
-- A Gold de streaming usa warehouse próprio.
-- O destino é append-only porque janelas concluídas
-- devem gerar um resultado final, e não atualizações parciais.
--
-- Recuperação: sempre pelo estado Flink compatível.
-- Não reiniciar do snapshot inicial no mesmo destino.

SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
-- Desativa a agregação local/global que descartou 141 resultados.
SET 'table.optimizer.agg-phase-strategy' = 'ONE_PHASE';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.dml-sync' = 'false';

CREATE TEMPORARY TABLE silver_watermarked (
    source_name STRING,
    source_seq BIGINT,
    collection_name STRING,
    operation_name STRING,
    event_time_utc TIMESTAMP(6),
    witnessed_at_utc TIMESTAMP(6),
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP_LTZ(3),

    event_time_ms AS CAST(
        event_time_utc AS TIMESTAMP(3)
    ),

    WATERMARK FOR event_time_ms AS
        event_time_ms - INTERVAL '5' SECOND
) WITH (
    'connector' = 'paimon',
    'path' = '__SILVER_TABLE_PATH__',
    'scan.mode' = 'latest'
);

CREATE CATALOG gold_streaming WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = '__GOLD_WAREHOUSE__'
);

USE CATALOG gold_streaming;
CREATE DATABASE IF NOT EXISTS social;
USE social;

CREATE TABLE IF NOT EXISTS gold_activity_10s (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    total_events BIGINT,
    create_count BIGINT,
    update_count BIGINT,
    delete_count BIGINT
) WITH (
    'bucket' = '-1'
);

INSERT INTO gold_activity_10s
SELECT
    window_start,
    window_end,
    COUNT(*) AS total_events,
    SUM(
        CASE WHEN operation_name = 'create'
        THEN 1 ELSE 0 END
    ) AS create_count,
    SUM(
        CASE WHEN operation_name = 'update'
        THEN 1 ELSE 0 END
    ) AS update_count,
    SUM(
        CASE WHEN operation_name = 'delete'
        THEN 1 ELSE 0 END
    ) AS delete_count
FROM TABLE(
    TUMBLE(
        TABLE default_catalog.default_database.silver_watermarked,
        DESCRIPTOR(event_time_ms),
        INTERVAL '10' SECOND
    )
)
GROUP BY window_start, window_end;
