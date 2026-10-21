-- Gold V2 — atividade por minuto de ocorrência, em UTC.
--
-- Grão: window_start, alinhado exatamente ao início do minuto.
-- Fonte: Silver validada da execução e0602ddcb53d.
-- Sem TUMBLE: utiliza agrupamento explícito de timestamps.
--
-- Não realiza deduplicação lógica por source_seq.
-- Warehouse isolado da Gold V1, preservando seu histórico.

SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '4';
SET 'table.dml-sync' = 'true';

CREATE CATALOG silver_gold_source WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = 'file:///opt/flink/runtime/paimon/live_runs/e0602ddcb53d/silver'
);

CREATE CATALOG gold_platform_v2 WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = 'file:///opt/flink/runtime/paimon/live_runs/e0602ddcb53d/gold_v2'
);

USE CATALOG gold_platform_v2;
CREATE DATABASE IF NOT EXISTS social;
USE social;

CREATE TABLE IF NOT EXISTS gold_activity_1min (
    window_start TIMESTAMP(6) NOT NULL,
    window_end TIMESTAMP(6),
    total_events BIGINT,
    distinct_source_sequences BIGINT,
    create_count BIGINT,
    update_count BIGINT,
    delete_count BIGINT,
    avg_observation_delay_ms DOUBLE,
    PRIMARY KEY (window_start) NOT ENFORCED
) WITH (
    'bucket' = '1'
);

-- Normalização antes da agregação: todos os eventos
-- ocorridos no mesmo minuto recebem a mesma chave.
CREATE TEMPORARY VIEW normalized_events AS
SELECT
    CAST(
        CONCAT(
            DATE_FORMAT(event_time_utc, 'yyyy-MM-dd HH:mm'),
            ':00'
        ) AS TIMESTAMP(6)
    ) AS minute_start,
    source_seq,
    operation_name,
    event_time_utc,
    witnessed_at_utc
FROM silver_gold_source.social.silver_valid_sink;

INSERT INTO gold_activity_1min
SELECT
    minute_start AS window_start,
    minute_start + INTERVAL '1' MINUTE AS window_end,
    COUNT(*) AS total_events,
    COUNT(DISTINCT source_seq) AS distinct_source_sequences,
    SUM(CASE WHEN operation_name = 'create' THEN 1 ELSE 0 END),
    SUM(CASE WHEN operation_name = 'update' THEN 1 ELSE 0 END),
    SUM(CASE WHEN operation_name = 'delete' THEN 1 ELSE 0 END),
    CAST(
        AVG(
            TIMESTAMPDIFF(
                MILLISECOND, event_time_utc, witnessed_at_utc
            )
        ) AS DOUBLE
    )
FROM normalized_events
GROUP BY minute_start;
