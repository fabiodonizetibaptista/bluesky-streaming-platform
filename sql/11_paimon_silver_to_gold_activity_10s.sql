-- Gold ML V1: atividade em intervalos de 10 segundos.
--
-- Fonte: Silver reconciliada, 100 mil registros.
-- Grão: uma linha por window_start, alinhado a 10 segundos.
-- Tempo: event_time_utc, interpretado em UTC.
-- Sem TUMBLE: evita o defeito temporal observado na Gold V1.
-- Sem deduplicação lógica: preserva volume bruto observado.
--
-- Warehouse independente das Golds anteriores.

SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '4';
SET 'table.dml-sync' = 'true';

CREATE CATALOG silver_ml_source WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = 'file:///opt/flink/runtime/paimon/live_runs/e0602ddcb53d/silver'
);

CREATE CATALOG gold_ml WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = 'file:///opt/flink/runtime/paimon/live_runs/e0602ddcb53d/gold_ml_10s'
);

USE CATALOG gold_ml;
CREATE DATABASE IF NOT EXISTS social;
USE social;

CREATE TABLE IF NOT EXISTS gold_activity_10s (
    window_start TIMESTAMP(6) NOT NULL,
    window_end TIMESTAMP(6),
    total_events BIGINT,
    distinct_source_sequences BIGINT,
    create_count BIGINT,
    update_count BIGINT,
    delete_count BIGINT,
    avg_observation_delay_ms DOUBLE,
    max_observation_delay_ms BIGINT,
    PRIMARY KEY (window_start) NOT ENFORCED
) WITH (
    'bucket' = '1'
);

-- Alinhamento explícito: 00, 10, 20, 30, 40 e 50.
CREATE TEMPORARY VIEW normalized_events AS
SELECT
    CAST(
        CONCAT(
            DATE_FORMAT(event_time_utc, 'yyyy-MM-dd HH:mm:'),
            LPAD(
                CAST(
                    CAST(
                        FLOOR(
                            EXTRACT(SECOND FROM event_time_utc) / 10
                        ) AS INT
                    ) * 10 AS STRING
                ),
                2,
                '0'
            )
        ) AS TIMESTAMP(6)
    ) AS window_start,
    source_seq,
    operation_name,
    event_time_utc,
    witnessed_at_utc
FROM silver_ml_source.social.silver_valid_sink;

INSERT INTO gold_activity_10s
SELECT
    window_start,
    window_start + INTERVAL '10' SECOND,
    COUNT(*),
    COUNT(DISTINCT source_seq),
    SUM(CASE WHEN operation_name = 'create' THEN 1 ELSE 0 END),
    SUM(CASE WHEN operation_name = 'update' THEN 1 ELSE 0 END),
    SUM(CASE WHEN operation_name = 'delete' THEN 1 ELSE 0 END),
    CAST(
        AVG(
            TIMESTAMPDIFF(
                MILLISECOND,
                event_time_utc,
                witnessed_at_utc
            )
        ) AS DOUBLE
    ),
    MAX(
        TIMESTAMPDIFF(
            MILLISECOND,
            event_time_utc,
            witnessed_at_utc
        )
    )
FROM normalized_events
GROUP BY window_start;
