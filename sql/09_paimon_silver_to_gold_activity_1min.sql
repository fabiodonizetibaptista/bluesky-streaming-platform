-- Gold V1: agregação de atividade por minuto.
--
-- Grão: uma linha por window_start (UTC).
-- Fonte: Silver válida de uma execução já reconciliada.
-- Modo: batch para validação inicial da modelagem.
--
-- Não inclui quarentena, pois ela não contém event_time_utc.
-- As contagens representam linhas, não eventos logicamente
-- deduplicados. O contrato de deduplicação será tratado à parte.

SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '4';
SET 'table.dml-sync' = 'true';

CREATE CATALOG silver_gold_source WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = 'file:///opt/flink/runtime/paimon/live_runs/e0602ddcb53d/silver'
);

CREATE CATALOG gold_platform WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = 'file:///opt/flink/runtime/paimon/live_runs/e0602ddcb53d/gold'
);

USE CATALOG gold_platform;

CREATE DATABASE IF NOT EXISTS social;
USE social;

-- Uma chave por minuto. O bucket fixo simplifica esta
-- primeira versão, que contém poucas linhas agregadas.
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

INSERT INTO gold_activity_1min
SELECT
    window_start,
    window_end,
    COUNT(*) AS total_events,
    COUNT(DISTINCT source_seq) AS distinct_source_sequences,

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
    ) AS delete_count,

    CAST(AVG(
        TIMESTAMPDIFF(
            MILLISECOND,
            event_time_utc,
            witnessed_at_utc
        )
    ) AS DOUBLE) AS avg_observation_delay_ms

FROM TABLE(
    TUMBLE(
        TABLE silver_gold_source.social.silver_valid_sink,
        DESCRIPTOR(event_time_utc),
        INTERVAL '1' MINUTE
    )
)

GROUP BY
    window_start,
    window_end;
