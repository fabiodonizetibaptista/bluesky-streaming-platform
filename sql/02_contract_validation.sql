-- Diagnóstico inicial de qualidade dos registros Kafka.
-- Não exibe payloads nem identificadores individuais.
-- A leitura termina nos offsets disponíveis no início do job.

SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TEMPORARY TABLE bluesky_contract_input (
    payload STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'bluesky.posts.raw',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flink-bluesky-quality-probe',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset',
    'format' = 'raw'
);

CREATE TEMPORARY VIEW classified_events AS
SELECT
    CASE
        WHEN payload IS NOT JSON OBJECT
            THEN 'INVALID_JSON'

        WHEN JSON_VALUE(payload, '$.source')
             IS DISTINCT FROM 'bluesky_jetstream_v2'
            THEN 'OTHER_OR_TEST'

        WHEN JSON_VALUE(payload, '$.collection')
             IS DISTINCT FROM 'app.bsky.feed.post'
          OR JSON_VALUE(payload, '$.operation')
             NOT IN ('create', 'update', 'delete')
          OR JSON_VALUE(payload, '$.operation') IS NULL
          OR JSON_VALUE(payload, '$.source_seq') IS NULL
          OR JSON_VALUE(payload, '$.event_time') IS NULL
          OR JSON_VALUE(payload, '$.witnessed_at') IS NULL
            THEN 'INVALID_CONTRACT'

        ELSE 'VALID_CANDIDATE'
    END AS quality_status
FROM bluesky_contract_input;

-- Mostra apenas as contagens agregadas por classificação.
SELECT
    quality_status,
    COUNT(*) AS total_records
FROM classified_events
GROUP BY quality_status;
