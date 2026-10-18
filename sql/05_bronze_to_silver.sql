-- Silver V4: contrato tipado, rastreável e com quarentena explicativa.
--
-- Objetivo:
--   1. Ler a Bronze sem alterar os dados originais.
--   2. Validar origem, campos e tipos.
--   3. Separar registros válidos e rejeitados.
--   4. Preservar a rastreabilidade pelos metadados Kafka.
--
-- Escopo: processamento bounded, sem deduplicação.
-- Os caminhos V3 são os destinos padrão deste SQL.
-- O validador substitui esses caminhos por diretórios isolados.
-- Não reutilizar destinos que já contenham resultados.

SET 'parallelism.default' = '1';
SET 'table.local-time-zone' = 'UTC';

-- ==========================================================
-- 1. Origem: Bronze já materializada
-- ==========================================================

CREATE TEMPORARY TABLE bronze_source (
    payload STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///opt/flink/runtime/bronze/events_v1',
    'format' = 'json'
);

-- ==========================================================
-- 2. Extração dos atributos de negócio
-- ==========================================================

CREATE TEMPORARY VIEW extracted_events AS
SELECT
    payload,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,

    JSON_VALUE(payload, '$.source') AS source_name,
    JSON_VALUE(payload, '$.source_seq') AS source_seq_text,
    -- Preserva o tipo JSON original para distinguir 123 de "123".
    JSON_QUERY(
        payload,
        '$.source_seq' WITH UNCONDITIONAL ARRAY WRAPPER
    ) AS source_seq_json,
    JSON_VALUE(payload, '$.collection') AS collection_name,
    JSON_VALUE(payload, '$.operation') AS operation_name,
    JSON_VALUE(payload, '$.event_time') AS event_time_text,
    JSON_VALUE(payload, '$.witnessed_at') AS witnessed_at_text

FROM bronze_source;

-- ==========================================================
-- 3. Conversão segura dos tipos
-- ==========================================================

CREATE TEMPORARY VIEW typed_events AS
SELECT
    *,

    TRY_CAST(source_seq_text AS BIGINT) AS source_seq,

    -- Converte a notação UTC ISO-8601 utilizada pelo coletor
    -- em TIMESTAMP(6), preservando microssegundos e UTC.
    -- TRY_CAST evita que um registro inválido derrube o job.
    TRY_CAST(
        REPLACE(
            REPLACE(event_time_text, 'T', ' '),
            'Z', ''
        ) AS TIMESTAMP(6)
    ) AS event_time_utc,

    TRY_CAST(
        REPLACE(
            REPLACE(witnessed_at_text, 'T', ' '),
            'Z', ''
        ) AS TIMESTAMP(6)
    ) AS witnessed_at_utc

FROM extracted_events;

-- ==========================================================
-- 4. Classificação: uma regra prioritária por registro
-- ==========================================================

-- O motivo de rejeição é a única fonte de verdade do contrato.
-- NULL significa que todas as validações foram satisfeitas.
CREATE TEMPORARY VIEW rejection_events AS
SELECT
    *,
        CASE
            WHEN payload IS NOT JSON OBJECT
                THEN 'INVALID_JSON'
            WHEN source_name IS DISTINCT FROM 'bluesky_jetstream_v2'
                THEN 'UNSUPPORTED_SOURCE'
            WHEN collection_name IS DISTINCT FROM 'app.bsky.feed.post'
                THEN 'INVALID_COLLECTION'
            WHEN operation_name IS NULL
              OR operation_name NOT IN ('create', 'update', 'delete')
                THEN 'INVALID_OPERATION'
            WHEN source_seq IS NULL
              OR source_seq < 0
              OR source_seq_json IS DISTINCT FROM
                 CONCAT('[', source_seq_text, ']')
              OR NOT COALESCE(
                  REGEXP(source_seq_text, '^[0-9]+$'), FALSE
              )
                THEN 'INVALID_SEQUENCE'
            WHEN event_time_utc IS NULL
              OR NOT COALESCE(
                  REGEXP(
                      event_time_text,
                      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?Z$'
                  ), FALSE
              )
                THEN 'INVALID_EVENT_TIME'
            WHEN witnessed_at_utc IS NULL
              OR NOT COALESCE(
                  REGEXP(
                      witnessed_at_text,
                      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?Z$'
                  ), FALSE
              )
                THEN 'INVALID_WITNESSED_AT'
            ELSE NULL
        END AS rejection_reason
FROM typed_events;

-- Mantém o contrato de categorias utilizado desde a Silver V3.
CREATE TEMPORARY VIEW classified_events AS
SELECT
    *,
    CASE
        WHEN rejection_reason IS NULL THEN 'VALID'
        WHEN rejection_reason IN (
            'INVALID_JSON',
            'UNSUPPORTED_SOURCE'
        ) THEN rejection_reason
        ELSE 'INVALID_CONTRACT'
    END AS quality_status
FROM rejection_events;

-- ==========================================================
-- 5. Destino: Silver válida
-- ==========================================================

CREATE TEMPORARY TABLE silver_valid_sink (
    source_name STRING,
    source_seq BIGINT,
    collection_name STRING,
    operation_name STRING,
    event_time_utc TIMESTAMP(6),
    witnessed_at_utc TIMESTAMP(6),
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///opt/flink/runtime/silver/valid_v3',
    'format' = 'json'
);

-- ==========================================================
-- 6. Destino: quarentena
-- ==========================================================

CREATE TEMPORARY TABLE silver_quarantine_sink (
    quality_status STRING,
    rejection_reason STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///opt/flink/runtime/silver/quarantine_v3',
    'format' = 'json'
);

-- ==========================================================
-- 7. Execução compartilhada com duas saídas
-- ==========================================================

EXECUTE STATEMENT SET
BEGIN

    INSERT INTO silver_valid_sink
    SELECT
        source_name,
        source_seq,
        collection_name,
        operation_name,
        event_time_utc,
        witnessed_at_utc,
        kafka_partition,
        kafka_offset,
        kafka_timestamp
    FROM classified_events
    WHERE quality_status = 'VALID';

    INSERT INTO silver_quarantine_sink
    SELECT
        quality_status,
        rejection_reason,
        kafka_partition,
        kafka_offset,
        kafka_timestamp
    FROM classified_events
    WHERE quality_status <> 'VALID';

END;
