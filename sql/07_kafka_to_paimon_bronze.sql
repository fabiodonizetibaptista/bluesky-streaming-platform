-- Bronze Streaming V2
-- Arquitetura: Kafka -> Flink SQL -> Apache Paimon.
--
-- Contrato:
--   Preservar o payload bruto e os metadados Kafka.
--   Não aplicar validações ou transformações de negócio.
--
-- Atenção:
--   Esta tabela é append-only, sem deduplicação automática.
--   Uma nova execução a partir do earliest-offset poderá
--   reprocessar registros; retomadas devem usar checkpoint
--   ou savepoint compatível.

SET 'parallelism.default' = '1';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.local-time-zone' = 'UTC';

-- O SQL Client recebe a confirmação da submissão sem aguardar
-- o término do job contínuo.
SET 'table.dml-sync' = 'false';

-- Warehouse exclusivo da Bronze V2. Não reutilizar o warehouse
-- utilizado nos experimentos incrementais anteriores.
CREATE CATALOG bronze_platform WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = 'file:///opt/flink/runtime/paimon/platform_bronze_v1'
);

USE CATALOG bronze_platform;

CREATE DATABASE social;
USE social;

-- Tabela append-only: uma linha por evento persistido.
CREATE TABLE bronze_events (
    payload STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP_LTZ(3)
) WITH (
    'bucket' = '-1'
);

-- Origem Kafka, preservando o contrato da Bronze anterior.
-- Sem scan.bounded.mode: a leitura permanece contínua.
CREATE TEMPORARY TABLE kafka_bronze_source (
    payload STRING,
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
    kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = 'bluesky.posts.raw',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flink-bluesky-paimon-bronze-v2',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'raw'
);

-- Um único destino transacional no Paimon.
INSERT INTO bronze_events
SELECT
    payload,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
FROM kafka_bronze_source;
