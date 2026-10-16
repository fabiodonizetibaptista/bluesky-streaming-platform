#!/usr/bin/env bash
set -euo pipefail

# O serviço kafka-init utiliza a rede interna do Docker.
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:19092}"
TOPIC="${TOPIC_NAME:-bluesky.posts.raw}"
EXPECTED_PARTITIONS="${EXPECTED_PARTITIONS:-1}"
EXPECTED_REPLICATION=1

KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

# Consulta os tópicos existentes. Falhas de conexão interrompem
# a execução, em vez de serem confundidas com tópico ausente.
existing_topics=$(
    "$KAFKA_TOPICS" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --list
)

if ! printf '%s\n' "$existing_topics" | grep -Fxq "$TOPIC"; then
    echo "Criando tópico: $TOPIC"

    "$KAFKA_TOPICS" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --create \
        --if-not-exists \
        --topic "$TOPIC" \
        --partitions "$EXPECTED_PARTITIONS" \
        --replication-factor "$EXPECTED_REPLICATION" \
        --config "retention.ms=86400000" \
        --config "retention.bytes=268435456"
fi

# Obtém a configuração efetiva do tópico.
description=$(
    "$KAFKA_TOPICS" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --describe \
        --topic "$TOPIC"
)

# Extrai as propriedades do cabeçalho retornado pelo Kafka.
if [[ "$description" =~ PartitionCount:[[:space:]]*([0-9]+) ]]; then
    actual_partitions="${BASH_REMATCH[1]}"
else
    echo "ERRO: não foi possível identificar as partições."
    exit 1
fi

if [[ "$description" =~ ReplicationFactor:[[:space:]]*([0-9]+) ]]; then
    actual_replication="${BASH_REMATCH[1]}"
else
    echo "ERRO: não foi possível identificar a replicação."
    exit 1
fi

if [[ "$actual_partitions" -ne "$EXPECTED_PARTITIONS" ||
      "$actual_replication" -ne "$EXPECTED_REPLICATION" ]]; then
    echo "ERRO: configuração do tópico diverge do contrato."
    exit 1
fi

# A retenção é parte do contrato operacional do tópico.
# Consultamos as configurações efetivas, incluindo valores herdados.
config_description=$(
    /opt/kafka/bin/kafka-configs.sh \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --entity-type topics \
        --entity-name "$TOPIC" \
        --describe --all
)

for expected in "retention.ms=86400000" "retention.bytes=268435456"; do
    if ! printf '%s\\n' "$config_description" | grep -Fq "$expected"; then
        echo "ERRO: retenção divergente para $TOPIC: $expected"
        exit 1
    fi
done

echo "OK: tópico $TOPIC validado."
echo "Partições: $actual_partitions"
echo "Replicação: $actual_replication"
