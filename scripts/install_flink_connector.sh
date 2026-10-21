#!/usr/bin/env bash
set -euo pipefail

# Versão homologada durante os testes Kafka -> Flink -> Bronze.
ARTIFACT="flink-sql-connector-kafka-5.0.0-2.2.jar"
EXPECTED_SHA256="5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616"

# Resolve o diretório do projeto independentemente de onde
# o usuário execute este script.
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONNECTOR_DIR="${PROJECT_ROOT}/connectors"
DESTINATION="${CONNECTOR_DIR}/${ARTIFACT}"

URL="https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/${ARTIFACT}"

mkdir -p "$CONNECTOR_DIR"

verify_checksum() {
    echo "${EXPECTED_SHA256}  $1" | sha256sum --check --status
}

# Evita downloads desnecessários e rejeita arquivos locais incorretos.
if [[ -f "$DESTINATION" ]]; then
    if verify_checksum "$DESTINATION"; then
        echo "CONNECTOR_STATUS=ALREADY_VALID"
        exit 0
    fi

    echo "ERROR=EXISTING_CONNECTOR_CHECKSUM_MISMATCH" >&2
    exit 1
fi

# O arquivo temporário evita que um download incompleto seja
# confundido com um conector válido.
TEMP_FILE="$(mktemp "${CONNECTOR_DIR}/.flink-kafka.XXXXXX")"
trap 'rm -f "$TEMP_FILE"' EXIT

curl --fail --location --show-error --silent \
    --retry 3 \
    --connect-timeout 15 \
    --max-time 180 \
    "$URL" \
    --output "$TEMP_FILE"

if ! verify_checksum "$TEMP_FILE"; then
    echo "ERROR=DOWNLOADED_CONNECTOR_CHECKSUM_MISMATCH" >&2
    exit 1
fi

chmod 644 "$TEMP_FILE"
mv "$TEMP_FILE" "$DESTINATION"

echo "CONNECTOR_STATUS=INSTALLED_AND_VERIFIED"
