#!/usr/bin/env bash
set -euo pipefail

# Instala os conectores Paimon homologados para o Flink 2.2.1.
# Não modifica containers, jobs, warehouses ou dados existentes.
#
# Segurança:
# - Versões e SHA-256 fixados.
# - Arquivos existentes são validados, nunca sobrescritos.
# - Downloads utilizam arquivos temporários.
# - Um download incompleto não fica disponível para o Flink.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONNECTOR_DIR="${PROJECT_ROOT}/runtime/flink/connectors"
MAVEN_BASE="https://repo.maven.apache.org/maven2/org/apache/paimon"

PAIMON_FLINK="paimon-flink-2.2-1.4.2.jar"
PAIMON_HADOOP="paimon-hadoop-uber-1.4.2.jar"

PAIMON_FLINK_SHA256="8398030a64f632fff29a3b0b310e44a84db45e0297d7ced642bc159bff222e48"
PAIMON_HADOOP_SHA256="7a0190171351a99282f720d593850631fd26119988a5ece17e77ca020cba2ca8"

verify_checksum() {
    local expected="$1"
    local file="$2"

    printf '%s  %s\n' "$expected" "$file" \
        | sha256sum --check --status
}

install_connector() {
    local artifact="$1"
    local expected_sha256="$2"
    local url="$3"
    local destination="${CONNECTOR_DIR}/${artifact}"
    local temporary

    if [[ -f "$destination" ]]; then
        if verify_checksum "$expected_sha256" "$destination"; then
            echo "CONNECTOR=${artifact}"
            echo "STATUS=ALREADY_VALID"
            return 0
        fi

        echo "ERROR=EXISTING_CHECKSUM_MISMATCH:${artifact}" >&2
        return 1
    fi

    # Não seguir links simbólicos inesperados no diretório de destino.
    if [[ -e "$destination" || -L "$destination" ]]; then
        echo "ERROR=UNEXPECTED_CONNECTOR_PATH:${artifact}" >&2
        return 1
    fi

    temporary="$(mktemp "${CONNECTOR_DIR}/.paimon-download.XXXXXX")"

    if ! curl --fail --location --show-error --silent \
        --retry 3 \
        --connect-timeout 15 \
        --max-time 240 \
        "$url" \
        --output "$temporary"; then
        rm -f "$temporary"
        echo "ERROR=DOWNLOAD_FAILED:${artifact}" >&2
        return 1
    fi

    if ! verify_checksum "$expected_sha256" "$temporary"; then
        rm -f "$temporary"
        echo "ERROR=DOWNLOADED_CHECKSUM_MISMATCH:${artifact}" >&2
        return 1
    fi

    # O nome final só aparece depois do arquivo estar completo
    # e sua integridade ter sido validada.
    if [[ -e "$destination" || -L "$destination" ]]; then
        rm -f "$temporary"
        echo "ERROR=DESTINATION_CREATED_CONCURRENTLY:${artifact}" >&2
        return 1
    fi

    chmod 644 "$temporary"
    mv -n "$temporary" "$destination"

    # Segunda verificação protege contra condições de corrida locais.
    if ! verify_checksum "$expected_sha256" "$destination"; then
        echo "ERROR=FINAL_CHECKSUM_MISMATCH:${artifact}" >&2
        return 1
    fi

    echo "CONNECTOR=${artifact}"
    echo "STATUS=INSTALLED_AND_VERIFIED"
}

mkdir -p "$CONNECTOR_DIR"

# Validar todos os arquivos existentes ANTES de tentar
# instalar qualquer dependência ausente.
if [[ -e "${CONNECTOR_DIR}/${PAIMON_FLINK}" ]]; then
    verify_checksum "$PAIMON_FLINK_SHA256" \
        "${CONNECTOR_DIR}/${PAIMON_FLINK}" \
        || { echo "ERROR=PAIMON_FLINK_CHECKSUM_MISMATCH" >&2; exit 1; }
fi

if [[ -e "${CONNECTOR_DIR}/${PAIMON_HADOOP}" ]]; then
    verify_checksum "$PAIMON_HADOOP_SHA256" \
        "${CONNECTOR_DIR}/${PAIMON_HADOOP}" \
        || { echo "ERROR=PAIMON_HADOOP_CHECKSUM_MISMATCH" >&2; exit 1; }
fi

install_connector \
    "$PAIMON_FLINK" \
    "$PAIMON_FLINK_SHA256" \
    "${MAVEN_BASE}/paimon-flink-2.2/1.4.2/${PAIMON_FLINK}"

install_connector \
    "$PAIMON_HADOOP" \
    "$PAIMON_HADOOP_SHA256" \
    "${MAVEN_BASE}/paimon-hadoop-uber/1.4.2/${PAIMON_HADOOP}"

echo "PAIMON_CONNECTORS_READY=True"
