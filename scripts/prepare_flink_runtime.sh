#!/usr/bin/env bash
set -Eeuo pipefail

# Prepara os arquivos compartilhados entre o usuário Linux
# e o usuário não privilegiado do Apache Flink nos containers.
#
# Compatível com Oracle Linux 9 e outros sistemas com ACL POSIX.
# O Compose gerencia os rótulos SELinux usando :z.
#
# Não remove, formata ou sobrescreve dados existentes.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME="${PROJECT_ROOT}/runtime/flink"
FLINK_UID="${FLINK_UID:-9999}"

if ! [[ "$FLINK_UID" =~ ^[0-9]+$ ]] || [[ "$FLINK_UID" == "0" ]]; then
    echo "ERROR=INVALID_FLINK_UID" >&2
    exit 1
fi

if ! command -v setfacl >/dev/null 2>&1; then
    echo "ERROR=SETFACL_NOT_INSTALLED" >&2
    exit 1
fi

# Corrige somente bibliotecas públicas de runtime.
# Não modifica arquivos de configuração ou credenciais.
for jar in \
    "${PROJECT_ROOT}/connectors/flink-sql-connector-kafka-5.0.0-2.2.jar" \
    "${RUNTIME}/connectors/paimon-flink-2.2-1.4.2.jar" \
    "${RUNTIME}/connectors/paimon-hadoop-uber-1.4.2.jar"; do
    test -f "$jar"
    test ! -L "$jar"
    chmod 644 "$jar"
done

mkdir -p \
    "${RUNTIME}/paimon" \
    "${RUNTIME}/checkpoints" \
    "${RUNTIME}/savepoints"

# Acesso efetivo do UID Flink aos diretórios existentes.
sudo setfacl -R -m "u:${FLINK_UID}:rwX" "$RUNTIME"

# ACL padrão em diretórios: novas execuções E2E também
# poderão ser acessadas pelo usuário interno do Flink.
sudo find "$RUNTIME" -type d -exec \
    setfacl -m "d:u:${FLINK_UID}:rwx" {} +

echo "FLINK_RUNTIME_PREPARED=True"
echo "FLINK_UID=${FLINK_UID}"
