# Apache Paimon — Bronze e Silver Streaming

## Escopo

O projeto utiliza Apache Kafka 4.2.2, Apache Flink 2.2.1
e Apache Paimon 1.4.2 para processamento incremental.

Fluxo projetado:

Bluesky Jetstream → coletor Python → Kafka → Flink →
Paimon Bronze → Flink → Paimon Silver / Quarentena.

A Gold ainda não foi implementada.

## Provisionamento

Instalar os conectores antes de iniciar os serviços Flink:

```bash
bash scripts/install_flink_connector.sh
bash scripts/install_paimon_connectors.sh
docker compose -p financial-streaming-platform -f compose.flink.yaml config --quiet
```

O Kafka utiliza o projeto Compose `financial-streaming`,
definido em `compose.yaml`. O Flink utiliza
`financial-streaming-platform`, definido em `compose.flink.yaml`.
Não remover volumes para atualizar ou reiniciar serviços.

Os JARs são baixados com versões fixas e verificados por SHA-256.
Os arquivos binários e os warehouses locais não são versionados.

## Contratos

`sql/07_kafka_to_paimon_bronze.sql` consome
`bluesky.posts.raw` e persiste `payload`,
`kafka_partition`, `kafka_offset` e `kafka_timestamp`.

`sql/08_paimon_bronze_to_silver.sql` lê a Bronze Paimon,
aplica as regras de classificação herdadas do SQL 06
e grava em duas tabelas Paimon: válidos e quarentena.

A Bronze é append-only e não elimina duplicações.
Replay de eventos do Bluesky pode gerar múltiplos offsets Kafka
para um mesmo evento lógico.

## Evidências obtidas

- Coletor real → broker Kafka real: aprovado com evento sintético.
- Kafka → Bronze Paimon: um evento persistido e validado.
- Bronze → Silver e quarentena: cinco eventos sintéticos,
  um válido e quatro rejeitados com motivos corretos.
- Paimon → Paimon: leitura incremental, savepoint, restauração
  e continuidade demonstrados em experimento isolado.
- Testes de regressão SQL: regras de classificação preservadas.

Essas evidências não comprovam exatamente-uma-vez ponta a ponta
sob todas as falhas, nem substituem um teste automatizado do
pipeline completo.

## Segurança e operação

Não imprimir payloads de usuários, credenciais ou cursores em logs.
Usar tópicos e warehouses isolados em testes de integração.
Não reenviar SQLs streaming cegamente: uma submissão anterior
pode ter sido aceita mesmo após erro no cliente.

Utilizar os scripts de integração com revisão do estado
registrado em `runtime/flink/paimon/`. Os scripts atuais
ainda não constituem um runner ponta a ponta reutilizável.
Não remover marcadores para forçar novas execuções sem
primeiro conferir os jobs e os dados persistidos.

## Validação local

```bash
.venv/bin/python -m pytest -q
bash -n scripts/install_flink_connector.sh
bash -n scripts/install_paimon_connectors.sh
docker compose -p financial-streaming-platform -f compose.flink.yaml config --quiet
git diff --check
```

## Pendências antes do merge

- Transformar os testes isolados em uma execução ponta a ponta
  reproduzível, com isolamento e limpeza controlada.
- Testar recuperação e reconciliação sob falhas do fluxo integrado.
- Definir e testar a deduplicação lógica de eventos Bluesky.
- Revisar os scripts de integração para permitir múltiplas execuções
  com identificação segura e sem duplicação acidental.
- Validar instalação dos conectores em ambiente vazio.
