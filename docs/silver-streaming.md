# Silver Streaming V1

## Objetivo

Processar continuamente registros do tópico Kafka
`bluesky.posts.raw` utilizando Apache Flink SQL.

O pipeline aplica o contrato de qualidade da Silver bounded
e produz dois destinos:

- **Silver válida:** registros que satisfazem o contrato.
- **Quarentena:** registros rejeitados com motivo explícito.

Ambos os destinos preservam as coordenadas Kafka necessárias
para reconciliação por partição e offset.

## Arquitetura

```text
Kafka: bluesky.posts.raw
          |
          v
    Flink SQL Streaming
          |
          +--> Classificação válida --> Silver JSON
          |
          +--> Rejeição -------------> Quarentena JSON
```

O pipeline utiliza `EXECUTE STATEMENT SET` para compartilhar
a origem entre os dois destinos.

## Pré-requisitos

- Ambiente Docker Compose do projeto operacional.
- Kafka acessível em `127.0.0.1:9092`.
- Flink JobManager REST acessível em `127.0.0.1:8081`.
- Pelo menos um slot Flink disponível.
- Ambiente virtual Python com dependências do projeto.
- Permissões de escrita nos diretórios Flink utilizados.

A validação requer `confluent-kafka` e `pytest`.
Para instalar as dependências de desenvolvimento, utilizar
o mecanismo de instalação do projeto com o extra `dev`:

```bash
.venv/bin/python -m pip install -e ".[dev]"
```

## SQL do pipeline

Arquivo: `sql/06_kafka_to_silver_streaming.sql`.

O template utiliza:

- Origem Kafka em modo `earliest-offset`.
- Checkpoints a cada 10 segundos.
- Rolling policy com intervalo de 15 segundos.
- Verificação da política de rotação a cada 5 segundos.
- Destinos filesystem em formato JSON.

**Atenção:** iniciar o SQL padrão com `earliest-offset`
não equivale a restaurar um checkpoint ou savepoint.

Uma execução independente pode reprocessar registros antigos.
Não reutilizar destinos que já contenham dados consolidados
sem uma estratégia explícita de continuidade.

## Validação ponta a ponta

O validador está em:

`scripts/validate_silver_streaming.py`

Executar:

```bash
.venv/bin/python scripts/validate_silver_streaming.py --run
```

A opção `--run` autoriza explicitamente:

1. Validação das condições do ambiente.
2. Construção de um SQL isolado por execução.
3. Validação do plano Flink.
4. Submissão de um job streaming.
5. Publicação de cinco registros sintéticos Kafka.
6. Espera por checkpoints e arquivos finalizados.
7. Reconciliação por partição e offset Kafka.

Resultado esperado:

`SILVER_STREAMING_VALIDATION = PASSED`

São esperados dois registros válidos e três em quarentena:
`INVALID_OPERATION`, `INVALID_SEQUENCE` e
`UNSUPPORTED_SOURCE`.

O validador gera um `RUN_ID` exclusivo e preserva recibos
de publicação em `/tmp`.

**Não executar novamente** após a submissão do job se
houver erro. Inspecionar primeiro o job e os offsets já
publicados. Uma nova execução pode publicar outro lote.

## Ciclo de vida do job

O validador deixa o job em execução para comprovar que
os arquivos foram finalizados enquanto o Flink permanece
em `RUNNING`.

O encerramento é uma operação posterior e explícita.

Para encerrar, obter o Job ID exibido pelo validador e
executar, substituindo `<JOB_ID>` pelo identificador real:

```bash
docker compose -f compose.flink.yaml exec -T flink-jobmanager \
  /opt/flink/bin/flink stop --drain \
  --savepointPath file:///opt/flink/runtime/savepoints \
  <JOB_ID>
```

Após o comando, confirmar o estado `FINISHED` na API REST
e a existência de um novo diretório de savepoint com
arquivo `_metadata`.

Não utilizar `docker compose down -v` nem remover
volumes para limpar a execução.

## Testes unitários

```bash
.venv/bin/python -m pytest -q
```

O arquivo `tests/test_silver_streaming_validator.py`
verifica contratos locais de coordenadas Kafka,
classificação dos cenários sintéticos, leitura de arquivos
finalizados e isolamento dos destinos SQL.

Esses testes não exigem que o pipeline esteja em execução.

## Recuperação e consistência

Os experimentos manuais do Marco 3 demonstraram:

- Detecção da perda do TaskManager.
- Reexecução dos operadores após recuperação.
- Restauração de estado a partir de checkpoint.
- Conclusão de novos checkpoints após a recuperação.
- Processamento e reconciliação de eventos publicados
  após a recuperação.
- Encerramento controlado com savepoint.

O experimento final de recuperação reconciliou dez
posições Kafka, sendo quatro válidas e seis em quarentena,
sem posições perdidas ou duplicadas.

Essas observações **não demonstram uma garantia geral
de exactly-once** para mensagens em trânsito durante
qualquer falha.

A deduplicação lógica de postagens também não faz parte
da V1. O campo `source_seq` não constitui uma identidade
global estável da postagem.

## Limitações conhecidas

- Os destinos são arquivos JSON locais.
- A finalização de arquivos depende da rolling policy
  e da progressão dos checkpoints.
- O validador assume um ambiente Flink de desenvolvimento
  sem jobs concorrentes.
- A validação padrão não executa injeção de falha.
- Os diretórios e recibos de execução são preservados.
- O encerramento com savepoint é manual.
- Não há deduplicação lógica por postagem na V1.
- Não foi implementado tratamento de event time com
  watermarks neste marco.
- Os resultados locais não representam testes de
  resiliência distribuída em produção.

## Próximas evoluções

Aprimorar o ciclo de vida do validador, automatizar o
encerramento seguro, acrescentar testes de falhas em
trânsito e avaliar armazenamento transacional para
consumo analítico contínuo.

A adoção de novas tecnologias deverá ser justificada
por requisitos concretos de volume, consistência,
consulta e operação.


## Decisões da revisão do PR #4

### Quarentena sem payload bruto

A quarentena segue o contrato estabelecido na Silver bounded:
armazena o status de qualidade, o motivo da rejeição e as
coordenadas Kafka, sem duplicar o payload bruto.

A decisão reduz a replicação de conteúdo potencialmente
sensível em camadas adicionais.

**Limitação:** a recuperação do evento original por coordenadas
Kafka depende da retenção do tópico. Quando o evento expira,
a quarentena isoladamente não permite recuperá-lo.

Uma evolução futura deverá avaliar retenção governada da
Bronze, privacidade, controles de acesso e requisitos de
reprocessamento antes de incluir payloads na quarentena.

### Parâmetros locais

O SQL V1 foi construído para o ambiente Docker Compose
local do projeto.

O endereço `127.0.0.1:9092` e os caminhos
`file:///opt/flink/runtime/...` não representam uma
configuração universal para ambientes de produção.

Outros ambientes precisam parametrizar conectividade,
armazenamento e políticas operacionais.

### Independência de branch

O validador não exige mais a branch de desenvolvimento.
Pode ser utilizado em `main`, branches de feature ou
checkouts automatizados, desde que os serviços estejam
disponíveis.

A publicação de cinco eventos continua exigindo
a opção explícita `--run`.

### Inicialização Kafka e recuperação

O modo `earliest-offset` presente no template inicia uma **nova
execução sem estado recuperado** a partir do início disponível do
tópico Kafka. Isso é intencional para experimentos locais de replay.

Esse comportamento não deve ser confundido com a restauração de
um checkpoint ou savepoint do Flink, que recupera também as posições
de leitura armazenadas no estado do job.

Submeter novamente o SQL sem restaurar estado pode reprocessar
offsets históricos e produzir arquivos duplicados. Reutilizar um
`group.id` Kafka, isoladamente, não garante continuidade exatamente
uma vez. Para retomar processamento, usar uma estratégia explícita
de recuperação de estado e gerenciamento dos destinos.
