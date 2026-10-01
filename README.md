# Pipeline: S3 → EventBridge → AWS Batch → SQS → Lambda → DynamoDB

## Arquitetura

1. Um arquivo `.txt` é enviado ao bucket S3.
2. O S3 emite um evento `Object Created` para o **EventBridge** (bus padrão).
3. Uma regra do EventBridge dispara um **AWS Batch job** (Fargate), passando `bucket` e `key` como parâmetros.
4. O job (container Python) baixa o arquivo do S3, quebra em linhas e envia cada linha como mensagem para uma fila **SQS**.
5. Uma **Lambda**, acionada pela fila SQS, grava cada linha como um item no **DynamoDB**.

## Pré-requisitos

- Terraform >= 1.5
- AWS CLI configurado (`aws configure`) com permissões de administrador (ou equivalentes a IAM, S3, Batch, ECR, SQS, Lambda, DynamoDB, EventBridge)
- Docker instalado (para build da imagem do Batch)

## Passo a passo

### 1. Deploy da infraestrutura base

```bash
cd tf-pipeline
terraform init
terraform apply
```

Na primeira vez isso cria tudo, **inclusive o repositório ECR vazio**. O Job Definition do Batch aponta para `<repo>:latest`, mas essa imagem ainda não existe — o job vai falhar até você fazer o passo 2.

### 2. Build e push da imagem do Batch

```bash
REGION=$(terraform output -raw ecr_repository_url | cut -d. -f4)
REPO_URL=$(terraform output -raw ecr_repository_url)

aws ecr get-login-password --region $REGION | \
  docker login --username AWS --password-stdin $(echo $REPO_URL | cut -d/ -f1)

cd batch-job
docker build -t $REPO_URL:latest .
docker push $REPO_URL:latest
cd ..
```

### 3. Testar o pipeline

Crie um arquivo de teste e envie ao bucket:

```bash
BUCKET=$(terraform output -raw upload_bucket_name)

printf "linha 1\nlinha 2\nlinha 3\n" > teste.txt
aws s3 cp teste.txt s3://$BUCKET/teste.txt
```

Acompanhe:

```bash
# Ver o job do Batch sendo criado/rodando
aws batch list-jobs --job-queue $(terraform output -raw batch_job_queue_name) --job-status RUNNING

# Ver logs do job no CloudWatch (grupo /aws/batch/job)
aws logs tail /aws/batch/job --follow

# Ver logs da Lambda
aws logs tail /aws/lambda/$(terraform output -raw lambda_function_name) --follow

# Conferir os itens gravados no DynamoDB
aws dynamodb scan --table-name $(terraform output -raw dynamodb_table_name)
```

Se tudo estiver certo, em alguns segundos as 3 linhas do arquivo devem aparecer como itens no DynamoDB.

## Observações importantes

- **Mecanismo bucket/key → Batch**: o EventBridge usa um `input_transformer` para extrair `bucket` e `key` do evento S3 e injetá-los como *Parameters* do job (`Ref::bucket`, `Ref::key` no `command`). Esse é o padrão oficial da AWS para passar dados do evento para o Batch.
- **VPC**: o projeto usa a VPC e as subnets *default* da sua conta/região, com uma tarefa Fargate com IP público, apenas para simplificar. Em produção, prefira subnets privadas + NAT Gateway ou VPC endpoints para S3/SQS/ECR.
- **Formato do arquivo**: o script assume texto simples, uma linha = um registro. Se o arquivo for CSV/JSON, ajuste `process_file.py`.
- **Duplicidade/erros**: mensagens que falharem 5x na Lambda vão para a DLQ `linepipe-lines-dlq`.
- **Custos**: Fargate, Lambda, DynamoDB on-demand e SQS têm custo por uso — bucket vazio e sem tráfego custam próximo de zero.

## Limpeza

```bash
terraform destroy
```

# Título

Criar estrutura DynamoDB para operações de registro em Clearing

# Objetivo

Criar, através de Terraform, a estrutura DynamoDB responsável por armazenar operações financeiras destinadas a registro em clearing, o estado atual do registro e o histórico dos eventos ocorridos durante o processo.

A solução deverá suportar múltiplos produtos e múltiplas clearings, respeitando a seguinte regra de negócio:

> Cada operação deverá possuir exatamente uma clearing de destino.

O conceito de multi-clearing significa que a plataforma poderá registrar operações em diferentes clearings, mas uma determinada operação pertencerá a apenas uma delas.

Exemplo:

```text id="hldctn"
OP001 → B3
OP002 → B3
OP003 → CLEARING_X
```

Não será permitido:

```text id="ifau7b"
             ┌── B3
OP001 ───────┤
             └── CLEARING_X
```

Inicialmente serão processadas operações de RDB destinadas à B3. O modelo deverá permitir posteriormente CDB, novos produtos e novas clearings sem alteração da estrutura principal de PK/SK.

As operações poderão ter origem em arquivo CSV e, futuramente, Kafka. O modelo persistido deverá ser independente do formato dessas fontes.

---

# Descrição detalhada

## 1. Modelo conceitual

O modelo será composto por três entidades:

```text id="nffz8j"
OPERACAO
    │
    │ 1:1
    ▼
REGISTRO_CLEARING
    │
    │ 1:N
    ▼
EVENTO_REGISTRO
```

### OPERACAO

Representa o fato financeiro que deverá ser registrado.

Responde:

> O que precisa ser registrado e para qual clearing?

### REGISTRO_CLEARING

Representa o processo de registro da operação na clearing determinada.

Responde:

> Qual é a situação atual do registro?

### EVENTO_REGISTRO

Representa os acontecimentos ocorridos durante o ciclo de vida do registro.

Responde:

> O que aconteceu com esse registro?

Os eventos deverão possuir comportamento append-only.

---

# 2. Modelo canônico e origens

O sistema poderá receber operações através de diferentes fontes.

Inicialmente:

```text id="t3odmn"
CSV
```

Futuramente:

```text id="imvh1x"
Kafka
```

As fontes de entrada não deverão determinar a estrutura da persistência.

Cada entrada deverá ser transformada por seu respectivo adaptador para o modelo canônico do sistema:

```text id="cp1cbp"
CSV
 │
 ▼
Adaptador CSV
 │
 └────────────┐
              ▼
           OPERACAO
              ▲
 ┌────────────┘
 │
Adaptador Kafka
 ▲
 │
Kafka
```

Dessa forma, mudanças no formato do CSV ou no contrato Kafka não deverão obrigatoriamente alterar o modelo interno de clearing.

---

# 3. Regra de clearing única

Cada `OPERACAO` deverá possuir exatamente uma clearing de destino.

A relação será:

```text id="r4dvcp"
OPERACAO 1 ───────── 1 REGISTRO_CLEARING
```

A operação deverá possuir:

```text id="jxpgxo"
clearingDestino
```

Exemplo:

```json id="hx9dnv"
{
  "idOperacao": "OP-000001",
  "tipoOperacao": "APLICACAO",
  "produto": "RDB",
  "clearingDestino": "B3",
  "valor": 15000.50
}
```

---

# 4. Estrutura física DynamoDB

Será utilizada inicialmente uma única tabela física:

```text id="1gg6xs"
OPERACOES_CLEARING
```

utilizando Single Table Design.

A tabela possuirá chave composta:

```text id="a1ltsp"
Partition Key + Sort Key
```

Os atributos físicos serão:

| Tipo | Campo | Tipo DynamoDB |
|---|---|---|
| Partition Key — PK | `chaveParticao` | String |
| Sort Key — SK | `chaveOrdenacao` | String |

Portanto:

```text id="bfb50h"
PK = chaveParticao
SK = chaveOrdenacao
```

---

# 5. Como funcionam PK e SK neste modelo

## Partition Key — PK

A PK identifica:

> De qual operação estamos falando?

Será utilizada:

```text id="vxy4fb"
OPERACAO#<idOperacao>
```

Exemplo:

```text id="ooh1o8"
OPERACAO#OP-000001
```

Todos os itens relacionados à mesma operação possuirão a mesma PK.

---

## Sort Key — SK

A SK identifica:

> Qual informação dessa operação este item representa?

Serão utilizadas as seguintes convenções:

### Operação

```text id="pc2s74"
SK = OPERACAO
```

### Registro atual

```text id="5c54aj"
SK = REGISTRO
```

### Eventos

```text id="tgjw5q"
SK =
REGISTRO#EVENTO#<timestamp>#<idEvento>
```

Portanto:

```text id="7gzrsf"
PK = OPERACAO#OP001
│
├── SK = OPERACAO
├── SK = REGISTRO
├── SK = REGISTRO#EVENTO#...#EVT001
├── SK = REGISTRO#EVENTO#...#EVT002
└── SK = REGISTRO#EVENTO#...#EVT003
```

Em termos simples:

```text id="84hnv6"
PK
"De qual operação é?"

SK
"O que é dentro dessa operação?"
```

---

# 6. Convenção final das chaves

| Entidade | PK | SK |
|---|---|---|
| `OPERACAO` | `OPERACAO#<idOperacao>` | `OPERACAO` |
| `REGISTRO_CLEARING` | `OPERACAO#<idOperacao>` | `REGISTRO` |
| `EVENTO_REGISTRO` | `OPERACAO#<idOperacao>` | `REGISTRO#EVENTO#<timestamp>#<idEvento>` |

Essa convenção deverá ser utilizada de forma consistente pela aplicação.

---

# 7. OPERACAO

Representa o fato financeiro recebido pelo sistema.

Exemplos:

```text id="hjnsdv"
Aplicação de RDB
Resgate de RDB
Aplicação de CDB
Resgate de CDB
```

Chaves:

```text id="5dt02m"
PK = OPERACAO#<idOperacao>
SK = OPERACAO
```

Principais atributos:

| Campo | Finalidade |
|---|---|
| `idOperacao` | Identificador interno |
| `tipoOperacao` | APLICAÇÃO, RESGATE etc. |
| `produto` | RDB, CDB etc. |
| `clearingDestino` | Única clearing da operação |
| `valor` | Valor financeiro |
| `dataOperacao` | Data da operação |
| `tipoOrigem` | ARQUIVO, KAFKA etc. |
| `idOperacaoOrigem` | Identificador recebido da origem |
| `chaveIdempotencia` | Identidade lógica para deduplicação |
| `dadosProduto` | Informações específicas do produto |
| `dataHoraCriacao` | Data/hora de criação |

---

# 8. REGISTRO_CLEARING

Representa o snapshot atual do processo de registro.

Chaves:

```text id="vujq4j"
PK = OPERACAO#<idOperacao>
SK = REGISTRO
```

Como existe apenas uma clearing por operação, deverá existir no máximo um item `REGISTRO_CLEARING` por operação.

Principais atributos:

| Campo | Finalidade |
|---|---|
| `idRegistro` | Identificador interno do registro |
| `idOperacao` | Operação relacionada |
| `clearing` | Clearing utilizada |
| `status` | Estado atual |
| `idExterno` | Identificador da integração |
| `protocoloExterno` | Protocolo externo, quando existente |
| `tentativa` | Controle de tentativas |
| `dataHoraEnvio` | Momento do envio |
| `dataHoraRetorno` | Momento do retorno |
| `dataHoraAtualizacao` | Última atualização |

---

# 9. EVENTO_REGISTRO

Representa acontecimentos durante o ciclo de vida do registro.

Chaves:

```text id="3x0a7c"
PK = OPERACAO#<idOperacao>

SK =
REGISTRO#EVENTO#<timestamp>#<idEvento>
```

Principais atributos:

| Campo | Finalidade |
|---|---|
| `idEvento` | Identificador único |
| `idRegistro` | Registro relacionado |
| `idOperacao` | Operação relacionada |
| `tipoEvento` | Acontecimento ocorrido |
| `statusAnterior` | Estado anterior, quando aplicável |
| `statusAtual` | Estado resultante, quando aplicável |
| `origemEvento` | Origem do acontecimento |
| `dataHoraEvento` | Momento do evento |

Os eventos deverão ser append-only.

---

# 10. GSI — identificador externo

Deverá existir inicialmente:

```text id="q98zz6"
INDICE_ID_EXTERNO
```

com os atributos físicos:

```text id="rdr41s"
GSI PK =
chaveParticaoIndiceIdExterno

GSI SK =
chaveOrdenacaoIndiceIdExterno
```

Para `REGISTRO_CLEARING`:

```text id="i2df4b"
GSI PK =
ID_EXTERNO#<idExterno>

GSI SK =
REGISTRO#<idOperacao>
```

Exemplo:

```text id="f3y36e"
ID_EXTERNO#CLEARING-000001
REGISTRO#OP-000001
```

Somente itens que possuírem esses atributos participarão do índice.

Inicialmente serão apenas itens `REGISTRO_CLEARING`.

---

# 11. Padrões de consulta

## AP01 — Buscar operação

```text id="ynodjz"
PK = OPERACAO#<idOperacao>
SK = OPERACAO
```

---

## AP02 — Buscar registro atual

```text id="3k0gln"
PK = OPERACAO#<idOperacao>
SK = REGISTRO
```

---

## AP03 — Buscar operação completa

```text id="kac59d"
PK = OPERACAO#<idOperacao>
```

Retorna:

```text id="rr29q7"
OPERACAO
REGISTRO
EVENTOS
```

---

## AP04 — Buscar histórico

```text id="7msixg"
PK = OPERACAO#<idOperacao>

SK begins_with
REGISTRO#EVENTO#
```

---

## AP05 — Buscar último evento

```text id="hqd5wo"
PK = OPERACAO#<idOperacao>

SK begins_with
REGISTRO#EVENTO#

ScanIndexForward = false

Limit = 1
```

---

## AP06 — Buscar pelo identificador externo

No:

```text id="n88q9x"
INDICE_ID_EXTERNO
```

consultar:

```text id="1eyvyb"
GSI PK =
ID_EXTERNO#<idExterno>
```

Retornando o `REGISTRO_CLEARING` correspondente.

---

# 12. POC — Massa de dados

A POC deverá possuir inicialmente três cenários distintos:

```text id="lv5mcz"
OP001
Aplicação RDB
Arquivo
B3
ACEITO

OP002
Resgate RDB
Arquivo
B3
REJEITADO

OP003
Aplicação CDB
Kafka
CLEARING_X
PENDENTE
```

O terceiro cenário tem como objetivo validar a flexibilidade futura do modelo.

---

# 13. POC — Cenário 1: aplicação RDB aceita pela B3

## OPERACAO

```json id="b1gb2n"
{
  "chaveParticao": "OPERACAO#OP-000001",
  "chaveOrdenacao": "OPERACAO",

  "tipoEntidade": "OPERACAO",

  "idOperacao": "OP-000001",
  "tipoOperacao": "APLICACAO",
  "produto": "RDB",
  "clearingDestino": "B3",

  "valor": 15000.50,
  "dataOperacao": "2026-10-01",

  "tipoOrigem": "ARQUIVO",
  "idOperacaoOrigem": "ARQ-987654",

  "chaveIdempotencia": "ARQUIVO#ARQ-987654",

  "dadosProduto": {
    "numeroRdb": "RDB-987654",
    "indexador": "CDI",
    "taxa": 102.5,
    "dataVencimento": "2027-10-01"
  },

  "dataHoraCriacao": "2026-10-01T10:00:00Z"
}
```

## REGISTRO_CLEARING

```json id="3y2igq"
{
  "chaveParticao": "OPERACAO#OP-000001",
  "chaveOrdenacao": "REGISTRO",

  "tipoEntidade": "REGISTRO_CLEARING",

  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",

  "clearing": "B3",

  "status": "ACEITO",

  "idExterno": "CLEARING-000001",
  "protocoloExterno": "B3-PROT-987654",

  "tentativa": 1,

  "chaveParticaoIndiceIdExterno":
    "ID_EXTERNO#CLEARING-000001",

  "chaveOrdenacaoIndiceIdExterno":
    "REGISTRO#OP-000001",

  "dataHoraEnvio": "2026-10-01T10:01:00Z",
  "dataHoraRetorno": "2026-10-01T10:05:00Z",
  "dataHoraAtualizacao": "2026-10-01T10:05:01Z"
}
```

## EVENTO — criado

```json id="pf4i73"
{
  "chaveParticao": "OPERACAO#OP-000001",
  "chaveOrdenacao":
    "REGISTRO#EVENTO#20261001T100030.000Z#EVT-001",

  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "idEvento": "EVT-001",

  "tipoEvento": "REGISTRO_CRIADO",
  "statusAtual": "PENDENTE",

  "origemEvento": "SISTEMA_CLEARING",

  "dataHoraEvento": "2026-10-01T10:00:30Z"
}
```

## EVENTO — enviado

```json id="smg3e4"
{
  "chaveParticao": "OPERACAO#OP-000001",
  "chaveOrdenacao":
    "REGISTRO#EVENTO#20261001T100100.000Z#EVT-002",

  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "idEvento": "EVT-002",

  "tipoEvento": "ENVIO_REALIZADO",

  "statusAnterior": "PENDENTE",
  "statusAtual": "ENVIADO",

  "origemEvento": "REGISTRATION_WORKER",

  "dataHoraEvento": "2026-10-01T10:01:00Z"
}
```

## EVENTO — retorno recebido

```json id="wruee1"
{
  "chaveParticao": "OPERACAO#OP-000001",
  "chaveOrdenacao":
    "REGISTRO#EVENTO#20261001T100500.000Z#EVT-003",

  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "idEvento": "EVT-003",

  "tipoEvento": "RETORNO_RECEBIDO",

  "origemEvento": "PISMO",

  "idEventoExterno": "PISMO-EVT-789",

  "dataHoraEvento": "2026-10-01T10:05:00Z"
}
```

## EVENTO — aceito

```json id="6khy81"
{
  "chaveParticao": "OPERACAO#OP-000001",
  "chaveOrdenacao":
    "REGISTRO#EVENTO#20261001T100501.000Z#EVT-004",

  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "idEvento": "EVT-004",

  "tipoEvento": "STATUS_ALTERADO",

  "statusAnterior": "ENVIADO",
  "statusAtual": "ACEITO",

  "origemEvento": "RETORNO_CLEARING",

  "dataHoraEvento": "2026-10-01T10:05:01Z"
}
```

---

# 14. POC — Cenário 2: resgate RDB rejeitado

## OPERACAO

```json id="8b5kmj"
{
  "chaveParticao": "OPERACAO#OP-000002",
  "chaveOrdenacao": "OPERACAO",

  "tipoEntidade": "OPERACAO",

  "idOperacao": "OP-000002",
  "tipoOperacao": "RESGATE",
  "produto": "RDB",
  "clearingDestino": "B3",

  "valor": 8000.00,
  "dataOperacao": "2026-10-01",

  "tipoOrigem": "ARQUIVO",
  "idOperacaoOrigem": "ARQ-987655",

  "chaveIdempotencia": "ARQUIVO#ARQ-987655",

  "dadosProduto": {
    "numeroRdb": "RDB-123456"
  },

  "dataHoraCriacao": "2026-10-01T11:00:00Z"
}
```

## REGISTRO_CLEARING

```json id="wdv45v"
{
  "chaveParticao": "OPERACAO#OP-000002",
  "chaveOrdenacao": "REGISTRO",

  "tipoEntidade": "REGISTRO_CLEARING",

  "idOperacao": "OP-000002",
  "idRegistro": "REG-000002",

  "clearing": "B3",

  "status": "REJEITADO",

  "idExterno": "CLEARING-000002",

  "tentativa": 1,

  "codigoRetorno": "B3-001",
  "descricaoRetorno": "Operacao rejeitada pela clearing",

  "chaveParticaoIndiceIdExterno":
    "ID_EXTERNO#CLEARING-000002",

  "chaveOrdenacaoIndiceIdExterno":
    "REGISTRO#OP-000002",

  "dataHoraEnvio": "2026-10-01T11:01:00Z",
  "dataHoraRetorno": "2026-10-01T11:04:00Z",
  "dataHoraAtualizacao": "2026-10-01T11:04:01Z"
}
```

## EVENTO — rejeitado

```json id="5cl28p"
{
  "chaveParticao": "OPERACAO#OP-000002",
  "chaveOrdenacao":
    "REGISTRO#EVENTO#20261001T110401.000Z#EVT-103",

  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000002",
  "idRegistro": "REG-000002",
  "idEvento": "EVT-103",

  "tipoEvento": "STATUS_ALTERADO",

  "statusAnterior": "ENVIADO",
  "statusAtual": "REJEITADO",

  "codigoRetorno": "B3-001",
  "descricaoRetorno": "Operacao rejeitada pela clearing",

  "origemEvento": "RETORNO_CLEARING",

  "dataHoraEvento": "2026-10-01T11:04:01Z"
}
```

---

# 15. POC — Cenário 3: futura operação Kafka/CDB

Este cenário será utilizado somente para validar que o modelo não está acoplado a:

```text id="l2ab8i"
RDB
+
CSV
+
B3
```

## OPERACAO

```json id="ehtm6q"
{
  "chaveParticao": "OPERACAO#OP-000003",
  "chaveOrdenacao": "OPERACAO",

  "tipoEntidade": "OPERACAO",

  "idOperacao": "OP-000003",
  "tipoOperacao": "APLICACAO",

  "produto": "CDB",

  "clearingDestino": "CLEARING_X",

  "valor": 25000.00,
  "dataOperacao": "2026-10-01",

  "tipoOrigem": "KAFKA",
  "idOperacaoOrigem": "KAFKA-123456",

  "chaveIdempotencia": "KAFKA#KAFKA-123456",

  "dadosProduto": {
    "codigoCdb": "CDB-000123",
    "indexador": "CDI",
    "taxa": 105.0
  },

  "dataHoraCriacao": "2026-10-01T12:00:00Z"
}
```

## REGISTRO_CLEARING

```json id="y42n4m"
{
  "chaveParticao": "OPERACAO#OP-000003",
  "chaveOrdenacao": "REGISTRO",

  "tipoEntidade": "REGISTRO_CLEARING",

  "idOperacao": "OP-000003",
  "idRegistro": "REG-000003",

  "clearing": "CLEARING_X",

  "status": "PENDENTE",

  "tentativa": 0,

  "dataHoraAtualizacao": "2026-10-01T12:00:01Z"
}
```

Nesse momento ainda não existe `idExterno`.

Consequentemente, esse item ainda não participa do:

```text id="g2dyhf"
INDICE_ID_EXTERNO
```

Isso demonstra também o comportamento de índice esparso.

---

# 16. POC — Cenários de consulta

A POC deverá validar os seguintes cenários.

## Cenário A — localizar somente a operação

Objetivo:

> Quero os dados financeiros da OP-000001.

Consulta:

```text id="smg40p"
PK = OPERACAO#OP-000001
SK = OPERACAO
```

Resultado esperado:

```text id="26nv2d"
tipoOperacao = APLICACAO
produto = RDB
clearingDestino = B3
valor = 15000.50
```

---

## Cenário B — localizar estado atual do registro

Objetivo:

> Como está atualmente a OP-000001 na clearing?

Consulta:

```text id="ocnuxk"
PK = OPERACAO#OP-000001
SK = REGISTRO
```

Resultado:

```text id="k90uj7"
clearing = B3
status = ACEITO
idExterno = CLEARING-000001
```

---

## Cenário C — carregar tudo da operação

Objetivo:

> Quero investigar completamente a OP-000001.

Consulta:

```text id="o7i5e9"
PK = OPERACAO#OP-000001
```

Resultado esperado:

```text id="0iqab9"
OPERACAO
REGISTRO
REGISTRO_CRIADO
ENVIO_REALIZADO
RETORNO_RECEBIDO
STATUS_ALTERADO
```

Esse padrão é particularmente útil para troubleshooting.

---

## Cenário D — consultar somente timeline

Objetivo:

> Quero saber tudo que aconteceu durante o registro.

Consulta:

```text id="14s4aq"
PK = OPERACAO#OP-000001

SK begins_with
REGISTRO#EVENTO#
```

Resultado:

```text id="9q11rp"
10:00:30 REGISTRO_CRIADO
10:01:00 ENVIO_REALIZADO
10:05:00 RETORNO_RECEBIDO
10:05:01 STATUS_ALTERADO → ACEITO
```

---

## Cenário E — consultar último evento

Objetivo:

> Qual foi o último acontecimento?

Consulta:

```text id="njzpcq"
PK = OPERACAO#OP-000001

SK begins_with
REGISTRO#EVENTO#

ScanIndexForward = false

Limit = 1
```

Resultado:

```text id="cxkq6n"
STATUS_ALTERADO
ENVIADO → ACEITO
```

---

## Cenário F — localizar operação a partir do retorno externo

Objetivo:

A Pismo enviou:

```text id="svyjg9"
idExterno = CLEARING-000001
```

e o processador precisa descobrir a operação.

Consulta:

```text id="f9sk22"
INDEX =
INDICE_ID_EXTERNO

PK =
ID_EXTERNO#CLEARING-000001
```

Resultado:

```text id="4h5sbj"
REGISTRO_CLEARING

idOperacao = OP-000001
idRegistro = REG-000001
status = ACEITO
```

Fluxo validado:

```text id="oh00xl"
Pismo
   │
   │ idExterno
   ▼
SQS Retorno
   │
   ▼
Processador
   │
   ▼
INDICE_ID_EXTERNO
   │
   ▼
REGISTRO
   │
   ▼
OP-000001
```

---

# 17. POC — Queries AWS CLI de referência

As queries abaixo são apenas referências técnicas para documentação e desenvolvimento.

Não é necessário utilizar AWS CLI para executar a POC pelo Console.

## Buscar operação completa

```bash id="45c8vu"
aws dynamodb query \
  --table-name OPERACOES_CLEARING \
  --key-condition-expression \
    "chaveParticao = :pk" \
  --expression-attribute-values '{
    ":pk": {
      "S": "OPERACAO#OP-000001"
    }
  }'
```

---

## Buscar somente o histórico

```bash id="nmdkmf"
aws dynamodb query \
  --table-name OPERACOES_CLEARING \
  --key-condition-expression \
    "chaveParticao = :pk AND begins_with(chaveOrdenacao, :sk)" \
  --expression-attribute-values '{
    ":pk": {
      "S": "OPERACAO#OP-000001"
    },
    ":sk": {
      "S": "REGISTRO#EVENTO#"
    }
  }'
```

---

## Buscar último evento

```bash id="a8xwnn"
aws dynamodb query \
  --table-name OPERACOES_CLEARING \
  --key-condition-expression \
    "chaveParticao = :pk AND begins_with(chaveOrdenacao, :sk)" \
  --expression-attribute-values '{
    ":pk": {
      "S": "OPERACAO#OP-000001"
    },
    ":sk": {
      "S": "REGISTRO#EVENTO#"
    }
  }' \
  --no-scan-index-forward \
  --limit 1
```

---

## Buscar pelo idExterno

```bash id="5l4qnt"
aws dynamodb query \
  --table-name OPERACOES_CLEARING \
  --index-name INDICE_ID_EXTERNO \
  --key-condition-expression \
    "chaveParticaoIndiceIdExterno = :pk" \
  --expression-attribute-values '{
    ":pk": {
      "S": "ID_EXTERNO#CLEARING-000001"
    }
  }'
```

---

# 18. POC — Resultado visual esperado

Após inserir os itens, a OP-000001 deverá aparecer conceitualmente:

```text id="tib1y6"
OPERACAO#OP-000001
│
├── OPERACAO
│     APLICACAO
│     RDB
│     R$ 15.000,50
│     B3
│
├── REGISTRO
│     B3
│     ACEITO
│     CLEARING-000001
│
├── REGISTRO#EVENTO#...#EVT-001
│     REGISTRO_CRIADO
│
├── REGISTRO#EVENTO#...#EVT-002
│     ENVIO_REALIZADO
│
├── REGISTRO#EVENTO#...#EVT-003
│     RETORNO_RECEBIDO
│
└── REGISTRO#EVENTO#...#EVT-004
      STATUS_ALTERADO
      ACEITO
```

---

# 19. O que a POC deverá provar

A POC deverá demonstrar que:

1. Operação, registro atual e histórico podem coexistir na mesma tabela.

2. A PK agrupa todos os dados relacionados à mesma operação.

3. A SK permite identificar e consultar seletivamente cada tipo de informação.

4. É possível obter somente a operação.

5. É possível obter somente o snapshot atual do registro.

6. É possível recuperar toda a operação e seu histórico com uma Query pela PK.

7. É possível recuperar somente a timeline utilizando prefixo da SK.

8. É possível recuperar o último evento sem ler toda a timeline.

9. O retorno externo pode localizar o registro através do `INDICE_ID_EXTERNO`.

10. O modelo suporta operação originada de arquivo ou Kafka.

11. O modelo suporta RDB, CDB e futuros produtos sem alterar PK/SK.

12. O modelo suporta B3 e futuras clearings sem alterar PK/SK.

13. Uma operação pertence a apenas uma clearing.

14. `EVENTO_REGISTRO` pode permanecer append-only enquanto `REGISTRO_CLEARING` representa o estado atual.

15. Itens que ainda não possuem `idExterno` não precisam participar do GSI.

---

# Requisitos

**RF01 — Tabela**

Deverá existir:

```text id="hgt8xl"
OPERACOES_CLEARING
```

**RF02 — Chave física**

```text id="n3fwl3"
PK = chaveParticao
SK = chaveOrdenacao
```

ambas do tipo String.

**RF03 — OPERACAO**

```text id="y39oeq"
PK = OPERACAO#<idOperacao>
SK = OPERACAO
```

**RF04 — REGISTRO_CLEARING**

```text id="xq3u7e"
PK = OPERACAO#<idOperacao>
SK = REGISTRO
```

**RF05 — EVENTO_REGISTRO**

```text id="pyyp8v"
PK = OPERACAO#<idOperacao>

SK =
REGISTRO#EVENTO#<timestamp>#<idEvento>
```

**RF06 — Clearing única**

Cada operação deverá possuir exatamente uma clearing de destino.

**RF07 — Registro único**

Cada operação deverá possuir no máximo um snapshot `REGISTRO_CLEARING`.

**RF08 — Eventos**

Um registro poderá possuir múltiplos eventos.

**RF09 — Append-only**

Eventos existentes não deverão ser sobrescritos.

**RF10 — GSI**

Deverá existir:

```text id="v56f0a"
INDICE_ID_EXTERNO
```

com:

```text id="wl9frg"
PK = ID_EXTERNO#<idExterno>
SK = REGISTRO#<idOperacao>
```

**RF11 — Produtos**

Novos produtos não deverão exigir alteração da PK/SK.

**RF12 — Clearings**

Novas clearings não deverão exigir alteração da PK/SK.

**RF13 — Origens**

O modelo deverá ser independente da origem da operação.

**RF14 — Idempotência**

O modelo deverá possuir uma estratégia de idempotência que considere reprocessamento, duplicidade e concorrência.

**RF15 — Terraform**

A estrutura oficial deverá ser provisionada através de Terraform.

---

# Critérios de aceite

**CA01**

Deverá ser possível persistir uma operação utilizando:

```text id="m7zt7q"
PK = OPERACAO#<idOperacao>
SK = OPERACAO
```

**CA02**

Deverá ser possível persistir o registro atual utilizando:

```text id="v0azui"
PK = OPERACAO#<idOperacao>
SK = REGISTRO
```

**CA03**

Uma operação não deverá possuir mais de um snapshot de registro.

**CA04**

Deverá ser possível persistir múltiplos eventos utilizando:

```text id="7ip8m6"
PK = OPERACAO#<idOperacao>

SK =
REGISTRO#EVENTO#<timestamp>#<idEvento>
```

**CA05**

Eventos deverão permanecer append-only.

**CA06**

Uma Query utilizando apenas:

```text id="s8ss7a"
PK = OPERACAO#<idOperacao>
```

deverá recuperar o agregado da operação.

**CA07**

Uma consulta utilizando:

```text id="3pwbh8"
PK = OPERACAO#<idOperacao>
SK = OPERACAO
```

deverá recuperar exclusivamente a operação.

**CA08**

Uma consulta utilizando:

```text id="hrb4yy"
PK = OPERACAO#<idOperacao>
SK = REGISTRO
```

deverá recuperar o snapshot atual.

**CA09**

Uma Query utilizando:

```text id="4kxbx5"
PK = OPERACAO#<idOperacao>

SK begins_with
REGISTRO#EVENTO#
```

deverá recuperar a timeline.

**CA10**

Deverá ser possível recuperar o último evento utilizando ordenação decrescente e `Limit = 1`.

**CA11**

Deverá ser possível localizar um registro através do `idExterno` utilizando `INDICE_ID_EXTERNO`.

**CA12**

Uma operação deverá possuir exatamente uma clearing de destino.

**CA13**

A inclusão de nova clearing não deverá exigir alteração de PK/SK.

**CA14**

A inclusão de novo produto não deverá exigir alteração de PK/SK.

**CA15**

A POC deverá conter ao menos:

```text id="qoxntj"
Aplicação aceita
Resgate rejeitado
Operação de produto/origem/clearing futura
```

para demonstrar a flexibilidade da estrutura.

**CA16**

A estrutura oficial deverá ser declarada através de Terraform.

---

# Resumo final das chaves

| Item | PK | SK |
|---|---|---|
| Operação | `OPERACAO#<idOperacao>` | `OPERACAO` |
| Registro | `OPERACAO#<idOperacao>` | `REGISTRO` |
| Evento | `OPERACAO#<idOperacao>` | `REGISTRO#EVENTO#<timestamp>#<idEvento>` |

GSI:

| Índice | PK | SK |
|---|---|---|
| `INDICE_ID_EXTERNO` | `ID_EXTERNO#<idExterno>` | `REGISTRO#<idOperacao>` |

Regra mental do modelo:

```text id="i8ks98"
PK
"De qual operação estamos falando?"

SK
"O que estamos olhando dentro dessa operação?"

GSI
"Não conheço a operação,
mas conheço um identificador externo.
Como encontro o registro?"
```