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

A solução deverá suportar múltiplos produtos e múltiplas clearings, respeitando a regra:

> Cada operação deverá possuir exatamente uma clearing de destino.

O conceito de multi-clearing significa que a plataforma poderá registrar operações em diferentes clearings, mas cada operação individual será destinada a apenas uma delas.

Exemplo:

```text
OP001 → B3
OP002 → B3
OP003 → CLEARING_X
```

Inicialmente serão processadas operações de RDB destinadas à B3.

O modelo deverá permitir futuramente CDB, novos produtos e novas clearings sem alteração da estrutura principal das chaves.

As operações poderão ter origem em arquivo CSV e, futuramente, Kafka. A estrutura persistida deverá ser independente dessas fontes.

---

# Descrição detalhada

## 1. Modelo conceitual

O modelo será composto por três entidades:

```text
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

```text
CSV
```

Futuramente:

```text
Kafka
```

As fontes não deverão determinar a estrutura interna de persistência.

Cada entrada deverá ser transformada por seu respectivo adaptador para o modelo canônico:

```text
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

Alterações no formato do CSV ou no contrato Kafka não deverão obrigatoriamente alterar o domínio de clearing.

---

# 3. Regra de clearing única

Cada `OPERACAO` deverá possuir exatamente uma clearing de destino.

```text
OPERACAO 1 ───────── 1 REGISTRO_CLEARING
```

A operação possuirá:

```text
clearingDestino
```

Exemplo:

```json
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

Será utilizada inicialmente uma única tabela:

```text
OPERACOES_CLEARING
```

utilizando Single Table Design.

A tabela possuirá chave primária composta.

| Conceito DynamoDB | Nome físico | Tipo |
|---|---|---|
| Partition Key | `identificadorAgregado` | String |
| Sort Key | `chaveEntidade` | String |

---

# 5. Funcionamento das chaves

## identificadorAgregado — Partition Key

O campo:

```text
identificadorAgregado
```

responde:

> De qual operação estes dados fazem parte?

Convenção:

```text
OPERACAO#<idOperacao>
```

Exemplo:

```text
OPERACAO#OP-000001
```

Todos os itens relacionados à operação utilizarão o mesmo `identificadorAgregado`.

---

## chaveEntidade — Sort Key

O campo:

```text
chaveEntidade
```

responde:

> Qual informação dentro da operação este item representa?

Convenções:

```text
OPERACAO

REGISTRO

REGISTRO#EVENTO#<timestamp>#<idEvento>
```

Exemplo:

```text
identificadorAgregado = OPERACAO#OP-000001

├── chaveEntidade = OPERACAO
├── chaveEntidade = REGISTRO
├── chaveEntidade = REGISTRO#EVENTO#...#EVT-001
├── chaveEntidade = REGISTRO#EVENTO#...#EVT-002
└── chaveEntidade = REGISTRO#EVENTO#...#EVT-003
```

Regra mental:

```text
identificadorAgregado
"De qual operação?"

chaveEntidade
"O que é dentro da operação?"
```

---

# 6. Convenção final das chaves

| Entidade | identificadorAgregado | chaveEntidade |
|---|---|---|
| OPERACAO | `OPERACAO#<idOperacao>` | `OPERACAO` |
| REGISTRO_CLEARING | `OPERACAO#<idOperacao>` | `REGISTRO` |
| EVENTO_REGISTRO | `OPERACAO#<idOperacao>` | `REGISTRO#EVENTO#<timestamp>#<idEvento>` |

---

# 7. Estrutura da OPERACAO

Principais atributos:

| Campo | Finalidade |
|---|---|
| `idOperacao` | Identificador interno da operação |
| `tipoOperacao` | APLICAÇÃO, RESGATE etc. |
| `produto` | RDB, CDB etc. |
| `clearingDestino` | Clearing responsável pelo registro |
| `valor` | Valor financeiro |
| `dataOperacao` | Data da operação |
| `tipoOrigem` | ARQUIVO, KAFKA etc. |
| `idOperacaoOrigem` | Identificação recebida da origem |
| `chaveIdempotencia` | Identidade lógica para deduplicação |
| `dadosProduto` | Dados específicos do produto |
| `dataHoraCriacao` | Data/hora de criação |

---

# 8. Estrutura do REGISTRO_CLEARING

Representa o snapshot atual do processo de registro.

Como cada operação possuirá uma única clearing, existirá no máximo um `REGISTRO_CLEARING` por operação.

Principais atributos:

| Campo | Finalidade |
|---|---|
| `idRegistro` | Identificador interno |
| `idOperacao` | Operação relacionada |
| `clearing` | Clearing utilizada |
| `status` | Estado atual |
| `idExterno` | Identificador utilizado na integração |
| `protocoloExterno` | Protocolo retornado pela clearing |
| `tentativa` | Quantidade/número da tentativa |
| `dataHoraEnvio` | Momento do envio |
| `dataHoraRetorno` | Momento do retorno |
| `dataHoraAtualizacao` | Última atualização |

---

# 9. Estrutura do EVENTO_REGISTRO

Representa os fatos ocorridos durante o ciclo de vida do registro.

Principais atributos:

| Campo | Finalidade |
|---|---|
| `idEvento` | Identificador do evento |
| `idRegistro` | Registro relacionado |
| `idOperacao` | Operação relacionada |
| `tipoEvento` | Tipo do acontecimento |
| `statusAnterior` | Status anterior, quando aplicável |
| `statusAtual` | Status resultante |
| `origemEvento` | Origem do acontecimento |
| `dataHoraEvento` | Data/hora do evento |

Os eventos deverão ser append-only.

---

# 10. Ordenação dos eventos

A Sort Key:

```text
REGISTRO#EVENTO#<timestamp>#<idEvento>
```

permite ordenação cronológica.

Exemplo:

```text
REGISTRO#EVENTO#20261001T100030.000Z#EVT-001
REGISTRO#EVENTO#20261001T100100.000Z#EVT-002
REGISTRO#EVENTO#20261001T100500.000Z#EVT-003
REGISTRO#EVENTO#20261001T100501.000Z#EVT-004
```

O `idEvento` também garante unicidade caso dois eventos possuam o mesmo timestamp.

---

# 11. Índice por identificador externo

Deverá existir o GSI:

```text
INDICE_ID_EXTERNO
```

| Conceito | Campo |
|---|---|
| GSI Partition Key | `identificadorExterno` |
| GSI Sort Key | `identificadorRegistro` |

Convenção:

```text
identificadorExterno =
ID_EXTERNO#<idExterno>

identificadorRegistro =
REGISTRO#<idOperacao>
```

Exemplo:

```text
identificadorExterno =
ID_EXTERNO#CLEARING-000001

identificadorRegistro =
REGISTRO#OP-000001
```

Somente itens que possuírem esses atributos participarão do índice.

Inicialmente serão os itens `REGISTRO_CLEARING`.

O índice será, portanto, esparso.

---

# 12. Padrões de consulta

## AP01 — Buscar operação

```text
identificadorAgregado = OPERACAO#<idOperacao>
chaveEntidade = OPERACAO
```

## AP02 — Buscar registro atual

```text
identificadorAgregado = OPERACAO#<idOperacao>
chaveEntidade = REGISTRO
```

## AP03 — Buscar agregado completo

```text
identificadorAgregado = OPERACAO#<idOperacao>
```

Retorna operação, registro atual e eventos.

## AP04 — Buscar histórico

```text
identificadorAgregado = OPERACAO#<idOperacao>

chaveEntidade begins_with
REGISTRO#EVENTO#
```

## AP05 — Buscar último evento

```text
identificadorAgregado = OPERACAO#<idOperacao>

chaveEntidade begins_with
REGISTRO#EVENTO#

ScanIndexForward = false
Limit = 1
```

## AP06 — Buscar pelo identificador externo

Utilizar:

```text
INDICE_ID_EXTERNO
```

com:

```text
identificadorExterno =
ID_EXTERNO#<idExterno>
```

---

# 13. Massa de dados para POC

A massa abaixo deverá permitir validar os principais padrões de acesso definidos nesta história.

Serão criadas duas operações:

```text
OP-000001
Aplicação de RDB
B3
ACEITO

OP-000002
Resgate de RDB
B3
REJEITADO
```

---

# 14. POC — Operação 1

## 14.1 OPERACAO

```json
{
  "identificadorAgregado": "OPERACAO#OP-000001",
  "chaveEntidade": "OPERACAO",
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

---

## 14.2 REGISTRO_CLEARING

```json
{
  "identificadorAgregado": "OPERACAO#OP-000001",
  "chaveEntidade": "REGISTRO",
  "tipoEntidade": "REGISTRO_CLEARING",

  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",

  "clearing": "B3",
  "status": "ACEITO",

  "idExterno": "CLEARING-000001",
  "protocoloExterno": "B3-PROT-987654",

  "tentativa": 1,

  "identificadorExterno": "ID_EXTERNO#CLEARING-000001",
  "identificadorRegistro": "REGISTRO#OP-000001",

  "dataHoraCriacao": "2026-10-01T10:00:30Z",
  "dataHoraEnvio": "2026-10-01T10:01:00Z",
  "dataHoraRetorno": "2026-10-01T10:05:00Z",
  "dataHoraAtualizacao": "2026-10-01T10:05:01Z"
}
```

---

## 14.3 EVENTO — Registro criado

```json
{
  "identificadorAgregado": "OPERACAO#OP-000001",
  "chaveEntidade": "REGISTRO#EVENTO#20261001T100030.000Z#EVT-001",
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

---

## 14.4 EVENTO — Envio realizado

```json
{
  "identificadorAgregado": "OPERACAO#OP-000001",
  "chaveEntidade": "REGISTRO#EVENTO#20261001T100100.000Z#EVT-002",
  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "idEvento": "EVT-002",

  "tipoEvento": "ENVIO_REALIZADO",

  "statusAnterior": "PENDENTE",
  "statusAtual": "ENVIADO",

  "origemEvento": "REGISTRATION_WORKER",

  "tentativa": 1,
  "idExterno": "CLEARING-000001",

  "dataHoraEvento": "2026-10-01T10:01:00Z"
}
```

---

## 14.5 EVENTO — Retorno recebido

```json
{
  "identificadorAgregado": "OPERACAO#OP-000001",
  "chaveEntidade": "REGISTRO#EVENTO#20261001T100500.000Z#EVT-003",
  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "idEvento": "EVT-003",

  "tipoEvento": "RETORNO_RECEBIDO",

  "origemEvento": "PISMO",

  "idEventoExterno": "PISMO-EVT-789",
  "idExterno": "CLEARING-000001",
  "protocoloExterno": "B3-PROT-987654",

  "dataHoraEvento": "2026-10-01T10:05:00Z"
}
```

---

## 14.6 EVENTO — Operação aceita

```json
{
  "identificadorAgregado": "OPERACAO#OP-000001",
  "chaveEntidade": "REGISTRO#EVENTO#20261001T100501.000Z#EVT-004",
  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "idEvento": "EVT-004",

  "tipoEvento": "STATUS_ALTERADO",

  "statusAnterior": "ENVIADO",
  "statusAtual": "ACEITO",

  "origemEvento": "RETORNO_CLEARING",

  "idEventoExterno": "PISMO-EVT-789",
  "idExterno": "CLEARING-000001",
  "protocoloExterno": "B3-PROT-987654",

  "dataHoraEvento": "2026-10-01T10:05:01Z"
}
```

---

# 15. POC — Operação 2

A segunda operação deverá permitir validar que diferentes operações permanecem isoladas através do `identificadorAgregado`.

## 15.1 OPERACAO

```json
{
  "identificadorAgregado": "OPERACAO#OP-000002",
  "chaveEntidade": "OPERACAO",
  "tipoEntidade": "OPERACAO",

  "idOperacao": "OP-000002",
  "tipoOperacao": "RESGATE",
  "produto": "RDB",
  "clearingDestino": "B3",

  "valor": 8500.00,
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

---

## 15.2 REGISTRO_CLEARING

```json
{
  "identificadorAgregado": "OPERACAO#OP-000002",
  "chaveEntidade": "REGISTRO",
  "tipoEntidade": "REGISTRO_CLEARING",

  "idOperacao": "OP-000002",
  "idRegistro": "REG-000002",

  "clearing": "B3",
  "status": "REJEITADO",

  "idExterno": "CLEARING-000002",
  "protocoloExterno": "B3-PROT-123456",

  "codigoRetorno": "B3-001",
  "descricaoRetorno": "Operacao rejeitada pela clearing",

  "tentativa": 1,

  "identificadorExterno": "ID_EXTERNO#CLEARING-000002",
  "identificadorRegistro": "REGISTRO#OP-000002",

  "dataHoraCriacao": "2026-10-01T11:00:30Z",
  "dataHoraEnvio": "2026-10-01T11:01:00Z",
  "dataHoraRetorno": "2026-10-01T11:04:00Z",
  "dataHoraAtualizacao": "2026-10-01T11:04:01Z"
}
```

---

## 15.3 EVENTO — Registro criado

```json
{
  "identificadorAgregado": "OPERACAO#OP-000002",
  "chaveEntidade": "REGISTRO#EVENTO#20261001T110030.000Z#EVT-101",
  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000002",
  "idRegistro": "REG-000002",
  "idEvento": "EVT-101",

  "tipoEvento": "REGISTRO_CRIADO",
  "statusAtual": "PENDENTE",

  "origemEvento": "SISTEMA_CLEARING",
  "dataHoraEvento": "2026-10-01T11:00:30Z"
}
```

---

## 15.4 EVENTO — Enviado

```json
{
  "identificadorAgregado": "OPERACAO#OP-000002",
  "chaveEntidade": "REGISTRO#EVENTO#20261001T110100.000Z#EVT-102",
  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000002",
  "idRegistro": "REG-000002",
  "idEvento": "EVT-102",

  "tipoEvento": "ENVIO_REALIZADO",

  "statusAnterior": "PENDENTE",
  "statusAtual": "ENVIADO",

  "origemEvento": "REGISTRATION_WORKER",

  "tentativa": 1,
  "idExterno": "CLEARING-000002",

  "dataHoraEvento": "2026-10-01T11:01:00Z"
}
```

---

## 15.5 EVENTO — Rejeitado

```json
{
  "identificadorAgregado": "OPERACAO#OP-000002",
  "chaveEntidade": "REGISTRO#EVENTO#20261001T110401.000Z#EVT-103",
  "tipoEntidade": "EVENTO_REGISTRO",

  "idOperacao": "OP-000002",
  "idRegistro": "REG-000002",
  "idEvento": "EVT-103",

  "tipoEvento": "STATUS_ALTERADO",

  "statusAnterior": "ENVIADO",
  "statusAtual": "REJEITADO",

  "origemEvento": "RETORNO_CLEARING",

  "idExterno": "CLEARING-000002",
  "protocoloExterno": "B3-PROT-123456",

  "codigoRetorno": "B3-001",
  "descricaoRetorno": "Operacao rejeitada pela clearing",

  "dataHoraEvento": "2026-10-01T11:04:01Z"
}
```

---

# 16. Validações da POC

Após inserir os itens, deverão ser executados os seguintes testes.

| Teste | Consulta | Resultado esperado |
|---|---|---|
| Buscar operação | `OP-000001 + OPERACAO` | Aplicação RDB |
| Buscar registro | `OP-000001 + REGISTRO` | Status ACEITO |
| Agregado completo | `OPERACAO#OP-000001` | 6 itens |
| Histórico | `begins_with(REGISTRO#EVENTO#)` | 4 eventos |
| Último evento | Histórico descendente + Limit 1 | EVT-004 / ACEITO |
| ID externo | `ID_EXTERNO#CLEARING-000001` | REG-000001 |
| Segunda operação | `OPERACAO#OP-000002` | 5 itens |
| Segundo ID externo | `ID_EXTERNO#CLEARING-000002` | REG-000002 |

Para a primeira operação, uma Query somente por:

```text
identificadorAgregado =
OPERACAO#OP-000001
```

deverá apresentar:

```text
OPERACAO#OP-000001
│
├── OPERACAO
│
├── REGISTRO
│
├── REGISTRO#EVENTO#20261001T100030.000Z#EVT-001
│
├── REGISTRO#EVENTO#20261001T100100.000Z#EVT-002
│
├── REGISTRO#EVENTO#20261001T100500.000Z#EVT-003
│
└── REGISTRO#EVENTO#20261001T100501.000Z#EVT-004
```

Isso deverá comprovar que operação, estado atual e histórico estão agrupados no mesmo agregado.

---

# 17. Flexibilidade por produto

Dados particulares do produto deverão ficar em:

```text
dadosProduto
```

Exemplo RDB:

```json
{
  "produto": "RDB",
  "dadosProduto": {
    "numeroRdb": "RDB-987654",
    "indexador": "CDI",
    "taxa": 102.5
  }
}
```

Exemplo futuro CDB:

```json
{
  "produto": "CDB",
  "dadosProduto": {
    "codigoCdb": "CDB-123456",
    "indexador": "CDI",
    "taxa": 105.0
  }
}
```

A inclusão de novos produtos não deverá exigir alteração de `identificadorAgregado` ou `chaveEntidade`.

---

# 18. Flexibilidade por clearing

Conceitualmente:

```text
OPERACAO
    │
    │ clearingDestino
    ▼
Roteamento
    │
    ├── B3 → Adaptador B3
    ├── X  → Adaptador X
    └── Y  → Adaptador Y
```

A inclusão de uma nova clearing não deverá exigir alteração da estrutura das chaves.

---

# 19. Idempotência

O processamento deverá considerar:

- reprocessamento do mesmo arquivo;
- mensagens duplicadas;
- reexecução após falha parcial;
- processamento concorrente;
- futura entrada através de Kafka.

O campo:

```text
chaveIdempotencia
```

representará a identidade lógica utilizada para deduplicação.

Exemplo:

```text
ARQUIVO#ARQ-987654
```

Um GUID aleatório utilizado como `idOperacao`, isoladamente, não deverá ser considerado garantia de idempotência.

A estratégia definitiva deverá considerar as garantias fornecidas pelos sistemas produtores.

---

# Requisitos

**RF01 — Tabela**

Deverá existir:

```text
OPERACOES_CLEARING
```

**RF02 — Partition Key**

```text
identificadorAgregado
```

String.

**RF03 — Sort Key**

```text
chaveEntidade
```

String.

**RF04 — OPERACAO**

```text
identificadorAgregado = OPERACAO#<idOperacao>
chaveEntidade = OPERACAO
```

**RF05 — REGISTRO_CLEARING**

```text
identificadorAgregado = OPERACAO#<idOperacao>
chaveEntidade = REGISTRO
```

**RF06 — EVENTO_REGISTRO**

```text
identificadorAgregado = OPERACAO#<idOperacao>

chaveEntidade =
REGISTRO#EVENTO#<timestamp>#<idEvento>
```

**RF07 — Clearing única**

Cada operação deverá possuir exatamente uma clearing de destino.

**RF08 — Registro único**

Cada operação deverá possuir no máximo um snapshot `REGISTRO_CLEARING`.

**RF09 — Eventos**

Um registro poderá possuir múltiplos eventos.

**RF10 — Append-only**

Eventos existentes não deverão ser sobrescritos.

**RF11 — GSI**

Deverá existir:

```text
INDICE_ID_EXTERNO
```

utilizando:

```text
Partition Key = identificadorExterno
Sort Key = identificadorRegistro
```

**RF12 — Produtos**

Novos produtos não deverão exigir alteração das chaves principais.

**RF13 — Clearings**

Novas clearings não deverão exigir alteração das chaves principais.

**RF14 — Origem**

O modelo deverá ser independente da origem da operação.

**RF15 — Idempotência**

O modelo deverá suportar uma estratégia de idempotência para evitar processamento duplicado.

**RF16 — Terraform**

A estrutura oficial deverá ser provisionada através de Terraform.

---

# Critérios de aceite

**CA01**

Deverá ser possível persistir uma operação utilizando:

```text
identificadorAgregado = OPERACAO#<idOperacao>
chaveEntidade = OPERACAO
```

**CA02**

Deverá ser possível persistir o registro atual utilizando:

```text
identificadorAgregado = OPERACAO#<idOperacao>
chaveEntidade = REGISTRO
```

**CA03**

Uma operação não deverá possuir mais de um snapshot `REGISTRO_CLEARING`.

**CA04**

Deverá ser possível persistir múltiplos eventos utilizando:

```text
identificadorAgregado =
OPERACAO#<idOperacao>

chaveEntidade =
REGISTRO#EVENTO#<timestamp>#<idEvento>
```

**CA05**

Eventos deverão permanecer append-only.

**CA06**

Uma Query utilizando somente:

```text
identificadorAgregado =
OPERACAO#<idOperacao>
```

deverá recuperar o agregado completo.

**CA07**

A combinação:

```text
identificadorAgregado = OPERACAO#<idOperacao>
chaveEntidade = OPERACAO
```

deverá recuperar exclusivamente a operação.

**CA08**

A combinação:

```text
identificadorAgregado = OPERACAO#<idOperacao>
chaveEntidade = REGISTRO
```

deverá recuperar exclusivamente o snapshot atual do registro.

**CA09**

Uma Query utilizando:

```text
identificadorAgregado =
OPERACAO#<idOperacao>

chaveEntidade begins_with
REGISTRO#EVENTO#
```

deverá recuperar a timeline do registro.

**CA10**

Deverá ser possível recuperar o último evento através da ordenação decrescente da Sort Key e `Limit = 1`.

**CA11**

Deverá ser possível localizar um registro pelo `idExterno` através do `INDICE_ID_EXTERNO`.

**CA12**

Uma operação deverá possuir exatamente uma clearing de destino.

**CA13**

Novos produtos não deverão exigir alteração da estrutura das chaves.

**CA14**

Novas clearings não deverão exigir alteração da estrutura das chaves.

**CA15**

A POC deverá validar pelo menos:

```text
Aplicação RDB aceita pela B3

Resgate RDB rejeitado pela B3

Consulta do agregado

Consulta da timeline

Consulta do último evento

Consulta pelo identificador externo
```

**CA16**

A estrutura oficial deverá estar declarada através de Terraform.

---

# Resumo final das chaves

| Item | identificadorAgregado | chaveEntidade |
|---|---|---|
| Operação | `OPERACAO#<idOperacao>` | `OPERACAO` |
| Registro | `OPERACAO#<idOperacao>` | `REGISTRO` |
| Evento | `OPERACAO#<idOperacao>` | `REGISTRO#EVENTO#<timestamp>#<idEvento>` |

GSI:

| Índice | Partition Key | Sort Key |
|---|---|---|
| `INDICE_ID_EXTERNO` | `identificadorExterno` | `identificadorRegistro` |

Regra mental final:

```text
identificadorAgregado
        │
        └── "De qual operação?"

chaveEntidade
        │
        └── "O que é dentro da operação?"

identificadorExterno
        │
        └── "Qual identificação recebemos do mundo externo?"

identificadorRegistro
        │
        └── "A qual registro/operação ela corresponde?"




Título

Criar estrutura DynamoDB para operações, registros e histórico de clearing

Objetivo

Criar a estrutura de persistência DynamoDB da plataforma de registro em clearing utilizando três tabelas:

1. OPERACOES
2. REGISTROS_CLEARING
3. EVENTOS_REGISTRO

A modelagem deverá suportar:

* alta volumetria;
* múltiplos produtos;
* múltiplas clearings;
* uma única clearing por operação;
* diferentes origens de entrada, inicialmente CSV e futuramente Kafka;
* idempotência;
* correlação com sistemas externos;
* rastreabilidade;
* evolução dos contratos;
* inclusão de novos produtos sem necessidade de alteração frequente da estrutura física;
* inclusão de novas clearings sem acoplamento do modelo interno aos seus contratos.

A infraestrutura oficial deverá ser criada através de Terraform.

Antes da implementação definitiva, a estrutura poderá ser criada manualmente no AWS Console para realização da POC.

⸻

1. Princípio da modelagem

Embora o DynamoDB não imponha schema sobre todos os atributos dos itens, a aplicação deverá possuir um modelo canônico explicitamente definido e versionado.

A ausência de schema físico no DynamoDB não deverá significar ausência de contrato de domínio.

A estrutura seguirá o princípio:

          ITEM DYNAMODB
                │
       ┌────────┴────────┐
       │                 │
       ▼                 ▼
ENVELOPE CANÔNICO    DADOS FLEXÍVEIS
       │                 │
       │                 ├── dadosOperacao
       │                 ├── dadosRegistro
       │                 └── dadosEvento
       │
       ├── identidade
       ├── roteamento
       ├── correlação
       ├── idempotência
       ├── controle
       ├── auditoria
       └── versão

Regra de decisão

Um atributo deverá permanecer no primeiro nível quando for necessário para pelo menos uma das seguintes finalidades:

* PK;
* SK;
* GSI;
* identificação;
* correlação;
* roteamento;
* idempotência;
* controle do workflow;
* versionamento;
* auditoria.

Dados específicos do negócio, produto, clearing ou evento deverão preferencialmente ficar dentro do Map correspondente.

⸻

2. Independência das origens e destinos

O modelo persistido não deverá representar diretamente o contrato do CSV, Kafka, B3, Pismo ou qualquer outra integração.

Entradas deverão ser adaptadas:

CSV
 │
 ▼
Adapter CSV
 │
 │
 ├─────────────┐
               ▼
        MODELO CANÔNICO
               ▲
               │
 ├─────────────┘
 │
Adapter Kafka
 ▲
 │
Kafka

Da mesma forma, os contratos externos das clearings deverão ser produzidos a partir do modelo canônico:

             MODELO CANÔNICO
                    │
                    ▼
             Clearing Router
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
      Adapter B3 Adapter X Adapter Y
          │         │         │
          ▼         ▼         ▼
          B3    Clearing X Clearing Y

Portanto:

A estrutura DynamoDB pertence ao domínio da plataforma de clearing e não às interfaces de entrada ou saída.

⸻

3. Visão geral das tabelas

                    OPERACOES
                       │
                       │ 1 : 1
                       ▼
              REGISTROS_CLEARING
                       │
                       │ 1 : N
                       ▼
                EVENTOS_REGISTRO

Responsabilidades:

OPERACOES
→ dado canônico da operação
REGISTROS_CLEARING
→ estado operacional atual
EVENTOS_REGISTRO
→ histórico append-only

⸻

4. Tabela OPERACOES

Finalidade

Representar uma operação financeira recebida pela plataforma.

Uma operação deverá possuir exatamente uma clearing de destino.

Chaves

PK = idOperacao
SK = não possui

A ausência de SK é intencional.

Existe exatamente um item de operação para cada idOperacao.

Adicionar uma SK constante não acrescentaria nenhum novo padrão de acesso.

⸻

5. Estrutura de OPERACOES

Campo	Tipo	Obrigatório	Finalidade
idOperacao	String	Sim	PK / identificador interno
produto	String	Sim	RDB, CDB etc.
tipoOperacao	String	Sim	APLICACAO / RESGATE
clearingDestino	String	Sim	Clearing responsável pelo registro
chaveIdempotencia	String	Sim	Identidade lógica da operação
versaoModelo	Number	Sim	Versão do contrato canônico
origem	Map	Sim	Informações da origem
dataHoraInclusao	String	Sim	Auditoria
dataHoraAlteracao	String	Sim	Auditoria
dadosOperacao	Map	Sim	Payload específico da operação

Estrutura:

OPERACOES
│
├── idOperacao                PK
├── produto
├── tipoOperacao
├── clearingDestino
├── chaveIdempotencia
├── versaoModelo
├── origem
│   ├── tipo
│   └── identificador
├── dataHoraInclusao
├── dataHoraAlteracao
│
└── dadosOperacao
      └── Map flexível

⸻

6. dadosOperacao

dadosOperacao deverá armazenar informações específicas do negócio da operação.

Exemplos:

* valor;
* data da operação;
* vencimento;
* taxa;
* indexador;
* código do instrumento;
* dados específicos do RDB;
* dados específicos do CDB;
* futuros campos necessários para novos produtos.

Exemplo RDB:

{
  "idOperacao": "OP-000001",
  "produto": "RDB",
  "tipoOperacao": "APLICACAO",
  "clearingDestino": "B3",
  "chaveIdempotencia": "ARQUIVO#20261005#000001",
  "versaoModelo": 1,
  "origem": {
    "tipo": "ARQUIVO",
    "identificador": "ARQ-000001"
  },
  "dataHoraInclusao": "2026-10-05T10:00:00.000Z",
  "dataHoraAlteracao": "2026-10-05T10:00:00.000Z",
  "dadosOperacao": {
    "valor": 15000.50,
    "dataOperacao": "2026-10-05",
    "codigoRdb": "RDB001",
    "dataVencimento": "2028-10-05",
    "taxa": 0.125
  }
}

Exemplo CDB:

{
  "idOperacao": "OP-000003",
  "produto": "CDB",
  "tipoOperacao": "APLICACAO",
  "clearingDestino": "B3",
  "chaveIdempotencia": "ARQUIVO#20261005#000003",
  "versaoModelo": 1,
  "origem": {
    "tipo": "ARQUIVO",
    "identificador": "ARQ-000003"
  },
  "dataHoraInclusao": "2026-10-05T10:02:00.000Z",
  "dataHoraAlteracao": "2026-10-05T10:02:00.000Z",
  "dadosOperacao": {
    "valor": 25000.00,
    "dataOperacao": "2026-10-05",
    "codigoCdb": "CDB001",
    "indexador": "CDI",
    "percentualIndexador": 105,
    "dataVencimento": "2029-10-05"
  }
}

⸻

7. Versionamento do modelo

O campo:

versaoModelo

deverá identificar a versão do contrato canônico.

Exemplo:

produto = RDB
versaoModelo = 1

poderá utilizar:

ValidadorRdbV1

No futuro:

produto = RDB
versaoModelo = 2

poderá utilizar:

ValidadorRdbV2

Isso permitirá evolução controlada do contrato sem reinterpretar silenciosamente dados históricos.

⸻

8. Idempotência

A tabela OPERACOES deverá possuir:

chaveIdempotencia

Exemplo para arquivo:

ARQUIVO#20261005#000001

Exemplo futuro para Kafka:

KAFKA#TOPICO-OPERACOES#12#987654

Poderá ser avaliado o GSI:

INDICE_IDEMPOTENCIA
PK = chaveIdempotencia

Entretanto:

GSI não deverá ser considerado sozinho como mecanismo de garantia de unicidade.

A estratégia definitiva deverá considerar concorrência e escrita condicional e/ou estrutura específica para controle de idempotência.

⸻

9. Tabela REGISTROS_CLEARING

Finalidade

Representar o estado operacional atual do registro da operação na clearing.

Como cada operação possui exatamente uma clearing:

OPERACAO 1 ───────── 1 REGISTRO_CLEARING

Chaves

PK = idOperacao
SK = não possui

A ausência de SK também é intencional.

A própria estrutura física reforça a regra:

uma operação
      ↓
um registro atual

⸻

10. Estrutura de REGISTROS_CLEARING

Campo	Tipo	Obrigatório	Finalidade
idOperacao	String	Sim	PK
idRegistro	String	Sim	Identificador interno
clearing	String	Sim	Clearing utilizada
status	String	Sim	Estado operacional atual
idExterno	String	Não	Correlação externa
tentativa	Number	Sim	Tentativa atual
versaoModelo	Number	Sim	Versão do contrato
dataHoraInclusao	String	Sim	Auditoria
dataHoraAlteracao	String	Sim	Auditoria
dadosRegistro	Map	Não	Dados variáveis do registro

Estrutura:

REGISTROS_CLEARING
│
├── idOperacao             PK
├── idRegistro
├── clearing
├── status
├── idExterno
├── tentativa
├── versaoModelo
├── dataHoraInclusao
├── dataHoraAlteracao
│
└── dadosRegistro
      └── Map flexível

⸻

11. dadosRegistro

Informações específicas da clearing ou do processamento que não sejam necessárias como atributos estruturais deverão ficar em:

dadosRegistro

Exemplo:

{
  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "clearing": "B3",
  "status": "REGISTRADO",
  "idExterno": "B3-20261005-000001",
  "tentativa": 1,
  "versaoModelo": 1,
  "dataHoraInclusao": "2026-10-05T10:00:10.000Z",
  "dataHoraAlteracao": "2026-10-05T10:05:00.000Z",
  "dadosRegistro": {
    "protocolo": "PROTOCOLO-B3-001",
    "codigoRetorno": "00",
    "mensagemRetorno": "Registro realizado com sucesso"
  }
}

O campo idExterno deverá permanecer no primeiro nível porque será utilizado para correlação e consulta através de GSI.

⸻

12. GSI de correlação externa

Criar:

INDICE_ID_EXTERNO

com:

PK = idExterno

Fluxo:

Pismo
  │
  │ retorno
  ▼
SNS
  │
  ▼
SQS RETORNO
  │
  ▼
Return Worker
  │
  │ idExterno
  ▼
INDICE_ID_EXTERNO
  │
  ▼
idOperacao

⸻

13. Tabela EVENTOS_REGISTRO

Finalidade

Armazenar a timeline append-only do processo de registro.

Uma operação poderá possuir N eventos:

OP-000001
   │
   ├── REGISTRO_CRIADO
   ├── ENVIO_INICIADO
   ├── ENVIADO_CLEARING
   └── REGISTRADO

Chaves

PK = idOperacao
SK = chaveEvento

Formato:

<dataHoraEvento>#<idEvento>

Exemplo:

2026-10-05T10:00:10.000Z#EVT-001
2026-10-05T10:01:00.000Z#EVT-002
2026-10-05T10:02:00.000Z#EVT-003
2026-10-05T10:05:00.000Z#EVT-004

⸻

14. Estrutura de EVENTOS_REGISTRO

Campo	Tipo	Obrigatório	Finalidade
idOperacao	String	Sim	PK
chaveEvento	String	Sim	SK
idEvento	String	Sim	Identificador do evento
idRegistro	String	Sim	Correlação com registro
tipoEvento	String	Sim	Tipo
statusAnterior	String	Não	Estado anterior
statusAtual	String	Não	Novo estado
origemEvento	String	Sim	Origem
dataHoraEvento	String	Sim	Data/hora
versaoModelo	Number	Sim	Versão
dadosEvento	Map	Não	Payload variável

Estrutura:

EVENTOS_REGISTRO
│
├── idOperacao             PK
├── chaveEvento            SK
├── idEvento
├── idRegistro
├── tipoEvento
├── statusAnterior
├── statusAtual
├── origemEvento
├── dataHoraEvento
├── versaoModelo
│
└── dadosEvento
      └── Map flexível

⸻

15. Exemplo de evento de sucesso

{
  "idOperacao": "OP-000001",
  "chaveEvento": "2026-10-05T10:05:00.000Z#EVT-004",
  "idEvento": "EVT-004",
  "idRegistro": "REG-000001",
  "tipoEvento": "STATUS_ALTERADO",
  "statusAnterior": "ENVIADO",
  "statusAtual": "REGISTRADO",
  "origemEvento": "RETORNO_PISMO",
  "dataHoraEvento": "2026-10-05T10:05:00.000Z",
  "versaoModelo": 1,
  "dadosEvento": {
    "idExterno": "B3-20261005-000001",
    "protocolo": "PROTOCOLO-B3-001",
    "codigoRetorno": "00",
    "mensagem": "Registro realizado com sucesso"
  }
}

⸻

16. Exemplo de evento de erro

{
  "idOperacao": "OP-000003",
  "chaveEvento": "2026-10-05T10:07:00.000Z#EVT-010",
  "idEvento": "EVT-010",
  "idRegistro": "REG-000003",
  "tipoEvento": "ERRO_REGISTRO",
  "statusAnterior": "EM_PROCESSAMENTO",
  "statusAtual": "ERRO",
  "origemEvento": "ADAPTER_B3",
  "dataHoraEvento": "2026-10-05T10:07:00.000Z",
  "versaoModelo": 1,
  "dadosEvento": {
    "codigoErro": "B3-001",
    "descricao": "Instrumento não encontrado",
    "tentativa": 2,
    "reprocessavel": true
  }
}

⸻

17. Resumo das chaves

Tabela	PK	SK
OPERACOES	idOperacao	—
REGISTROS_CLEARING	idOperacao	—
EVENTOS_REGISTRO	idOperacao	chaveEvento

GSIs:

Tabela	Índice	PK
OPERACOES	INDICE_IDEMPOTENCIA*	chaveIdempotencia
REGISTROS_CLEARING	INDICE_ID_EXTERNO	idExterno
EVENTOS_REGISTRO	Nenhum inicialmente	—

* INDICE_IDEMPOTENCIA deverá ser validado junto à estratégia definitiva de idempotência.

⸻

18. Atomicidade

Alterações que representem uma única transição lógica deverão manter o snapshot e histórico consistentes.

Exemplo:

ENVIADO
   │
   ▼
REGISTRADO

Deverá resultar em:

TransactWriteItems
       │
       ├── UPDATE REGISTROS_CLEARING
       │      status = REGISTRADO
       │
       └── PUT EVENTOS_REGISTRO
              ENVIADO → REGISTRADO

Deve ocorrer:

TUDO
 OU
NADA

⸻

19. DynamoDB Streams

Somente:

OPERACOES

deverá possuir o Stream utilizado para iniciar o fluxo de registro:

OPERACOES
    │
    ▼
DynamoDB Streams
    │
    ▼
EventBridge Pipes
    │
    ▼
SQS REGISTRO

Alterações em:

REGISTROS_CLEARING
EVENTOS_REGISTRO

não deverão iniciar novo envio para clearing.

⸻

20. Configuração manual para POC

OPERACOES

Configuração	Valor
Table name	OPERACOES
Partition key	idOperacao
Partition key type	String
Sort key	Não
Capacity	On-demand
Stream	Habilitado para POC
GSI opcional	INDICE_IDEMPOTENCIA
GSI PK	chaveIdempotencia

REGISTROS_CLEARING

Configuração	Valor
Table name	REGISTROS_CLEARING
Partition key	idOperacao
Partition key type	String
Sort key	Não
Capacity	On-demand
GSI	INDICE_ID_EXTERNO
GSI PK	idExterno

EVENTOS_REGISTRO

Configuração	Valor
Table name	EVENTOS_REGISTRO
Partition key	idOperacao
Partition key type	String
Sort key	chaveEvento
Sort key type	String
Capacity	On-demand
Stream	Não
GSI	Nenhum inicialmente

⸻

21. Massa da POC

A POC deverá possuir ao menos:

OP-000001 | RDB | APLICACAO | B3 | REGISTRADO
OP-000002 | RDB | RESGATE   | B3 | EM_PROCESSAMENTO
OP-000003 | CDB | APLICACAO | B3 | ERRO
OP-000004 | RDB | APLICACAO | B3 | PENDENTE

Os itens deverão seguir o novo modelo de envelope + Maps descrito nesta história.

⸻

22. Consultas da POC

Consultar operação

Tabela: OPERACOES
GetItem
idOperacao = OP-000001

Consultar estado atual

Tabela: REGISTROS_CLEARING
GetItem
idOperacao = OP-000001

Consultar histórico

Tabela: EVENTOS_REGISTRO
Query
idOperacao = OP-000001

Consultar último evento

idOperacao = OP-000001
ScanIndexForward = false
Limit = 1

Localizar pelo retorno externo

Tabela: REGISTROS_CLEARING
Índice: INDICE_ID_EXTERNO
idExterno = B3-20261005-000001

⸻

23. Consultas analíticas

Não deverão ser criados GSIs indiscriminadamente para consultas como:

* total de RDB;
* total de CDB;
* operações B3;
* operações registradas;
* operações com erro;
* operações pendentes;
* dashboard por período.

Esses padrões deverão ser avaliados futuramente através de read model, exportação/ETL, S3/Athena ou solução equivalente.

O DynamoDB atual deverá ser otimizado principalmente para o processamento transacional.

⸻

24. Volumetria

Considerando conceitualmente 20 milhões de operações:

OPERACOES
≈ 20 milhões de itens
REGISTROS_CLEARING
≈ 20 milhões de itens + atualizações
EVENTOS_REGISTRO
≈ 20 milhões × quantidade média de eventos

Com cinco eventos por operação:

20 milhões × 5
=
100 milhões de eventos

O exemplo é apenas ilustrativo para demonstrar a diferença de crescimento entre as tabelas.

As PKs não deverão utilizar valores de baixa cardinalidade como:

B3
RDB
REGISTRADO

idOperacao deverá fornecer distribuição adequada das partições.

⸻

25. Requisitos

RF01. Criar OPERACOES.

RF02. OPERACOES deverá utilizar idOperacao como PK e não possuir SK inicialmente.

RF03. Criar REGISTROS_CLEARING.

RF04. REGISTROS_CLEARING deverá utilizar idOperacao como PK e não possuir SK inicialmente.

RF05. Criar EVENTOS_REGISTRO.

RF06. EVENTOS_REGISTRO deverá utilizar idOperacao como PK e chaveEvento como SK.

RF07. Criar INDICE_ID_EXTERNO.

RF08. Avaliar INDICE_IDEMPOTENCIA.

RF09. Cada tabela deverá possuir envelope canônico estável.

RF10. Dados específicos deverão ser armazenados nos Maps dadosOperacao, dadosRegistro e dadosEvento.

RF11. O modelo não deverá ser acoplado ao contrato CSV.

RF12. O modelo não deverá ser acoplado ao contrato Kafka.

RF13. O modelo não deverá ser acoplado ao contrato B3.

RF14. Cada item deverá possuir versaoModelo.

RF15. Os contratos versionados deverão ser validados pela aplicação.

RF16. EVENTOS_REGISTRO deverá ser append-only.

RF17. Cada operação deverá possuir exatamente uma clearing.

RF18. Mudanças de estado e histórico deverão permanecer consistentes.

RF19. Somente OPERACOES deverá iniciar o fluxo através do Stream.

RF20. A infraestrutura oficial deverá ser provisionada através de Terraform.

⸻

26. Critérios de aceite

CA01. As três tabelas deverão ser criadas com as PK/SK especificadas.

CA02. Uma operação deverá ser recuperável diretamente por idOperacao.

CA03. Seu estado atual deverá ser recuperável diretamente por idOperacao.

CA04. Seu histórico deverá ser consultável por idOperacao.

CA05. Eventos deverão ser retornados cronologicamente.

CA06. O último evento deverá ser recuperável sem Scan completo.

CA07. O retorno Pismo deverá localizar o registro através de idExterno.

CA08. RDB e CDB deverão coexistir sem alteração estrutural da tabela.

CA09. Um novo produto deverá poder introduzir novos atributos dentro de dadosOperacao.

CA10. Uma nova clearing deverá poder introduzir dados específicos sem alterar o envelope canônico desnecessariamente.

CA11. Eventos diferentes deverão suportar estruturas distintas dentro de dadosEvento.

CA12. Os adapters de entrada deverão produzir o mesmo modelo canônico independentemente da origem.

CA13. O modelo deverá possuir versionamento explícito.

CA14. Histórico deverá permanecer append-only.

CA15. Snapshot e histórico deverão permanecer consistentes.

CA16. A POC deverá validar TransactWriteItems.

CA17. A POC deverá validar INDICE_ID_EXTERNO.

CA18. A estratégia de idempotência deverá ser validada antes da implementação definitiva.

CA19. Somente inserções elegíveis em OPERACOES deverão iniciar o fluxo de registro.

CA20. A configuração definitiva deverá estar representada em Terraform.

⸻

27. Resultado arquitetural esperado

A estrutura final deverá seguir:

                   ENTRADAS
              CSV          Kafka
               │             │
               ▼             ▼
          CSV Adapter   Kafka Adapter
               │             │
               └──────┬──────┘
                      ▼
               MODELO CANÔNICO
                      │
                      ▼
                  OPERACOES
                      │
                      ▼
             REGISTROS_CLEARING
                      │
                      ▼
              EVENTOS_REGISTRO
OPERACOES
└── dadosOperacao { }
REGISTROS_CLEARING
└── dadosRegistro { }
EVENTOS_REGISTRO
└── dadosEvento { }

Na saída:

MODELO CANÔNICO
       │
       ▼
Clearing Router
       │
   ┌───┴───────────────┐
   ▼                   ▼
Adapter B3        Adapter Clearing X
   │                   │
   ▼                   ▼
  B3               Clearing X

O DynamoDB permanecerá flexível fisicamente, enquanto o domínio permanecerá controlado através de envelopes canônicos, contratos versionados, adapters e validação na aplicação.


```
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>H02 - Implementar ingestão de operações via AWS Batch</title>
<style>
    body {
        font-family: Arial, Helvetica, sans-serif;
        line-height: 1.6;
        color: #24292f;
        max-width: 1200px;
        margin: 0 auto;
        padding: 40px;
    }
    h1 {
        color: #1f4e79;
        border-bottom: 2px solid #d0d7de;
        padding-bottom: 8px;
        margin-top: 40px;
    }
    h2 {
        color: #2f5f8f;
        margin-top: 30px;
    }
    h3 {
        color: #444;
        margin-top: 24px;
    }
    pre {
        background-color: #f6f8fa;
        border: 1px solid #d0d7de;
        border-radius: 6px;
        padding: 16px;
        overflow-x: auto;
    }
    code {
        font-family: Consolas, Monaco, monospace;
    }
    table {
        border-collapse: collapse;
        width: 100%;
        margin: 20px 0;
    }
    th, td {
        border: 1px solid #d0d7de;
        padding: 10px;
        text-align: left;
    }
    th {
        background-color: #f6f8fa;
    }
    blockquote {
        border-left: 4px solid #1f4e79;
        padding: 10px 20px;
        margin: 20px 0;
        background-color: #f6f8fa;
    }
    .important {
        background-color: #fff8c5;
        border-left: 4px solid #d4a72c;
        padding: 12px;
        margin: 20px 0;
    }
    .success {
        background-color: #dafbe1;
        border-left: 4px solid #2da44e;
        padding: 12px;
        margin: 20px 0;
    }
</style>
</head>
<body>
<h1>Título</h1>
<p>
    Implementar ingestão de operações via AWS Batch com adaptação
    para o modelo canônico.
</p>
<h1>Objetivo</h1>
<p>
    Implementar o fluxo responsável por receber o evento de disponibilização
    de um arquivo de operações no S3, iniciar um processamento AWS Batch,
    realizar a leitura do arquivo de forma eficiente, converter cada registro
    externo para o modelo canônico da plataforma de clearing e persistir
    as operações no DynamoDB.
</p>
<p>
    O fluxo deverá suportar inicialmente arquivos CSV contendo operações
    de RDB, considerando volumes estimados entre
    <strong>20 e 30 milhões de registros por arquivo</strong>.
</p>
<p>
    A implementação deverá ser preparada para evolução futura de produtos
    e origens sem acoplar o domínio da plataforma ao formato do arquivo.
</p>
<pre>
Arquivo CSV
    │
    ▼
S3
    │
    ▼
EventBridge
    │
    ▼
AWS Batch
    │
    ▼
Leitor CSV
    │
    ▼
Adapter da origem
    │
    ▼
Modelo canônico
    │
    ▼
Validação
    │
    ▼
Persistência
    │
    ├── OPERACOES
    │
    └── REGISTROS_CLEARING
</pre>
<h1>1. Responsabilidade do AWS Batch</h1>
<p>
    O AWS Batch será responsável exclusivamente pela etapa de
    <strong>ingestão</strong>.
</p>
<p>Suas responsabilidades serão:</p>
<ol>
    <li>receber a identificação do arquivo;</li>
    <li>abrir o arquivo no S3;</li>
    <li>realizar leitura streaming;</li>
    <li>interpretar cada linha;</li>
    <li>converter o registro externo para o modelo canônico;</li>
    <li>validar o modelo produzido;</li>
    <li>gerar identificadores técnicos;</li>
    <li>aplicar estratégia de idempotência;</li>
    <li>persistir a operação;</li>
    <li>criar seu registro inicial de clearing;</li>
    <li>registrar métricas e erros de processamento.</li>
</ol>
<div class="important">
    <strong>Importante:</strong>
    O Batch não deverá registrar diretamente a operação na B3.
</div>
<pre>
Batch
  │
  ▼
OPERACOES
  │
  ▼
DynamoDB Stream
  │
  ▼
EventBridge Pipes
  │
  ▼
SQS
  │
  ▼
Registration Worker
  │
  ▼
B3
</pre>
<p>
    Essa separação impede que problemas da B3 reduzam a capacidade
    de ingestão do arquivo.
</p>
<h1>2. Princípio arquitetural</h1>
<p>
    O arquivo CSV é um <strong>contrato externo</strong>.
</p>
<p>
    O modelo DynamoDB é um <strong>contrato interno da plataforma</strong>.
</p>
<p>
    Portanto, não deverão ser equivalentes.
</p>
<pre>
CSV
CD_PROD
TP_MOV
VL_MOV
DT_MOV
CD_CLEARING
COD_RDB
DT_VCTO
        │
        ▼
     CsvAdapter
        │
        ▼
Modelo canônico
produto
tipoOperacao
clearingDestino
dadosOperacao
...
</pre>
<blockquote>
    A aplicação não deverá possuir código de domínio dependente
    de nomes de colunas do CSV.
</blockquote>
<h1>3. Arquitetura interna do Batch</h1>
<pre>
Batch Job
│
├── Orquestração
│   └── ProcessarArquivoUseCase
│
├── Entrada
│   ├── S3FileReader
│   ├── CsvParser
│   └── CsvAdapter
│
├── Domínio
│   ├── Operacao
│   ├── RegistroClearing
│   ├── Validadores
│   └── Regras
│
├── Persistência
│   ├── OperacaoRepository
│   └── RegistroClearingRepository
│
└── Infraestrutura
    ├── S3
    ├── DynamoDB
    ├── Logs
    └── Métricas
</pre>
<p>O fluxo interno deverá ser aproximadamente:</p>
<pre>
Stream S3
    │
    ▼
Parser
    │
    ▼
DTO externo
    │
    ▼
Adapter
    │
    ▼
Modelo canônico
    │
    ▼
Validator
    │
    ▼
Persistence
</pre>
<h1>4. Separação Parser × Adapter</h1>
<p>
    Essas duas responsabilidades não deverão ser misturadas.
</p>
<h2>Parser</h2>
<p>
    Responsável apenas por interpretar fisicamente o arquivo.
</p>
<p>Entrada:</p>
<pre>
RDB;A;15000.50;2026-10-05;B3;RDB001
</pre>
<p>Saída conceitual:</p>
<pre><code>ArquivoOperacaoDto</code></pre>
<p>Exemplo:</p>
<pre><code>public sealed class ArquivoOperacaoDto
{
    public string CodigoProduto { get; init; }
    public string TipoMovimento { get; init; }
    public decimal ValorMovimento { get; init; }
    public DateOnly DataMovimento { get; init; }
    public string CodigoClearing { get; init; }
    public string CodigoRdb { get; init; }
}</code></pre>
<p>
    Esse DTO representa <strong>o contrato do arquivo</strong>,
    não o domínio.
</p>
<h1>5. Adapter</h1>
<p>O Adapter será responsável por traduzir:</p>
<pre>
Contrato externo
       ↓
Modelo canônico
</pre>
<p>Interface sugerida:</p>
<pre><code>public interface IOperacaoAdapter&lt;in TEntrada&gt;
{
    Operacao Adaptar(
        TEntrada entrada,
        ContextoIngestao contexto);
}</code></pre>
<p>Implementação inicial:</p>
<pre><code>public sealed class CsvRdbOperacaoAdapter
    : IOperacaoAdapter&lt;ArquivoOperacaoDto&gt;
{
    public Operacao Adaptar(
        ArquivoOperacaoDto entrada,
        ContextoIngestao contexto)
    {
        // tradução para o domínio
    }
}</code></pre>
<p>O Adapter deverá conhecer:</p>
<pre>
CSV → modelo canônico
</pre>
<p>Mas o domínio não deverá conhecer:</p>
<pre>
modelo canônico → CSV
</pre>
<h1>6. Modelo canônico produzido</h1>
<p>
    O resultado do Adapter deverá seguir o contrato definido
    na história do DynamoDB.
</p>
<pre><code>{
  "idOperacao": "550e8400-e29b-41d4-a716-446655440001",
  "produto": "RDB",
  "tipoOperacao": "APLICACAO",
  "clearingDestino": "B3",
  "chaveIdempotencia": "ARQUIVO#20261005#987654",
  "versaoModelo": 1,
  "origem": {
    "tipo": "ARQUIVO",
    "identificador": "ARQ-987654"
  },
  "dataHoraInclusao": "2026-10-05T10:00:00.000Z",
  "dataHoraAlteracao": "2026-10-05T10:00:00.000Z",
  "dadosOperacao": {
    "valor": 15000.50,
    "dataOperacao": "2026-10-05",
    "codigoRdb": "RDB001",
    "dataVencimento": "2028-10-05",
    "taxa": 0.125
  }
}</code></pre>
<h1>7. Evolução para novos produtos</h1>
<p>
    A inclusão de CDB não deverá exigir alteração no fluxo principal do Batch.
</p>
<pre>
                  Registro CSV
                       │
                       ▼
                    Parser
                       │
                       ▼
                 Identifica produto
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
      CsvRdbAdapter        CsvCdbAdapter
             │                   │
             └─────────┬─────────┘
                       ▼
                Modelo canônico
</pre>
<p>
    A seleção poderá ser realizada através de uma Factory/Resolver.
</p>
<pre><code>public interface IOperacaoAdapterResolver
{
    IOperacaoAdapter&lt;ArquivoOperacaoDto&gt; Obter(
        string produto);
}</code></pre>
<pre><code>var adapter =
    adapterResolver.Obter(dto.CodigoProduto);
var operacao =
    adapter.Adaptar(dto, contexto);</code></pre>
<p>Evitar código desse tipo espalhado pela aplicação:</p>
<pre><code>if (produto == "RDB")
{
    ...
}
else if (produto == "CDB")
{
    ...
}
else if (produto == "LCI")
{
    ...
}</code></pre>
<h1>8. Evolução para Kafka</h1>
<pre>
CSV
 │
 ▼
CsvParser
 │
 ▼
CsvAdapter
 │
 └──────────────┐
                │
                ▼
         MODELO CANÔNICO
                ▲
                │
KafkaAdapter ───┘
 ▲
 │
Kafka
</pre>
<p>
    O domínio e os repositories deverão receber o mesmo objeto
    <code>Operacao</code>, independentemente da origem.
</p>
<pre>
CsvAdapter
KafkaAdapter
ApiAdapter
</pre>
<p>Esses adapters poderão existir sem alterar:</p>
<pre>
OperacaoRepository
RegistroClearingRepository
Registration Worker
Clearing Router
Adapter B3
</pre>
<h1>9. Validação</h1>
<p>
    A validação deverá ocorrer <strong>depois do Adapter</strong>.
</p>
<pre>
DTO externo
    │
    ▼
Adapter
    │
    ▼
Operacao canônica
    │
    ▼
Validator
</pre>
<p>
    Isso é importante porque a validação deve ocorrer sobre o contrato
    da plataforma, e não apenas sobre o contrato externo.
</p>
<pre><code>public interface IOperacaoValidator
{
    ResultadoValidacao Validar(Operacao operacao);
}</code></pre>
<p>Poderão existir:</p>
<pre>
RDB + versão 1
      ↓
RdbOperacaoValidatorV1
CDB + versão 1
      ↓
CdbOperacaoValidatorV1
</pre>
<h1>10. Tipos de validação</h1>
<h2>Validação estrutural</h2>
<pre>
campo obrigatório ausente
data inválida
decimal inválido
linha corrompida
quantidade inesperada de colunas
</pre>
<h2>Validação canônica</h2>
<pre>
produto inexistente
tipoOperacao inválido
clearing não suportada
versaoModelo não suportada
</pre>
<h2>Validação de produto</h2>
<p>Exemplo RDB:</p>
<pre>
valor > 0
codigoRdb obrigatório
dataOperacao válida
vencimento válido
</pre>
<h1>11. Tratamento de linhas inválidas</h1>
<p>
    Uma linha inválida <strong>não deverá interromper automaticamente
    o arquivo inteiro</strong>.
</p>
<pre>
20.000.000 linhas
19.999.970 válidas
30 inválidas
</pre>
<p>
    As 30 inválidas deverão ser registradas para análise.
</p>
<p>O processamento deverá produzir métricas como:</p>
<pre>
linhasRecebidas
linhasProcessadas
linhasPersistidas
linhasInvalidas
linhasDuplicadas
linhasErroTecnico
</pre>
<p>
    A política definitiva para rejeição do arquivo inteiro deverá
    ser parametrizável ou definida com negócio.
</p>
<pre>
erro estrutural do arquivo
        ↓
interrompe job
erro em uma operação
        ↓
registra rejeição
        ↓
continua processamento
</pre>
<h1>12. Leitura do arquivo</h1>
<p>
    Considerando arquivos com até aproximadamente 30 milhões de linhas,
    o arquivo <strong>não deverá ser carregado integralmente em memória</strong>.
</p>
<p>Não fazer:</p>
<pre><code>var conteudo =
    await File.ReadAllTextAsync(...);</code></pre>
<p>Nem:</p>
<pre><code>var linhas =
    await File.ReadAllLinesAsync(...);</code></pre>
<p>O processamento deverá ser streaming:</p>
<pre>
S3 Object
    │
    ▼
Stream
    │
    ▼
linha
linha
linha
linha
...
</pre>
<p>Exemplo conceitual:</p>
<pre><code>await using var stream =
    await s3Reader.OpenReadAsync(bucket, key);
using var reader = new StreamReader(stream);
while (await reader.ReadLineAsync() is { } linha)
{
    await processador.ProcessarAsync(linha);
}</code></pre>
<h1>13. Uso de memória</h1>
<pre>
arquivo de 20 GB
não significa
20 GB em memória
</pre>
<p>
    A memória deverá depender principalmente do tamanho
    do buffer/lote.
</p>
<pre>
Arquivo
  │
  ▼
Streaming
  │
  ▼
Buffer pequeno
  │
  ▼
Persistência
</pre>
<h1>14. Processamento em lotes</h1>
<p>
    Embora a leitura seja linha a linha, a persistência não precisa
    necessariamente ocorrer individualmente.
</p>
<pre>
Streaming
   │
   ├── operação 1
   ├── operação 2
   ├── ...
   └── operação N
          │
          ▼
       Buffer
          │
          ▼
     Persistência
</pre>
<p>
    O tamanho deverá ser configurável e medido através
    de testes de carga.
</p>
<h1>15. Persistência</h1>
<p>Para cada operação válida deverão ser criados:</p>
<pre>
OPERACOES
+
REGISTROS_CLEARING
</pre>
<p>Estado inicial sugerido:</p>
<pre>
PENDENTE
</pre>
<pre>
OP-001
OPERACOES
idOperacao = OP-001
REGISTROS_CLEARING
idOperacao = OP-001
status = PENDENTE
</pre>
<h1>16. Consistência entre as duas tabelas</h1>
<p>Não deverá ocorrer:</p>
<pre>
OPERACOES
OP-001 existe
REGISTROS_CLEARING
OP-001 não existe
</pre>
<p>
    Para a criação inicial, avaliar/utilizar
    <code>TransactWriteItems</code>.
</p>
<pre>
TransactWriteItems
       │
       ├── PUT OPERACOES
       │
       └── PUT REGISTROS_CLEARING
</pre>
<p>Resultado:</p>
<pre>
ambos persistidos
OU
nenhum persistido
</pre>
<h1>17. Idempotência</h1>
<p>
    O processamento deverá assumir que um arquivo ou uma operação
    pode ser recebido novamente.
</p>
<pre>
EventBridge entrega novamente evento
Batch reinicia
arquivo é reenviado
linha é processada novamente
</pre>
<p>
    Isso não deverá resultar em uma nova operação financeira.
</p>
<p>A operação deverá possuir:</p>
<pre>
chaveIdempotencia
</pre>
<p>Exemplo:</p>
<pre>
ARQUIVO#20261005#987654
</pre>
<p>
    A estratégia definitiva deverá ser implementada de forma
    segura para concorrência.
</p>
<div class="important">
    Não depender exclusivamente de uma consulta ao GSI seguida
    de PutItem, pois dois processos concorrentes podem não encontrar
    o registro e tentar inserir simultaneamente.
</div>
<p>
    Deverá ser considerada escrita condicional e/ou mecanismo
    dedicado de controle de idempotência.
</p>
<h1>18. Identificador da operação</h1>
<p>
    <code>idOperacao</code> deverá ser um identificador interno
    da plataforma.
</p>
<p>Sugestão:</p>
<pre>
UUID/GUID
</pre>
<p>Exemplo:</p>
<pre>
550e8400-e29b-41d4-a716-446655440001
</pre>
<p>
    O identificador recebido da origem deverá ser mantido separadamente.
</p>
<pre><code>{
    "origem": {
        "tipo": "ARQUIVO",
        "identificador": "ARQ-987654"
    }
}</code></pre>
<h1>19. Contexto do arquivo</h1>
<p>
    O processamento deverá manter informações suficientes para rastrear
    de qual arquivo uma operação foi originada.
</p>
<pre>
bucket
objectKey
ETag/versionId, quando aplicável
batchJobId
dataHoraInicio
</pre>
<p>
    Essas informações poderão ser utilizadas em logs, métricas
    e na geração da chave de idempotência.
</p>
<h1>20. Inicialização do Batch</h1>
<p>
    O EventBridge deverá iniciar o AWS Batch ao detectar o evento
    esperado relacionado ao arquivo.
</p>
<pre>
S3
 │
 │ Object Created
 ▼
EventBridge
 │
 ▼
AWS Batch SubmitJob
</pre>
<p>O job deverá receber parâmetros mínimos, por exemplo:</p>
<pre><code>{
    "bucket": "bucket-operacoes",
    "objectKey": "entrada/2026/10/05/operacoes.csv"
}</code></pre>
<p>
    Evitar transportar conteúdo do arquivo no evento.
</p>
<h1>21. Infraestrutura AWS Batch</h1>
<p>
    A infraestrutura deverá ser criada via Terraform e contemplar,
    conforme os padrões da empresa:
</p>
<pre>
AWS Batch
├── Compute Environment
├── Job Queue
├── Job Definition
├── Container Image
├── IAM Role
├── CloudWatch Logs
└── configurações de retry
</pre>
<p>
    A imagem da aplicação .NET deverá ser publicada no repositório
    de imagens adotado pela empresa, tipicamente ECR.
</p>
<pre>
Código .NET
    │
    ▼
Docker Build
    │
    ▼
Container Image
    │
    ▼
ECR
    │
    ▼
Batch Job Definition
</pre>
<h1>22. Permissões mínimas</h1>
<p>
    O Job deverá possuir somente as permissões necessárias.
</p>
<pre>
S3
→ GetObject
DynamoDB OPERACOES
→ PutItem / TransactWriteItems
→ ações adicionais estritamente necessárias
DynamoDB REGISTROS_CLEARING
→ PutItem / TransactWriteItems
CloudWatch
→ logs/métricas conforme padrão corporativo
</pre>
<p>Não utilizar permissões genéricas como:</p>
<pre>
s3:*
dynamodb:*
Resource: *
</pre>
<p>
    sem necessidade justificada.
</p>
<h1>23. Retry do Batch</h1>
<p>
    Retry do AWS Batch deverá ser utilizado principalmente
    para falhas técnicas do job.
</p>
<pre>
falha transitória AWS
problema de infraestrutura
container encerrado inesperadamente
</pre>
<blockquote>
    Retry do job não substitui idempotência.
</blockquote>
<p>
    Se o job processou 10 milhões de linhas e falhou,
    sua nova execução não poderá gerar duplicidade nas operações
    já persistidas.
</p>
<h1>24. Estratégia de retomada</h1>
<h2>Alternativa A — reprocessar desde o início</h2>
<pre>
falhou na linha 15M
        ↓
novo job
        ↓
começa na linha 1
        ↓
idempotência ignora já processadas
</pre>
<h3>Prós</h3>
<ul>
    <li>implementação mais simples;</li>
    <li>menor quantidade de estado operacional;</li>
    <li>menor complexidade de recuperação.</li>
</ul>
<h3>Contras</h3>
<ul>
    <li>releitura de grande volume;</li>
    <li>aumento de custo;</li>
    <li>reexecução de trabalho já realizado.</li>
</ul>
<h2>Alternativa B — checkpoint</h2>
<pre>
arquivo X
último bloco processado = Y
</pre>
<p>Novo job:</p>
<pre>
retoma de Y
</pre>
<h3>Prós</h3>
<ul>
    <li>menos reprocessamento;</li>
    <li>recuperação potencialmente mais rápida.</li>
</ul>
<h3>Contras</h3>
<ul>
    <li>maior complexidade;</li>
    <li>gerenciamento de checkpoint;</li>
    <li>cuidado adicional com consistência;</li>
    <li>risco de introduzir erros na fronteira de retomada.</li>
</ul>
<h2>Recomendação para POC</h2>
<pre>
Alternativa A
+
idempotência robusta
</pre>
<p>
    Adicionar checkpoint somente se os testes demonstrarem necessidade.
</p>
<h1>25. Paralelismo</h1>
<p>
    Não assumir inicialmente que um arquivo processado por uma única
    thread será suficiente para a volumetria final.
</p>
<p>
    Também não implementar paralelismo agressivo sem medição.
</p>
<p>A POC deverá medir:</p>
<pre>
linhas/segundo
operações/segundo
tempo total
CPU
memória
consumo DynamoDB
throttling
erros
</pre>
<h1>26. Backpressure</h1>
<p>
    O Batch não deverá produzir operações em velocidade superior
    à capacidade segura de persistência.
</p>
<pre>
Leitura rápida
    │
    ▼
Buffer limitado
    │
    ▼
Workers limitados
    │
    ▼
DynamoDB
</pre>
<p>Evitar:</p>
<pre>
30 milhões de Tasks simultâneas
</pre>
<p>
    A concorrência deverá ser configurável.
</p>
<h1>27. Observabilidade</h1>
<p>
    Cada execução deverá gerar logs estruturados.
</p>
<pre><code>{
  "evento": "PROCESSAMENTO_ARQUIVO",
  "batchJobId": "...",
  "arquivo": "operacoes.csv",
  "linhasProcessadas": 1250000,
  "linhasPersistidas": 1249980,
  "linhasInvalidas": 15,
  "linhasDuplicadas": 5
}</code></pre>
<div class="important">
    Não gerar um log INFO por linha em produção.
    Com 30 milhões de linhas isso poderia produzir
    30 milhões de logs desnecessários.
</div>
<h1>28. Métricas mínimas</h1>
<pre>
arquivosRecebidos
linhasRecebidas
linhasProcessadas
linhasPersistidas
linhasInvalidas
linhasDuplicadas
linhasErroTecnico
tempoProcessamento
linhasPorSegundo
operacoesRdb
operacoesCdb
operacoesPorClearing
</pre>
<h1>29. Estrutura sugerida da solução .NET</h1>
<pre>
src/
│
├── Clearing.Ingestion.Batch
│   ├── Program.cs
│   └── Workers/
│
├── Clearing.Application
│   ├── UseCases/
│   │   └── ProcessarArquivo/
│   │
│   ├── Adapters/
│   │   ├── IOperacaoAdapter.cs
│   │   ├── CsvRdbOperacaoAdapter.cs
│   │   └── CsvCdbOperacaoAdapter.cs
│   │
│   └── Validators/
│
├── Clearing.Domain
│   ├── Operacao.cs
│   ├── RegistroClearing.cs
│   └── ValueObjects/
│
└── Clearing.Infrastructure
    ├── S3/
    ├── DynamoDb/
    └── Observability/
</pre>
<p>
    O nome real deverá seguir o padrão corporativo.
</p>
<h1>30. Testes unitários</h1>
<p>
    Os Adapters deverão possuir testes unitários independentes da AWS.
</p>
<pre>
Given
linha RDB válida
When
CsvRdbAdapter.Adaptar()
Then
produto = RDB
tipoOperacao = APLICACAO
clearingDestino = B3
dadosOperacao.valor = 15000.50
</pre>
<p>Casos mínimos:</p>
<ul>
    <li>RDB aplicação;</li>
    <li>RDB resgate;</li>
    <li>CDB aplicação;</li>
    <li>CDB resgate;</li>
    <li>campo obrigatório ausente;</li>
    <li>tipo de operação inválido;</li>
    <li>produto inválido;</li>
    <li>clearing inválida;</li>
    <li>data inválida;</li>
    <li>valor inválido.</li>
</ul>
<h1>31. Testes de contrato dos Adapters</h1>
<pre>
entrada externa conhecida
          ↓
        Adapter
          ↓
modelo canônico esperado
</pre>
<p>
    Se o time responsável pelo arquivo alterar uma coluna ou semântica,
    os testes deverão evidenciar a quebra.
</p>
<h1>32. Testes dos validadores</h1>
<pre>
RDB válido
→ válido
RDB sem codigoRdb
→ inválido
RDB valor = -100
→ inválido
</pre>
<h1>33. Testes de persistência</h1>
<p>Deverão validar:</p>
<pre>
OperacaoRepository
RegistroClearingRepository
</pre>
<p>Incluindo:</p>
<pre>
Put
Get
TransactWriteItems
ConditionalWrite
idempotência
</pre>
<h1>34. Teste de idempotência</h1>
<p>Cenário obrigatório:</p>
<pre>
mesma operação
      │
      ├── processamento 1
      └── processamento 2
</pre>
<p>Resultado:</p>
<pre>
OPERACOES
→ 1 operação lógica
REGISTROS_CLEARING
→ 1 registro
envio futuro
→ não poderá ocorrer duas vezes devido à duplicação da ingestão
</pre>
<p>
    Também deverá ser testado cenário concorrente.
</p>
<h1>35. Teste de atomicidade</h1>
<p>Simular falha durante:</p>
<pre>
PUT OPERACOES
+
PUT REGISTROS_CLEARING
</pre>
<p>
    Validar que <code>TransactWriteItems</code> impede estado parcial.
</p>
<p>Resultado permitido:</p>
<pre>
ambos existem
</pre>
<p>ou:</p>
<pre>
nenhum existe
</pre>
<p>Nunca:</p>
<pre>
OPERACOES existe
REGISTROS_CLEARING não existe
</pre>
<h1>36. Testes de integração</h1>
<pre>
arquivo de teste
      ↓
parser
      ↓
adapter
      ↓
validator
      ↓
DynamoDB
</pre>
<p>
    Validar conteúdo final das duas tabelas.
</p>
<h1>37. Testes de carga</h1>
<p>
    Essa etapa é obrigatória devido à volumetria.
</p>
<p>Criar arquivos progressivos:</p>
<pre>
10 mil
100 mil
1 milhão
5 milhões
20 milhões
</pre>
<p>Se possível:</p>
<pre>
30 milhões
</pre>
<table>
    <thead>
        <tr>
            <th>Métrica</th>
            <th>Objetivo</th>
        </tr>
    </thead>
    <tbody>
        <tr><td>tempo total</td><td>determinar janela necessária</td></tr>
        <tr><td>linhas/s</td><td>throughput</td></tr>
        <tr><td>CPU</td><td>dimensionamento</td></tr>
        <tr><td>memória</td><td>estabilidade</td></tr>
        <tr><td>writes/s</td><td>capacidade DynamoDB</td></tr>
        <tr><td>throttling</td><td>identificar gargalos</td></tr>
        <tr><td>erros</td><td>confiabilidade</td></tr>
        <tr><td>custo</td><td>dimensionamento</td></tr>
    </tbody>
</table>
<h1>38. Teste de memória</h1>
<pre>
100 MB arquivo
   ↓
memória X
10 GB arquivo
   ↓
memória aproximadamente X
</pre>
<p>
    A variação deverá estar relacionada ao buffer, runtime e concorrência,
    e não ao tamanho integral do arquivo.
</p>
<h1>39. Teste de falha e reprocessamento</h1>
<pre>
arquivo
   ↓
processa parcialmente
   ↓
job interrompido
   ↓
Batch executa novamente
</pre>
<p>Validar:</p>
<pre>
operações anteriores
→ reconhecidas
operações restantes
→ persistidas
duplicidade financeira
→ zero
</pre>
<h1>40. Teste de linha inválida</h1>
<pre>
linha válida
linha válida
linha inválida
linha válida
</pre>
<p>Resultado:</p>
<pre>
3 operações processadas
1 rejeição registrada
job continua
</pre>
<h1>41. Teste de arquivo inválido</h1>
<pre>
header incorreto
layout desconhecido
versão incompatível
arquivo corrompido
</pre>
<p>
    Nesse cenário, o job deverá falhar antes de iniciar processamento
    financeiro relevante, quando possível.
</p>
<h1>42. Teste de Adapter independente da infraestrutura</h1>
<p>
    Deverá ser possível executar:
</p>
<pre><code>var operacao =
    adapter.Adaptar(dto, contexto);</code></pre>
<p>Sem:</p>
<pre>
S3
DynamoDB
EventBridge
AWS Batch
B3
</pre>
<p>
    Isso permitirá testar toda a tradução de contrato
    de forma rápida e isolada.
</p>
<h1>43. POC mínima</h1>
<pre>
S3
 ↓
EventBridge
 ↓
Batch
 ↓
streaming CSV
 ↓
Adapter RDB
 ↓
modelo canônico
 ↓
validação
 ↓
TransactWriteItems
 ↓
OPERACOES
+
REGISTROS_CLEARING
</pre>
<p>
    Utilizar inicialmente um arquivo pequeno para validação funcional
    e depois executar carga progressiva.
</p>
<h1>44. Massa mínima da POC</h1>
<pre>
RDB aplicação válida
RDB resgate válido
CDB aplicação válida, se já suportado
operação duplicada
operação inválida
clearing inválida
valor inválido
data inválida
</pre>
<h1>45. Requisitos</h1>
<ol>
    <li><strong>RF01.</strong> O EventBridge deverá iniciar o AWS Batch ao receber o evento configurado do S3.</li>
    <li><strong>RF02.</strong> O Batch deverá receber bucket e chave do objeto como parâmetros.</li>
    <li><strong>RF03.</strong> O arquivo deverá ser lido via streaming.</li>
    <li><strong>RF04.</strong> O arquivo não deverá ser carregado integralmente em memória.</li>
    <li><strong>RF05.</strong> O Parser deverá ser independente do Adapter.</li>
    <li><strong>RF06.</strong> O Parser deverá transformar a representação física da linha em DTO externo.</li>
    <li><strong>RF07.</strong> O Adapter deverá converter o DTO externo para o modelo canônico.</li>
    <li><strong>RF08.</strong> O domínio não deverá conhecer o layout CSV.</li>
    <li><strong>RF09.</strong> Deverá existir Adapter específico por contrato/produto quando houver diferenças relevantes de transformação.</li>
    <li><strong>RF10.</strong> A seleção do Adapter deverá ser centralizada através de Resolver/Factory ou mecanismo equivalente.</li>
    <li><strong>RF11.</strong> A inclusão de novo produto não deverá exigir alteração do fluxo principal de ingestão.</li>
    <li><strong>RF12.</strong> O modelo produzido deverá seguir o contrato canônico versionado.</li>
    <li><strong>RF13.</strong> O modelo deverá ser validado após adaptação.</li>
    <li><strong>RF14.</strong> Deverão existir validações estruturais, canônicas e específicas de produto.</li>
    <li><strong>RF15.</strong> Cada operação deverá receber idOperacao.</li>
    <li><strong>RF16.</strong> Cada operação deverá possuir chaveIdempotencia.</li>
    <li><strong>RF17.</strong> O processamento deverá ser idempotente.</li>
    <li><strong>RF18.</strong> A idempotência deverá suportar concorrência.</li>
    <li><strong>RF19.</strong> Operações válidas deverão ser persistidas em OPERACOES.</li>
    <li><strong>RF20.</strong> Deverá ser criado estado inicial em REGISTROS_CLEARING.</li>
    <li><strong>RF21.</strong> A criação de operação e registro deverá ser atomicamente consistente.</li>
    <li><strong>RF22.</strong> Deverá ser avaliado/utilizado TransactWriteItems.</li>
    <li><strong>RF23.</strong> Linhas inválidas não deverão necessariamente interromper o arquivo inteiro.</li>
    <li><strong>RF24.</strong> Erros deverão ser classificados entre erro da linha e erro estrutural/técnico.</li>
    <li><strong>RF25.</strong> A concorrência interna deverá ser limitada e configurável.</li>
    <li><strong>RF26.</strong> A aplicação deverá produzir logs estruturados.</li>
    <li><strong>RF27.</strong> A aplicação deverá produzir métricas de processamento.</li>
    <li><strong>RF28.</strong> Não deverão ser produzidos logs INFO individualmente para todas as linhas.</li>
    <li><strong>RF29.</strong> Retry do Batch não poderá gerar duplicidade.</li>
    <li><strong>RF30.</strong> A infraestrutura deverá ser criada via Terraform.</li>
    <li><strong>RF31.</strong> A aplicação deverá executar em container.</li>
    <li><strong>RF32.</strong> A imagem deverá ser versionada.</li>
    <li><strong>RF33.</strong> As permissões AWS deverão seguir princípio de menor privilégio.</li>
    <li><strong>RF34.</strong> O Batch não deverá chamar diretamente a B3.</li>
    <li><strong>RF35.</strong> A arquitetura deverá permitir futuramente uma entrada Kafka produzindo o mesmo modelo canônico.</li>
</ol>
<h1>46. Critérios de aceite</h1>
<ol>
    <li><strong>CA01.</strong> Upload de arquivo válido no local configurado inicia o processamento esperado.</li>
    <li><strong>CA02.</strong> O Batch recebe corretamente bucket e object key.</li>
    <li><strong>CA03.</strong> O arquivo é processado via streaming.</li>
    <li><strong>CA04.</strong> O consumo de memória não cresce proporcionalmente ao tamanho integral do arquivo.</li>
    <li><strong>CA05.</strong> O Parser interpreta corretamente o layout esperado.</li>
    <li><strong>CA06.</strong> O Adapter RDB produz o modelo canônico esperado.</li>
    <li><strong>CA07.</strong> O Adapter pode ser testado sem dependências AWS.</li>
    <li><strong>CA08.</strong> Uma aplicação RDB válida é persistida corretamente.</li>
    <li><strong>CA09.</strong> Um resgate RDB válido é persistido corretamente.</li>
    <li><strong>CA10.</strong> OPERACOES recebe o envelope canônico e dadosOperacao.</li>
    <li><strong>CA11.</strong> REGISTROS_CLEARING recebe o estado inicial esperado.</li>
    <li><strong>CA12.</strong> A criação das duas estruturas não produz estado parcial.</li>
    <li><strong>CA13.</strong> Uma operação duplicada não produz nova operação financeira.</li>
    <li><strong>CA14.</strong> Duas tentativas concorrentes da mesma operação não produzem duplicidade.</li>
    <li><strong>CA15.</strong> Uma linha inválida é identificada e contabilizada.</li>
    <li><strong>CA16.</strong> Uma linha inválida não interrompe indevidamente todo o arquivo.</li>
    <li><strong>CA17.</strong> Um arquivo estruturalmente inválido é rejeitado conforme política definida.</li>
    <li><strong>CA18.</strong> Falha e retry do Batch não geram duplicidade.</li>
    <li><strong>CA19.</strong> Logs permitem identificar arquivo, job e erro.</li>
    <li><strong>CA20.</strong> Métricas apresentam quantidade processada, persistida, inválida e duplicada.</li>
    <li><strong>CA21.</strong> Teste de carga demonstra throughput do Batch.</li>
    <li><strong>CA22.</strong> Teste de carga demonstra utilização de CPU e memória.</li>
    <li><strong>CA23.</strong> Teste de carga demonstra comportamento do DynamoDB.</li>
    <li><strong>CA24.</strong> Não existe chamada direta do Batch para B3.</li>
    <li><strong>CA25.</strong> Inclusão de um segundo Adapter não exige alteração relevante no fluxo principal.</li>
    <li><strong>CA26.</strong> A configuração AWS está representada em Terraform.</li>
</ol>
<h1>47. Definição de pronto</h1>
<p>
    A história será considerada concluída quando estiverem
    implementados e demonstrados:
</p>
<pre>
Infraestrutura Batch
        +
Container .NET
        +
EventBridge
        +
Leitura streaming S3
        +
Parser CSV
        +
Adapter RDB
        +
Modelo canônico
        +
Validação
        +
Idempotência
        +
Persistência atômica
        +
OPERACOES
        +
REGISTROS_CLEARING
        +
Logs
        +
Métricas
        +
Testes unitários
        +
Testes de integração
        +
Teste de idempotência
        +
Teste de falha/retry
        +
Teste de carga
</pre>
<div class="success">
    <strong>Resultado esperado:</strong>
    o contrato de entrada deverá permanecer isolado do domínio,
    e o Batch deverá conseguir ingerir grandes volumes sem acoplar
    a plataforma de clearing ao formato do arquivo.
</div>
</body>
</html>





<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>H03 - Criar estrutura DynamoDB para operações, registros e histórico</title>
<style>
body {
    font-family: Arial, Helvetica, sans-serif;
    line-height: 1.6;
    color: #24292f;
    max-width: 1200px;
    margin: 0 auto;
    padding: 40px;
}
h1 {
    color: #1f4e79;
    border-bottom: 2px solid #d0d7de;
    padding-bottom: 8px;
    margin-top: 40px;
}
h2 {
    color: #2f5f8f;
    margin-top: 30px;
}
h3 {
    color: #444;
    margin-top: 24px;
}
pre {
    background-color: #f6f8fa;
    border: 1px solid #d0d7de;
    border-radius: 6px;
    padding: 16px;
    overflow-x: auto;
}
code {
    font-family: Consolas, Monaco, monospace;
}
table {
    border-collapse: collapse;
    width: 100%;
    margin: 20px 0;
}
th, td {
    border: 1px solid #d0d7de;
    padding: 10px;
    text-align: left;
    vertical-align: top;
}
th {
    background-color: #f6f8fa;
}
blockquote {
    border-left: 4px solid #1f4e79;
    padding: 10px 20px;
    margin: 20px 0;
    background-color: #f6f8fa;
}
.important {
    background-color: #fff8c5;
    border-left: 4px solid #d4a72c;
    padding: 12px;
    margin: 20px 0;
}
.success {
    background-color: #dafbe1;
    border-left: 4px solid #2da44e;
    padding: 12px;
    margin: 20px 0;
}
</style>
</head>
<body>
<h1>Título</h1>
<p>
Criar estrutura DynamoDB para operações, registros e histórico
do processo de registro em clearing.
</p>
<h1>Objetivo</h1>
<p>
Criar a estrutura de persistência DynamoDB da plataforma de registro
em clearing utilizando três tabelas:
</p>
<ol>
    <li><code>OPERACOES</code></li>
    <li><code>REGISTROS_CLEARING</code></li>
    <li><code>EVENTOS_REGISTRO</code></li>
</ol>
<p>A modelagem deverá suportar:</p>
<ul>
    <li>alta volumetria;</li>
    <li>múltiplos produtos;</li>
    <li>múltiplas clearings;</li>
    <li>uma única clearing por operação;</li>
    <li>origem inicial via CSV;</li>
    <li>origem futura via Kafka;</li>
    <li>idempotência;</li>
    <li>correlação com sistemas externos;</li>
    <li>rastreabilidade;</li>
    <li>evolução dos contratos;</li>
    <li>novos produtos sem remodelagem constante das tabelas;</li>
    <li>novas clearings sem acoplamento do modelo interno ao contrato externo.</li>
</ul>
<p>
A infraestrutura oficial deverá ser criada através de
<strong>Terraform</strong>.
</p>
<p>
Antes da implementação definitiva, as tabelas poderão ser criadas
manualmente no AWS Console para realização da POC.
</p>
<h1>1. Princípio da modelagem</h1>
<p>
O DynamoDB não possui um schema rígido para todos os atributos dos itens.
Entretanto, isso não significa que a aplicação não deverá possuir
um contrato definido.
</p>
<p>
A plataforma deverá possuir um
<strong>modelo canônico explicitamente definido e versionado</strong>.
</p>
<blockquote>
Ausência de schema físico no DynamoDB não significa ausência
de contrato de domínio.
</blockquote>
<p>A estrutura seguirá o princípio:</p>
<pre>
          ITEM DYNAMODB
                │
       ┌────────┴────────┐
       │                 │
       ▼                 ▼
ENVELOPE CANÔNICO    DADOS FLEXÍVEIS
       │                 │
       │                 ├── dadosOperacao
       │                 ├── dadosRegistro
       │                 └── dadosEvento
       │
       ├── identidade
       ├── roteamento
       ├── correlação
       ├── idempotência
       ├── controle
       ├── auditoria
       └── versão
</pre>
<h1>2. Regra para definição dos atributos</h1>
<p>
Um atributo deverá permanecer no primeiro nível do item quando for
necessário para pelo menos uma das seguintes finalidades:
</p>
<ul>
    <li>Partition Key;</li>
    <li>Sort Key;</li>
    <li>Global Secondary Index;</li>
    <li>identificação;</li>
    <li>correlação;</li>
    <li>roteamento;</li>
    <li>idempotência;</li>
    <li>controle do workflow;</li>
    <li>versionamento;</li>
    <li>auditoria.</li>
</ul>
<p>
Dados específicos do produto, clearing, operação ou evento deverão,
preferencialmente, ficar dentro do Map correspondente.
</p>
<h1>3. Independência das origens</h1>
<p>
O modelo DynamoDB não deverá representar diretamente o contrato
do CSV, Kafka ou qualquer outra origem.
</p>
<pre>
CSV
 │
 ▼
Adapter CSV
 │
 └───────────────┐
                 │
                 ▼
          MODELO CANÔNICO
                 ▲
                 │
Kafka Adapter ───┘
 ▲
 │
Kafka
</pre>
<p>
A plataforma deverá persistir o mesmo modelo canônico
independentemente da origem.
</p>
<h1>4. Independência das clearings</h1>
<p>
O modelo também não deverá representar diretamente o contrato
da B3 ou de outra clearing.
</p>
<pre>
             MODELO CANÔNICO
                    │
                    ▼
             Clearing Router
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
      Adapter B3 Adapter X Adapter Y
          │         │         │
          ▼         ▼         ▼
          B3    Clearing X Clearing Y
</pre>
<blockquote>
A estrutura DynamoDB pertence ao domínio da plataforma de clearing
e não às interfaces externas.
</blockquote>
<h1>5. Visão geral das tabelas</h1>
<pre>
                    OPERACOES
                       │
                       │ 1 : 1
                       ▼
              REGISTROS_CLEARING
                       │
                       │ 1 : N
                       ▼
                EVENTOS_REGISTRO
</pre>
<table>
<thead>
<tr>
    <th>Tabela</th>
    <th>Responsabilidade</th>
</tr>
</thead>
<tbody>
<tr>
    <td>OPERACOES</td>
    <td>Dado canônico da operação financeira.</td>
</tr>
<tr>
    <td>REGISTROS_CLEARING</td>
    <td>Estado operacional atual do registro da operação.</td>
</tr>
<tr>
    <td>EVENTOS_REGISTRO</td>
    <td>Histórico append-only do processo de registro.</td>
</tr>
</tbody>
</table>
<h1>6. Tabela OPERACOES</h1>
<h2>Finalidade</h2>
<p>
Representar uma operação financeira recebida pela plataforma.
</p>
<p>
Cada operação deverá possuir exatamente
<strong>uma clearing de destino</strong>.
</p>
<h2>Chaves</h2>
<pre>
PK = idOperacao
SK = não possui
</pre>
<p>
A ausência de Sort Key é intencional.
</p>
<p>
Existe exatamente um item de operação para cada
<code>idOperacao</code>.
</p>
<p>
Adicionar uma SK constante não acrescentaria um novo padrão
de acesso e aumentaria desnecessariamente a modelagem.
</p>
<h1>7. Estrutura da tabela OPERACOES</h1>
<table>
<thead>
<tr>
    <th>Campo</th>
    <th>Tipo</th>
    <th>Obrigatório</th>
    <th>Finalidade</th>
</tr>
</thead>
<tbody>
<tr>
<td>idOperacao</td>
<td>String</td>
<td>Sim</td>
<td>Partition Key e identificador interno da operação.</td>
</tr>
<tr>
<td>produto</td>
<td>String</td>
<td>Sim</td>
<td>Produto financeiro, como RDB, CDB etc.</td>
</tr>
<tr>
<td>tipoOperacao</td>
<td>String</td>
<td>Sim</td>
<td>APLICACAO ou RESGATE.</td>
</tr>
<tr>
<td>clearingDestino</td>
<td>String</td>
<td>Sim</td>
<td>Clearing responsável pelo registro.</td>
</tr>
<tr>
<td>chaveIdempotencia</td>
<td>String</td>
<td>Sim</td>
<td>Identidade lógica utilizada na estratégia de idempotência.</td>
</tr>
<tr>
<td>versaoModelo</td>
<td>Number</td>
<td>Sim</td>
<td>Versão do contrato canônico.</td>
</tr>
<tr>
<td>origem</td>
<td>Map</td>
<td>Sim</td>
<td>Informações sobre a origem da operação.</td>
</tr>
<tr>
<td>dataHoraInclusao</td>
<td>String</td>
<td>Sim</td>
<td>Auditoria.</td>
</tr>
<tr>
<td>dataHoraAlteracao</td>
<td>String</td>
<td>Sim</td>
<td>Auditoria.</td>
</tr>
<tr>
<td>dadosOperacao</td>
<td>Map</td>
<td>Sim</td>
<td>Dados específicos do produto/operação.</td>
</tr>
</tbody>
</table>
<h1>8. Representação de OPERACOES</h1>
<pre>
OPERACOES
│
├── idOperacao                PK
├── produto
├── tipoOperacao
├── clearingDestino
├── chaveIdempotencia
├── versaoModelo
├── origem
│   ├── tipo
│   └── identificador
│
├── dataHoraInclusao
├── dataHoraAlteracao
│
└── dadosOperacao
      └── Map flexível
</pre>
<h1>9. dadosOperacao</h1>
<p>
O campo <code>dadosOperacao</code> deverá armazenar informações
específicas do produto e da operação.
</p>
<p>Exemplos:</p>
<ul>
    <li>valor;</li>
    <li>data da operação;</li>
    <li>vencimento;</li>
    <li>taxa;</li>
    <li>indexador;</li>
    <li>código do instrumento;</li>
    <li>dados específicos do RDB;</li>
    <li>dados específicos do CDB;</li>
    <li>futuros campos de novos produtos.</li>
</ul>
<h1>10. Exemplo de operação RDB</h1>
<pre><code>{
  "idOperacao": "OP-000001",
  "produto": "RDB",
  "tipoOperacao": "APLICACAO",
  "clearingDestino": "B3",
  "chaveIdempotencia": "ARQUIVO#20261005#000001",
  "versaoModelo": 1,
  "origem": {
    "tipo": "ARQUIVO",
    "identificador": "ARQ-000001"
  },
  "dataHoraInclusao": "2026-10-05T10:00:00.000Z",
  "dataHoraAlteracao": "2026-10-05T10:00:00.000Z",
  "dadosOperacao": {
    "valor": 15000.50,
    "dataOperacao": "2026-10-05",
    "codigoRdb": "RDB001",
    "dataVencimento": "2028-10-05",
    "taxa": 0.125
  }
}</code></pre>
<h1>11. Exemplo de operação CDB</h1>
<pre><code>{
  "idOperacao": "OP-000003",
  "produto": "CDB",
  "tipoOperacao": "APLICACAO",
  "clearingDestino": "B3",
  "chaveIdempotencia": "ARQUIVO#20261005#000003",
  "versaoModelo": 1,
  "origem": {
    "tipo": "ARQUIVO",
    "identificador": "ARQ-000003"
  },
  "dataHoraInclusao": "2026-10-05T10:02:00.000Z",
  "dataHoraAlteracao": "2026-10-05T10:02:00.000Z",
  "dadosOperacao": {
    "valor": 25000.00,
    "dataOperacao": "2026-10-05",
    "codigoCdb": "CDB001",
    "indexador": "CDI",
    "percentualIndexador": 105,
    "dataVencimento": "2029-10-05"
  }
}</code></pre>
<h1>12. Versionamento do modelo</h1>
<p>
O campo <code>versaoModelo</code> deverá identificar explicitamente
a versão do contrato canônico.
</p>
<pre>
produto = RDB
versaoModelo = 1
        ↓
ValidadorRdbV1
produto = RDB
versaoModelo = 2
        ↓
ValidadorRdbV2
</pre>
<p>
Isso permitirá evolução controlada sem reinterpretar silenciosamente
dados históricos.
</p>
<h1>13. Idempotência</h1>
<p>
A tabela <code>OPERACOES</code> deverá possuir
<code>chaveIdempotencia</code>.
</p>
<p>Exemplo para arquivo:</p>
<pre>
ARQUIVO#20261005#000001
</pre>
<p>Exemplo futuro para Kafka:</p>
<pre>
KAFKA#TOPICO-OPERACOES#12#987654
</pre>
<p>Poderá ser avaliado o GSI:</p>
<pre>
INDICE_IDEMPOTENCIA
PK = chaveIdempotencia
</pre>
<div class="important">
<strong>Importante:</strong>
um GSI não deverá ser utilizado sozinho como mecanismo
de garantia de unicidade.
</div>
<p>
A estratégia definitiva deverá considerar concorrência e
escrita condicional e/ou uma estrutura dedicada ao controle
de idempotência.
</p>
<h1>14. Tabela REGISTROS_CLEARING</h1>
<h2>Finalidade</h2>
<p>
Representar o estado operacional atual do registro
da operação na clearing.
</p>
<pre>
OPERACAO 1 ───────── 1 REGISTRO_CLEARING
</pre>
<h2>Chaves</h2>
<pre>
PK = idOperacao
SK = não possui
</pre>
<p>
A ausência de Sort Key também é intencional.
</p>
<pre>
uma operação
      │
      ▼
um registro atual
</pre>
<h1>15. Estrutura da tabela REGISTROS_CLEARING</h1>
<table>
<thead>
<tr>
    <th>Campo</th>
    <th>Tipo</th>
    <th>Obrigatório</th>
    <th>Finalidade</th>
</tr>
</thead>
<tbody>
<tr>
<td>idOperacao</td>
<td>String</td>
<td>Sim</td>
<td>Partition Key.</td>
</tr>
<tr>
<td>idRegistro</td>
<td>String</td>
<td>Sim</td>
<td>Identificador interno do registro.</td>
</tr>
<tr>
<td>clearing</td>
<td>String</td>
<td>Sim</td>
<td>Clearing utilizada.</td>
</tr>
<tr>
<td>status</td>
<td>String</td>
<td>Sim</td>
<td>Estado operacional atual.</td>
</tr>
<tr>
<td>idExterno</td>
<td>String</td>
<td>Não</td>
<td>Identificador retornado/utilizado pelo sistema externo.</td>
</tr>
<tr>
<td>tentativa</td>
<td>Number</td>
<td>Sim</td>
<td>Número da tentativa atual.</td>
</tr>
<tr>
<td>versaoModelo</td>
<td>Number</td>
<td>Sim</td>
<td>Versão do contrato.</td>
</tr>
<tr>
<td>dataHoraInclusao</td>
<td>String</td>
<td>Sim</td>
<td>Auditoria.</td>
</tr>
<tr>
<td>dataHoraAlteracao</td>
<td>String</td>
<td>Sim</td>
<td>Auditoria.</td>
</tr>
<tr>
<td>dadosRegistro</td>
<td>Map</td>
<td>Não</td>
<td>Dados variáveis do processo de registro.</td>
</tr>
</tbody>
</table>
<h1>16. Representação de REGISTROS_CLEARING</h1>
<pre>
REGISTROS_CLEARING
│
├── idOperacao             PK
├── idRegistro
├── clearing
├── status
├── idExterno
├── tentativa
├── versaoModelo
├── dataHoraInclusao
├── dataHoraAlteracao
│
└── dadosRegistro
      └── Map flexível
</pre>
<h1>17. dadosRegistro</h1>
<p>
Informações específicas da clearing ou do processamento que não
sejam necessárias para consulta, correlação ou controle poderão
ficar dentro de <code>dadosRegistro</code>.
</p>
<pre><code>{
  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "clearing": "B3",
  "status": "REGISTRADO",
  "idExterno": "B3-20261005-000001",
  "tentativa": 1,
  "versaoModelo": 1,
  "dataHoraInclusao": "2026-10-05T10:00:10.000Z",
  "dataHoraAlteracao": "2026-10-05T10:05:00.000Z",
  "dadosRegistro": {
    "protocolo": "PROTOCOLO-B3-001",
    "codigoRetorno": "00",
    "mensagemRetorno": "Registro realizado com sucesso"
  }
}</code></pre>
<p>
O campo <code>idExterno</code> deverá permanecer no primeiro nível,
pois será utilizado para correlação e consulta.
</p>
<h1>18. GSI de correlação externa</h1>
<p>Deverá ser criado:</p>
<pre>
INDICE_ID_EXTERNO
PK = idExterno
</pre>
<p>Fluxo conceitual:</p>
<pre>
Sistema externo
      │
      │ retorno
      ▼
     SNS
      │
      ▼
SQS RETORNO
      │
      ▼
Return Worker
      │
      │ idExterno
      ▼
INDICE_ID_EXTERNO
      │
      ▼
idOperacao
</pre>
<p>
Isso permitirá localizar rapidamente a operação interna
utilizando o identificador retornado pelo sistema externo.
</p>
<h1>19. Tabela EVENTOS_REGISTRO</h1>
<h2>Finalidade</h2>
<p>
Armazenar a timeline <strong>append-only</strong> do processo
de registro.
</p>
<pre>
OP-000001
   │
   ├── REGISTRO_CRIADO
   ├── ENVIO_INICIADO
   ├── ENVIADO_CLEARING
   └── REGISTRADO
</pre>
<h2>Chaves</h2>
<pre>
PK = idOperacao
SK = chaveEvento
</pre>
<p>A Sort Key deverá possuir o formato:</p>
<pre>
&lt;dataHoraEvento&gt;#&lt;idEvento&gt;
</pre>
<p>Exemplo:</p>
<pre>
2026-10-05T10:00:10.000Z#EVT-001
2026-10-05T10:01:00.000Z#EVT-002
2026-10-05T10:02:00.000Z#EVT-003
2026-10-05T10:05:00.000Z#EVT-004
</pre>
<p>
Isso permitirá recuperar o histórico da operação já ordenado
cronologicamente.
</p>
<h1>20. Estrutura da tabela EVENTOS_REGISTRO</h1>
<table>
<thead>
<tr>
    <th>Campo</th>
    <th>Tipo</th>
    <th>Obrigatório</th>
    <th>Finalidade</th>
</tr>
</thead>
<tbody>
<tr>
<td>idOperacao</td>
<td>String</td>
<td>Sim</td>
<td>Partition Key.</td>
</tr>
<tr>
<td>chaveEvento</td>
<td>String</td>
<td>Sim</td>
<td>Sort Key cronológica.</td>
</tr>
<tr>
<td>idEvento</td>
<td>String</td>
<td>Sim</td>
<td>Identificador único do evento.</td>
</tr>
<tr>
<td>idRegistro</td>
<td>String</td>
<td>Sim</td>
<td>Correlação com o registro.</td>
</tr>
<tr>
<td>tipoEvento</td>
<td>String</td>
<td>Sim</td>
<td>Tipo do evento.</td>
</tr>
<tr>
<td>statusAnterior</td>
<td>String</td>
<td>Não</td>
<td>Estado anterior.</td>
</tr>
<tr>
<td>statusAtual</td>
<td>String</td>
<td>Não</td>
<td>Novo estado.</td>
</tr>
<tr>
<td>origemEvento</td>
<td>String</td>
<td>Sim</td>
<td>Componente responsável pelo evento.</td>
</tr>
<tr>
<td>dataHoraEvento</td>
<td>String</td>
<td>Sim</td>
<td>Data/hora do evento.</td>
</tr>
<tr>
<td>versaoModelo</td>
<td>Number</td>
<td>Sim</td>
<td>Versão do contrato.</td>
</tr>
<tr>
<td>dadosEvento</td>
<td>Map</td>
<td>Não</td>
<td>Payload específico do evento.</td>
</tr>
</tbody>
</table>
<h1>21. Representação de EVENTOS_REGISTRO</h1>
<pre>
EVENTOS_REGISTRO
│
├── idOperacao             PK
├── chaveEvento            SK
├── idEvento
├── idRegistro
├── tipoEvento
├── statusAnterior
├── statusAtual
├── origemEvento
├── dataHoraEvento
├── versaoModelo
│
└── dadosEvento
      └── Map flexível
</pre>
<h1>22. Exemplo de evento de sucesso</h1>
<pre><code>{
  "idOperacao": "OP-000001",
  "chaveEvento":
    "2026-10-05T10:05:00.000Z#EVT-004",
  "idEvento": "EVT-004",
  "idRegistro": "REG-000001",
  "tipoEvento": "STATUS_ALTERADO",
  "statusAnterior": "ENVIADO",
  "statusAtual": "REGISTRADO",
  "origemEvento": "RETORNO_CLEARING",
  "dataHoraEvento":
    "2026-10-05T10:05:00.000Z",
  "versaoModelo": 1,
  "dadosEvento": {
    "idExterno": "B3-20261005-000001",
    "protocolo": "PROTOCOLO-B3-001",
    "codigoRetorno": "00",
    "mensagem": "Registro realizado com sucesso"
  }
}</code></pre>
<h1>23. Exemplo de evento de erro</h1>
<pre><code>{
  "idOperacao": "OP-000003",
  "chaveEvento":
    "2026-10-05T10:07:00.000Z#EVT-010",
  "idEvento": "EVT-010",
  "idRegistro": "REG-000003",
  "tipoEvento": "ERRO_REGISTRO",
  "statusAnterior": "EM_PROCESSAMENTO",
  "statusAtual": "ERRO",
  "origemEvento": "ADAPTER_B3",
  "dataHoraEvento":
    "2026-10-05T10:07:00.000Z",
  "versaoModelo": 1,
  "dadosEvento": {
    "codigoErro": "B3-001",
    "descricao": "Instrumento não encontrado",
    "tentativa": 2,
    "reprocessavel": true
  }
}</code></pre>
<h1>24. Resumo das chaves</h1>
<table>
<thead>
<tr>
    <th>Tabela</th>
    <th>Partition Key</th>
    <th>Sort Key</th>
</tr>
</thead>
<tbody>
<tr>
<td>OPERACOES</td>
<td>idOperacao</td>
<td>Não possui</td>
</tr>
<tr>
<td>REGISTROS_CLEARING</td>
<td>idOperacao</td>
<td>Não possui</td>
</tr>
<tr>
<td>EVENTOS_REGISTRO</td>
<td>idOperacao</td>
<td>chaveEvento</td>
</tr>
</tbody>
</table>
<h1>25. Resumo dos GSIs</h1>
<table>
<thead>
<tr>
    <th>Tabela</th>
    <th>Índice</th>
    <th>Partition Key</th>
    <th>Finalidade</th>
</tr>
</thead>
<tbody>
<tr>
<td>OPERACOES</td>
<td>INDICE_IDEMPOTENCIA</td>
<td>chaveIdempotencia</td>
<td>Localizar operação pela identidade lógica. Uso definitivo depende da estratégia de idempotência.</td>
</tr>
<tr>
<td>REGISTROS_CLEARING</td>
<td>INDICE_ID_EXTERNO</td>
<td>idExterno</td>
<td>Localizar operação através do identificador externo.</td>
</tr>
<tr>
<td>EVENTOS_REGISTRO</td>
<td>Nenhum inicialmente</td>
<td>-</td>
<td>O padrão inicial é consultar histórico diretamente por idOperacao.</td>
</tr>
</tbody>
</table>
<h1>26. Atomicidade entre registro e histórico</h1>
<p>
Quando ocorrer uma alteração de estado, o snapshot atual e
o histórico deverão permanecer consistentes.
</p>
<p>Exemplo:</p>
<pre>
ENVIADO
   │
   ▼
REGISTRADO
</pre>
<p>A alteração poderá utilizar:</p>
<pre>
TransactWriteItems
       │
       ├── UPDATE REGISTROS_CLEARING
       │
       │      status = REGISTRADO
       │
       └── PUT EVENTOS_REGISTRO
              ENVIADO → REGISTRADO
</pre>
<p>Resultado:</p>
<pre>
TUDO
 OU
NADA
</pre>
<p>
Não deverá existir situação final em que o status esteja atualizado
sem que o evento correspondente tenha sido persistido, ou vice-versa.
</p>
<h1>27. DynamoDB Streams</h1>
<p>
Somente <code>OPERACOES</code> deverá possuir inicialmente o Stream
utilizado para iniciar o fluxo de registro.
</p>
<pre>
OPERACOES
    │
    ▼
DynamoDB Streams
    │
    ▼
EventBridge Pipes
    │
    ▼
SQS REGISTRO
</pre>
<p>
Alterações em <code>REGISTROS_CLEARING</code> e
<code>EVENTOS_REGISTRO</code> não deverão iniciar automaticamente
um novo envio para clearing.
</p>
<h1>28. Configuração manual para POC</h1>
<h2>OPERACOES</h2>
<table>
<tbody>
<tr><th>Configuração</th><th>Valor</th></tr>
<tr><td>Table name</td><td>OPERACOES</td></tr>
<tr><td>Partition key</td><td>idOperacao</td></tr>
<tr><td>Partition key type</td><td>String</td></tr>
<tr><td>Sort key</td><td>Não</td></tr>
<tr><td>Capacity mode</td><td>On-demand</td></tr>
<tr><td>Stream</td><td>Habilitado para POC</td></tr>
<tr><td>GSI opcional</td><td>INDICE_IDEMPOTENCIA</td></tr>
<tr><td>GSI Partition Key</td><td>chaveIdempotencia</td></tr>
</tbody>
</table>
<h2>REGISTROS_CLEARING</h2>
<table>
<tbody>
<tr><th>Configuração</th><th>Valor</th></tr>
<tr><td>Table name</td><td>REGISTROS_CLEARING</td></tr>
<tr><td>Partition key</td><td>idOperacao</td></tr>
<tr><td>Partition key type</td><td>String</td></tr>
<tr><td>Sort key</td><td>Não</td></tr>
<tr><td>Capacity mode</td><td>On-demand</td></tr>
<tr><td>GSI</td><td>INDICE_ID_EXTERNO</td></tr>
<tr><td>GSI Partition Key</td><td>idExterno</td></tr>
</tbody>
</table>
<h2>EVENTOS_REGISTRO</h2>
<table>
<tbody>
<tr><th>Configuração</th><th>Valor</th></tr>
<tr><td>Table name</td><td>EVENTOS_REGISTRO</td></tr>
<tr><td>Partition key</td><td>idOperacao</td></tr>
<tr><td>Partition key type</td><td>String</td></tr>
<tr><td>Sort key</td><td>chaveEvento</td></tr>
<tr><td>Sort key type</td><td>String</td></tr>
<tr><td>Capacity mode</td><td>On-demand</td></tr>
<tr><td>Stream</td><td>Não</td></tr>
<tr><td>GSI</td><td>Nenhum inicialmente</td></tr>
</tbody>
</table>
<h1>29. Massa da POC</h1>
<p>
A POC deverá possuir ao menos os seguintes cenários:
</p>
<pre>
OP-000001 | RDB | APLICACAO | B3 | REGISTRADO
OP-000002 | RDB | RESGATE   | B3 | EM_PROCESSAMENTO
OP-000003 | CDB | APLICACAO | B3 | ERRO
OP-000004 | RDB | APLICACAO | B3 | PENDENTE
</pre>
<p>
Os itens deverão seguir o modelo de envelope canônico +
Maps definido nesta história.
</p>
<h1>30. Massa POC - Operação 1</h1>
<pre><code>{
  "idOperacao": "OP-000001",
  "produto": "RDB",
  "tipoOperacao": "APLICACAO",
  "clearingDestino": "B3",
  "chaveIdempotencia": "ARQUIVO#20261005#000001",
  "versaoModelo": 1,
  "origem": {
    "tipo": "ARQUIVO",
    "identificador": "ARQ-000001"
  },
  "dataHoraInclusao": "2026-10-05T10:00:00.000Z",
  "dataHoraAlteracao": "2026-10-05T10:00:00.000Z",
  "dadosOperacao": {
    "valor": 15000.50,
    "dataOperacao": "2026-10-05",
    "codigoRdb": "RDB001"
  }
}</code></pre>
<h1>31. Massa POC - Registro 1</h1>
<pre><code>{
  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "clearing": "B3",
  "status": "REGISTRADO",
  "idExterno": "B3-20261005-000001",
  "tentativa": 1,
  "versaoModelo": 1,
  "dataHoraInclusao": "2026-10-05T10:00:10.000Z",
  "dataHoraAlteracao": "2026-10-05T10:05:00.000Z",
  "dadosRegistro": {
    "protocolo": "PROTOCOLO-B3-001",
    "codigoRetorno": "00"
  }
}</code></pre>
<h1>32. Massa POC - Eventos da operação 1</h1>
<h2>Registro criado</h2>
<pre><code>{
  "idOperacao": "OP-000001",
  "chaveEvento": "2026-10-05T10:00:10.000Z#EVT-001",
  "idEvento": "EVT-001",
  "idRegistro": "REG-000001",
  "tipoEvento": "REGISTRO_CRIADO",
  "statusAtual": "PENDENTE",
  "origemEvento": "INGESTAO",
  "dataHoraEvento": "2026-10-05T10:00:10.000Z",
  "versaoModelo": 1,
  "dadosEvento": {}
}</code></pre>
<h2>Enviado</h2>
<pre><code>{
  "idOperacao": "OP-000001",
  "chaveEvento": "2026-10-05T10:02:00.000Z#EVT-002",
  "idEvento": "EVT-002",
  "idRegistro": "REG-000001",
  "tipoEvento": "STATUS_ALTERADO",
  "statusAnterior": "PENDENTE",
  "statusAtual": "ENVIADO",
  "origemEvento": "REGISTRATION_WORKER",
  "dataHoraEvento": "2026-10-05T10:02:00.000Z",
  "versaoModelo": 1,
  "dadosEvento": {}
}</code></pre>
<h2>Registrado</h2>
<pre><code>{
  "idOperacao": "OP-000001",
  "chaveEvento": "2026-10-05T10:05:00.000Z#EVT-003",
  "idEvento": "EVT-003",
  "idRegistro": "REG-000001",
  "tipoEvento": "STATUS_ALTERADO",
  "statusAnterior": "ENVIADO",
  "statusAtual": "REGISTRADO",
  "origemEvento": "RETORNO_CLEARING",
  "dataHoraEvento": "2026-10-05T10:05:00.000Z",
  "versaoModelo": 1,
  "dadosEvento": {
    "idExterno": "B3-20261005-000001",
    "protocolo": "PROTOCOLO-B3-001"
  }
}</code></pre>
<h1>33. Consultar uma operação</h1>
<pre>
Tabela: OPERACOES
Operação:
GetItem
Partition Key:
idOperacao = OP-000001
</pre>
<p>
Essa consulta deverá ser direta, sem Scan.
</p>
<h1>34. Consultar estado atual do registro</h1>
<pre>
Tabela: REGISTROS_CLEARING
Operação:
GetItem
Partition Key:
idOperacao = OP-000001
</pre>
<h1>35. Consultar todo o histórico de uma operação</h1>
<pre>
Tabela: EVENTOS_REGISTRO
Operação:
Query
Partition Key:
idOperacao = OP-000001
</pre>
<p>Resultado esperado:</p>
<pre>
2026-10-05T10:00:10.000Z#EVT-001
2026-10-05T10:02:00.000Z#EVT-002
2026-10-05T10:05:00.000Z#EVT-003
</pre>
<p>
Como <code>chaveEvento</code> é a Sort Key, o DynamoDB
retornará naturalmente os eventos ordenados.
</p>
<h1>36. Consultar o último evento</h1>
<pre>
Tabela: EVENTOS_REGISTRO
idOperacao = OP-000001
ScanIndexForward = false
Limit = 1
</pre>
<p>
Isso permite recuperar o último evento sem realizar
Scan completo da tabela.
</p>
<h1>37. Localizar operação pelo idExterno</h1>
<pre>
Tabela:
REGISTROS_CLEARING
Índice:
INDICE_ID_EXTERNO
Partition Key:
idExterno = B3-20261005-000001
</pre>
<p>Resultado:</p>
<pre>
idOperacao = OP-000001
</pre>
<h1>38. Consultar operação pela chave de idempotência</h1>
<p>
Caso <code>INDICE_IDEMPOTENCIA</code> seja utilizado:
</p>
<pre>
Tabela:
OPERACOES
Índice:
INDICE_IDEMPOTENCIA
Partition Key:
chaveIdempotencia =
ARQUIVO#20261005#000001
</pre>
<div class="important">
Esta consulta é útil para localização, mas não deverá ser considerada
sozinha como garantia transacional contra duplicidade.
</div>
<h1>39. Consultas analíticas</h1>
<p>
Não deverão ser criados GSIs indiscriminadamente apenas para atender
consultas analíticas como:
</p>
<ul>
    <li>listar todas as operações;</li>
    <li>listar operações por produto;</li>
    <li>listar operações por clearing;</li>
    <li>total em processo de registro;</li>
    <li>total registrado;</li>
    <li>total com erro;</li>
    <li>dashboard por período;</li>
    <li>relatórios operacionais.</li>
</ul>
<p>
Esses padrões deverão ser avaliados posteriormente através de um
<strong>read model</strong>.
</p>
<p>Exemplo futuro:</p>
<pre>
DynamoDB
   │
   ▼
Exportação / ETL
   │
   ▼
S3
   │
   ▼
Athena
   │
   ├── API de consulta
   │
   └── Dashboard
</pre>
<p>
A estrutura definida nesta história deverá ser otimizada principalmente
para o fluxo transacional de registro.
</p>
<h1>40. Volumetria</h1>
<p>
Considerando conceitualmente 20 milhões de operações:
</p>
<pre>
OPERACOES
≈ 20 milhões de itens
</pre>
<pre>
REGISTROS_CLEARING
≈ 20 milhões de itens
+
atualizações de estado
</pre>
<p>
A tabela que apresentará crescimento significativamente maior
será <code>EVENTOS_REGISTRO</code>.
</p>
<p>
Com uma média hipotética de cinco eventos por operação:
</p>
<pre>
20 milhões de operações
×
5 eventos
=
100 milhões de eventos
</pre>
<div class="important">
Os números acima são ilustrativos. O objetivo é demonstrar
a diferença de comportamento de crescimento entre as três tabelas.
</div>
<h1>41. Distribuição das partições</h1>
<p>
As Partition Keys não deverão utilizar valores de baixa cardinalidade,
como:
</p>
<pre>
B3
RDB
REGISTRADO
ERRO
</pre>
<p>
Utilizar esses valores como PK poderia concentrar grande volume
de operações na mesma chave lógica.
</p>
<p>
O identificador <code>idOperacao</code> deverá fornecer alta
cardinalidade e distribuição adequada.
</p>
<h1>42. Por que utilizar três tabelas</h1>
<p>
As três estruturas possuem comportamentos diferentes.
</p>
<table>
<thead>
<tr>
    <th>Tabela</th>
    <th>Comportamento</th>
</tr>
</thead>
<tbody>
<tr>
<td>OPERACOES</td>
<td>Principalmente escrita inicial e leitura.</td>
</tr>
<tr>
<td>REGISTROS_CLEARING</td>
<td>Atualizações frequentes do estado operacional.</td>
</tr>
<tr>
<td>EVENTOS_REGISTRO</td>
<td>Append-only e crescimento muito maior.</td>
</tr>
</tbody>
</table>
<p>
A separação permite tratar capacidade, retenção, índices,
observabilidade e evolução de cada tabela independentemente.
</p>
<h1>43. OPERACOES x REGISTROS_CLEARING</h1>
<p>
As tabelas não deverão ser consideradas redundantes.
</p>
<p>
<code>OPERACOES</code> representa:
</p>
<pre>
O QUE é a operação financeira?
</pre>
<p>
<code>REGISTROS_CLEARING</code> representa:
</p>
<pre>
QUAL é o estado atual do processo
de registro dessa operação?
</pre>
<p>
<code>EVENTOS_REGISTRO</code> representa:
</p>
<pre>
COMO o processo chegou
ao estado atual?
</pre>
<p>Portanto:</p>
<pre>
OPERACOES
    ↓
fato de negócio
REGISTROS_CLEARING
    ↓
snapshot operacional
EVENTOS_REGISTRO
    ↓
timeline/auditoria
</pre>
<h1>44. Reprocessamento futuro</h1>
<p>
A estrutura deverá permitir futuramente reprocessamento de operações
com erro ou estados reprocessáveis.
</p>
<p>Exemplo:</p>
<pre>
REGISTROS_CLEARING
status = ERRO
tentativa = 2
       │
       ▼
Reprocessamento
       │
       ▼
status = PENDENTE_REPROCESSAMENTO
tentativa = 3
</pre>
<p>
Cada mudança deverá produzir um novo evento em
<code>EVENTOS_REGISTRO</code>.
</p>
<h1>45. Conciliação futura</h1>
<p>
A arquitetura também deverá permitir futuramente comparar o estado
interno da plataforma com informações fornecidas pela clearing.
</p>
<pre>
Estado interno
REGISTROS_CLEARING
        │
        │ comparação
        ▼
Retorno / posição Clearing
        │
        ▼
Conciliação
</pre>
<p>
Caso seja encontrada divergência, poderá ser produzido um novo
evento de domínio/auditoria sem alterar os eventos históricos anteriores.
</p>
<h1>46. Modelo lógico para PowerDesigner</h1>
<p>
No modelo lógico, representar as três entidades:
</p>
<pre>
OPERACAO
│
│ 1
│
│
│ 1
▼
REGISTRO_CLEARING
│
│ 1
│
│ N
▼
EVENTO_REGISTRO
</pre>
<h2>OPERACAO</h2>
<pre>
idOperacao
produto
tipoOperacao
clearingDestino
chaveIdempotencia
versaoModelo
origem
dataHoraInclusao
dataHoraAlteracao
dadosOperacao
</pre>
<h2>REGISTRO_CLEARING</h2>
<pre>
idOperacao
idRegistro
clearing
status
idExterno
tentativa
versaoModelo
dataHoraInclusao
dataHoraAlteracao
dadosRegistro
</pre>
<h2>EVENTO_REGISTRO</h2>
<pre>
idOperacao
chaveEvento
idEvento
idRegistro
tipoEvento
statusAnterior
statusAtual
origemEvento
dataHoraEvento
versaoModelo
dadosEvento
</pre>
<h1>47. Modelo físico para PowerDesigner</h1>
<h2>OPERACOES</h2>
<pre>
TABLE: OPERACOES
PK:
idOperacao STRING
Attributes:
produto STRING
tipoOperacao STRING
clearingDestino STRING
chaveIdempotencia STRING
versaoModelo NUMBER
origem MAP
dataHoraInclusao STRING
dataHoraAlteracao STRING
dadosOperacao MAP
</pre>
<h2>REGISTROS_CLEARING</h2>
<pre>
TABLE: REGISTROS_CLEARING
PK:
idOperacao STRING
Attributes:
idRegistro STRING
clearing STRING
status STRING
idExterno STRING
tentativa NUMBER
versaoModelo NUMBER
dataHoraInclusao STRING
dataHoraAlteracao STRING
dadosRegistro MAP
</pre>
<h2>EVENTOS_REGISTRO</h2>
<pre>
TABLE: EVENTOS_REGISTRO
PK:
idOperacao STRING
SK:
chaveEvento STRING
Attributes:
idEvento STRING
idRegistro STRING
tipoEvento STRING
statusAnterior STRING
statusAtual STRING
origemEvento STRING
dataHoraEvento STRING
versaoModelo NUMBER
dadosEvento MAP
</pre>
<h1>48. Infraestrutura</h1>
<p>
A configuração definitiva deverá ser criada utilizando Terraform.
</p>
<p>O código deverá contemplar:</p>
<ul>
    <li>criação das três tabelas;</li>
    <li>Partition Keys;</li>
    <li>Sort Key de EVENTOS_REGISTRO;</li>
    <li>INDICE_ID_EXTERNO;</li>
    <li>INDICE_IDEMPOTENCIA, caso aprovado;</li>
    <li>modo de capacidade definido pelo projeto;</li>
    <li>DynamoDB Stream de OPERACOES;</li>
    <li>criptografia conforme padrão corporativo;</li>
    <li>tags corporativas;</li>
    <li>configurações de backup/PITR conforme padrão corporativo;</li>
    <li>IAM seguindo menor privilégio.</li>
</ul>
<h1>49. Requisitos</h1>
<ol>
<li>
<strong>RF01.</strong>
Criar a tabela <code>OPERACOES</code>.
</li>
<li>
<strong>RF02.</strong>
<code>OPERACOES</code> deverá utilizar
<code>idOperacao</code> como Partition Key.
</li>
<li>
<strong>RF03.</strong>
<code>OPERACOES</code> não deverá possuir Sort Key inicialmente.
</li>
<li>
<strong>RF04.</strong>
Criar a tabela <code>REGISTROS_CLEARING</code>.
</li>
<li>
<strong>RF05.</strong>
<code>REGISTROS_CLEARING</code> deverá utilizar
<code>idOperacao</code> como Partition Key.
</li>
<li>
<strong>RF06.</strong>
<code>REGISTROS_CLEARING</code> não deverá possuir Sort Key inicialmente.
</li>
<li>
<strong>RF07.</strong>
Criar a tabela <code>EVENTOS_REGISTRO</code>.
</li>
<li>
<strong>RF08.</strong>
<code>EVENTOS_REGISTRO</code> deverá utilizar
<code>idOperacao</code> como Partition Key.
</li>
<li>
<strong>RF09.</strong>
<code>EVENTOS_REGISTRO</code> deverá utilizar
<code>chaveEvento</code> como Sort Key.
</li>
<li>
<strong>RF10.</strong>
Criar <code>INDICE_ID_EXTERNO</code>.
</li>
<li>
<strong>RF11.</strong>
Avaliar a criação de <code>INDICE_IDEMPOTENCIA</code>.
</li>
<li>
<strong>RF12.</strong>
Cada tabela deverá possuir um envelope canônico estável.
</li>
<li>
<strong>RF13.</strong>
Dados específicos da operação deverão ser armazenados em
<code>dadosOperacao</code>.
</li>
<li>
<strong>RF14.</strong>
Dados específicos do processo de registro deverão ser armazenados em
<code>dadosRegistro</code>.
</li>
<li>
<strong>RF15.</strong>
Dados específicos dos eventos deverão ser armazenados em
<code>dadosEvento</code>.
</li>
<li>
<strong>RF16.</strong>
O modelo não deverá ser acoplado ao contrato CSV.
</li>
<li>
<strong>RF17.</strong>
O modelo não deverá ser acoplado ao contrato Kafka.
</li>
<li>
<strong>RF18.</strong>
O modelo não deverá ser acoplado ao contrato B3 ou de outra clearing.
</li>
<li>
<strong>RF19.</strong>
Cada item deverá possuir <code>versaoModelo</code>.
</li>
<li>
<strong>RF20.</strong>
Os contratos versionados deverão ser validados pela aplicação.
</li>
<li>
<strong>RF21.</strong>
<code>EVENTOS_REGISTRO</code> deverá ser append-only.
</li>
<li>
<strong>RF22.</strong>
Cada operação deverá possuir exatamente uma clearing de destino.
</li>
<li>
<strong>RF23.</strong>
Mudanças de estado e histórico deverão permanecer consistentes.
</li>
<li>
<strong>RF24.</strong>
Somente <code>OPERACOES</code> deverá iniciar o fluxo através
do DynamoDB Stream nesta etapa.
</li>
<li>
<strong>RF25.</strong>
A infraestrutura oficial deverá ser provisionada através de Terraform.
</li>
</ol>
<h1>50. Critérios de aceite</h1>
<ol>
<li>
<strong>CA01.</strong>
As três tabelas deverão ser criadas com as PK/SK especificadas.
</li>
<li>
<strong>CA02.</strong>
Uma operação deverá ser recuperável diretamente por
<code>idOperacao</code>.
</li>
<li>
<strong>CA03.</strong>
O estado atual do registro deverá ser recuperável diretamente
por <code>idOperacao</code>.
</li>
<li>
<strong>CA04.</strong>
O histórico deverá ser consultável por
<code>idOperacao</code>.
</li>
<li>
<strong>CA05.</strong>
Os eventos deverão ser retornados cronologicamente.
</li>
<li>
<strong>CA06.</strong>
O último evento deverá ser recuperável sem Scan completo.
</li>
<li>
<strong>CA07.</strong>
O retorno externo deverá localizar o registro através de
<code>idExterno</code>.
</li>
<li>
<strong>CA08.</strong>
RDB e CDB deverão coexistir sem alteração estrutural das tabelas.
</li>
<li>
<strong>CA09.</strong>
Um novo produto deverá poder introduzir novos atributos dentro
de <code>dadosOperacao</code>.
</li>
<li>
<strong>CA10.</strong>
Uma nova clearing deverá poder introduzir dados específicos
sem alterar desnecessariamente o envelope canônico.
</li>
<li>
<strong>CA11.</strong>
Eventos diferentes deverão suportar estruturas distintas
dentro de <code>dadosEvento</code>.
</li>
<li>
<strong>CA12.</strong>
Os adapters de entrada deverão produzir o mesmo modelo canônico,
independentemente da origem.
</li>
<li>
<strong>CA13.</strong>
O modelo deverá possuir versionamento explícito.
</li>
<li>
<strong>CA14.</strong>
O histórico deverá permanecer append-only.
</li>
<li>
<strong>CA15.</strong>
Snapshot e histórico deverão permanecer consistentes.
</li>
<li>
<strong>CA16.</strong>
A POC deverá validar <code>TransactWriteItems</code>.
</li>
<li>
<strong>CA17.</strong>
A POC deverá validar <code>INDICE_ID_EXTERNO</code>.
</li>
<li>
<strong>CA18.</strong>
A estratégia de idempotência deverá ser validada antes
da implementação definitiva.
</li>
<li>
<strong>CA19.</strong>
Somente inserções elegíveis em <code>OPERACOES</code> deverão
iniciar o fluxo de registro.
</li>
<li>
<strong>CA20.</strong>
A configuração definitiva deverá estar representada em Terraform.
</li>
</ol>
<h1>51. Resultado arquitetural esperado</h1>
<pre>
                   ENTRADAS
              CSV          Kafka
               │             │
               ▼             ▼
          CSV Adapter   Kafka Adapter
               │             │
               └──────┬──────┘
                      │
                      ▼
               MODELO CANÔNICO
                      │
                      ▼
                  OPERACOES
                      │
                      │ 1 : 1
                      ▼
             REGISTROS_CLEARING
                      │
                      │ 1 : N
                      ▼
              EVENTOS_REGISTRO
</pre>
<p>Estrutura dos dados:</p>
<pre>
OPERACOES
└── dadosOperacao { }
REGISTROS_CLEARING
└── dadosRegistro { }
EVENTOS_REGISTRO
└── dadosEvento { }
</pre>
<p>Na saída:</p>
<pre>
MODELO CANÔNICO
       │
       ▼
Clearing Router
       │
   ┌───┴──────────────────┐
   │                      │
   ▼                      ▼
Adapter B3         Adapter Clearing X
   │                      │
   ▼                      ▼
  B3                  Clearing X
</pre>
<div class="success">
<strong>Resultado esperado:</strong>

O DynamoDB permanecerá flexível fisicamente, enquanto o domínio
permanecerá controlado através de envelopes canônicos,
contratos versionados, adapters e validação na aplicação.

</div>
</body>
</html>


TÍTULO

Criar estrutura DynamoDB para armazenamento de operações, registros em clearing e histórico de eventos

DESCRIÇÃO

Esta história tem como objetivo criar a estrutura de persistência em DynamoDB utilizada pela plataforma de registro de operações em múltiplas clearings.

A solução deverá ser preparada para alta volumetria e evolução do negócio, considerando que inicialmente serão processadas operações de RDB e, posteriormente, outros produtos, como CDB e novos produtos que venham a ser registrados pela plataforma.

Embora a plataforma suporte múltiplas clearings, cada operação individual será destinada a apenas uma clearing.

A modelagem não deverá ser baseada no formato das fontes de entrada. Inicialmente teremos operações provenientes de arquivos CSV e futuramente operações provenientes de Kafka, porém CSV e Kafka são apenas contratos externos de entrada.

Cada origem deverá possuir seu próprio parser/adapter responsável por transformar os dados recebidos no modelo canônico definido pela plataforma.

Da mesma forma, a estrutura não deverá ser modelada de acordo com o contrato de uma clearing específica, como B3. A comunicação com cada clearing deverá ser responsabilidade dos respectivos adapters de saída.

A persistência deverá ser dividida em três tabelas:

OPERACOES

REGISTROS_CLEARING

EVENTOS_REGISTRO

A separação das tabelas tem como objetivo permitir que cada conjunto de informações possua estratégia própria de armazenamento, acesso, crescimento, índices e capacidade.

As tabelas possuem responsabilidades diferentes.

OPERACOES representa o fato financeiro recebido pela plataforma.

REGISTROS_CLEARING representa o estado operacional atual do processo de registro da operação na clearing.

EVENTOS_REGISTRO representa o histórico cronológico das mudanças ocorridas durante o processo de registro.

MODELO CANÔNICO

Apesar de o DynamoDB não possuir schema rígido para todos os atributos de um item, a aplicação deverá possuir um modelo canônico bem definido.

O fato de o DynamoDB ser schema-less não significa que os itens possam possuir estruturas arbitrárias sem controle.

Cada tabela deverá possuir um conjunto de atributos canônicos no primeiro nível do documento.

Devem permanecer no primeiro nível principalmente os atributos necessários para:

Identificação

Partition Key

Sort Key

Global Secondary Index

Idempotência

Correlação

Roteamento

Controle do workflow

Versionamento

Auditoria

Os dados específicos de cada produto, clearing ou evento deverão ser armazenados em objetos flexíveis.

Para OPERACOES será utilizado o objeto dadosOperacao.

Para REGISTROS_CLEARING será utilizado o objeto dadosRegistro.

Para EVENTOS_REGISTRO será utilizado o objeto dadosEvento.

Dessa forma, a inclusão de novos produtos ou novos campos não deverá exigir necessariamente alteração estrutural das tabelas.

TABELA OPERACOES

Finalidade:

Armazenar a representação canônica da operação financeira recebida pela plataforma.

Cada operação deverá possuir exatamente uma clearing de destino.

Partition Key:

idOperacao

Tipo:

String

Sort Key:

Não possui.

A ausência de Sort Key é intencional.

Existe somente um item representando cada operação financeira. Portanto, idOperacao identifica completamente o item e não existe atualmente um segundo padrão de ordenação que justifique uma Sort Key.

Adicionar uma Sort Key constante apenas aumentaria a complexidade sem fornecer benefício para os padrões de acesso conhecidos.

ATRIBUTOS PRINCIPAIS DE OPERACOES

idOperacao

Tipo: String

Finalidade: Partition Key e identificador interno único da operação.

Sugestão de geração: UUID/GUID.

produto

Tipo: String

Finalidade: identificar o produto financeiro da operação.

Exemplos:

RDB

CDB

tipoOperacao

Tipo: String

Finalidade: identificar a natureza da operação.

Exemplos:

APLICACAO

RESGATE

clearingDestino

Tipo: String

Finalidade: identificar para qual clearing a operação deverá ser enviada.

Cada operação possuirá somente uma clearing de destino.

chaveIdempotencia

Tipo: String

Finalidade: representar a identidade lógica da operação para auxiliar na prevenção de processamento duplicado.

Exemplo para arquivo:

ARQUIVO#20261006#000001

No futuro, para Kafka, a composição poderá utilizar informações próprias da mensagem, desde que a estratégia definida garanta uma identidade estável para a mesma operação.

versaoModelo

Tipo: Number

Finalidade: identificar a versão do contrato canônico utilizado pelo item.

Exemplo:

1

origem

Tipo: Map

Finalidade: identificar de onde a operação foi recebida.

Exemplo conceitual:

tipo = ARQUIVO

identificador = ARQ-000001

dataHoraInclusao

Tipo: String

Formato recomendado: ISO-8601.

Finalidade: auditoria da criação do item.

dataHoraAlteracao

Tipo: String

Formato recomendado: ISO-8601.

Finalidade: auditoria da última alteração.

dadosOperacao

Tipo: Map

Finalidade: armazenar os dados específicos da operação e do produto.

Exemplo para RDB:

valor = 15000.50

dataOperacao = 2026-10-06

codigoRdb = RDB001

dataVencimento = 2028-10-06

taxa = 0.125

Um futuro CDB poderá possuir atributos diferentes dentro de dadosOperacao sem exigir a criação de novas colunas obrigatórias para todos os demais produtos.

EXEMPLO CONCEITUAL DE UMA OPERAÇÃO

idOperacao = OP-000001

produto = RDB

tipoOperacao = APLICACAO

clearingDestino = B3

chaveIdempotencia = ARQUIVO#20261006#000001

versaoModelo = 1

origem.tipo = ARQUIVO

origem.identificador = ARQ-000001

dataHoraInclusao = 2026-10-06T10:00:00.000Z

dataHoraAlteracao = 2026-10-06T10:00:00.000Z

dadosOperacao.valor = 15000.50

dadosOperacao.dataOperacao = 2026-10-06

dadosOperacao.codigoRdb = RDB001

dadosOperacao.dataVencimento = 2028-10-06

dadosOperacao.taxa = 0.125

TABELA REGISTROS_CLEARING

Finalidade:

Armazenar o estado operacional atual do processo de registro da operação na clearing.

A relação esperada inicialmente será:

Uma operação possui um registro atual de clearing.

Partition Key:

idOperacao

Tipo:

String

Sort Key:

Não possui.

Como cada operação será direcionada para somente uma clearing, não existe necessidade atual de utilizar a clearing como Sort Key.

O idOperacao identifica diretamente o registro operacional atual.

ATRIBUTOS PRINCIPAIS DE REGISTROS_CLEARING

idOperacao

Tipo: String

Finalidade: Partition Key e correlação com a operação.

idRegistro

Tipo: String

Finalidade: identificador interno do processo de registro.

clearing

Tipo: String

Finalidade: clearing utilizada para o registro.

status

Tipo: String

Finalidade: representar o estado operacional atual.

Exemplos possíveis:

PENDENTE

EM_PROCESSAMENTO

ENVIADO

REGISTRADO

ERRO

idExterno

Tipo: String

Finalidade: armazenar o identificador utilizado/retornado pela clearing e permitir correlação quando o retorno assíncrono for recebido.

Esse campo deverá permanecer fora de dadosRegistro porque possui função de correlação e poderá participar de índice.

tentativa

Tipo: Number

Finalidade: controlar o número da tentativa atual de processamento.

versaoModelo

Tipo: Number

Finalidade: versionamento do contrato.

dataHoraInclusao

Tipo: String

Finalidade: auditoria.

dataHoraAlteracao

Tipo: String

Finalidade: auditoria.

dadosRegistro

Tipo: Map

Finalidade: armazenar informações específicas do processo de registro ou da clearing que não sejam necessárias para identificação, roteamento ou consulta direta.

Exemplos:

protocolo

codigoRetorno

mensagemRetorno

informações específicas de uma determinada clearing

EXEMPLO CONCEITUAL DE REGISTRO

idOperacao = OP-000001

idRegistro = REG-000001

clearing = B3

status = REGISTRADO

idExterno = B3-20261006-000001

tentativa = 1

versaoModelo = 1

dataHoraInclusao = 2026-10-06T10:00:10.000Z

dataHoraAlteracao = 2026-10-06T10:05:00.000Z

dadosRegistro.protocolo = PROTOCOLO-B3-001

dadosRegistro.codigoRetorno = 00

dadosRegistro.mensagemRetorno = Registro realizado com sucesso

ÍNDICE PARA IDENTIFICADOR EXTERNO

Deverá ser criado um Global Secondary Index na tabela REGISTROS_CLEARING para permitir localizar uma operação através do identificador externo utilizado pela clearing.

Nome sugerido:

INDICE_ID_EXTERNO

Partition Key do índice:

idExterno

Esse índice será importante principalmente no fluxo de retorno.

Exemplo:

A plataforma envia OP-000001 para B3.

O processo recebe ou associa o identificador B3-20261006-000001.

Posteriormente é recebido um retorno contendo B3-20261006-000001.

A aplicação consulta INDICE_ID_EXTERNO.

O índice retorna o registro correspondente.

A aplicação identifica idOperacao = OP-000001.

A partir desse momento o retorno pode ser correlacionado com a operação interna.

TABELA EVENTOS_REGISTRO

Finalidade:

Armazenar o histórico cronológico do processo de registro.

Diferentemente de REGISTROS_CLEARING, que representa o estado atual, EVENTOS_REGISTRO deverá manter a timeline de como o registro chegou ao estado atual.

A tabela deverá seguir o conceito append-only.

Eventos históricos não deverão ser atualizados para representar novos estados.

Uma mudança deverá produzir um novo evento.

Partition Key:

idOperacao

Tipo:

String

Sort Key:

chaveEvento

Tipo:

String

COMPOSIÇÃO DA SORT KEY

A chaveEvento deverá possuir uma composição que permita ordenação cronológica e unicidade.

Formato sugerido:

dataHoraEvento#idEvento

Exemplo:

2026-10-06T10:00:10.000Z#EVT-001

2026-10-06T10:02:00.000Z#EVT-002

2026-10-06T10:05:00.000Z#EVT-003

Dessa forma, uma Query utilizando idOperacao retornará naturalmente a timeline da operação ordenada pela Sort Key.

O idEvento no final da chave evita colisões caso mais de um evento possua o mesmo timestamp.

ATRIBUTOS PRINCIPAIS DE EVENTOS_REGISTRO

idOperacao

Tipo: String

Finalidade: Partition Key.

chaveEvento

Tipo: String

Finalidade: Sort Key responsável pela ordenação cronológica.

idEvento

Tipo: String

Finalidade: identificador único do evento.

idRegistro

Tipo: String

Finalidade: correlação com o registro de clearing.

tipoEvento

Tipo: String

Finalidade: identificar o que ocorreu.

Exemplos:

REGISTRO_CRIADO

ENVIO_INICIADO

ENVIADO_CLEARING

STATUS_ALTERADO

ERRO_REGISTRO

RETORNO_RECEBIDO

REPROCESSAMENTO_INICIADO

statusAnterior

Tipo: String

Obrigatório somente quando aplicável.

Finalidade: estado anterior do processo.

statusAtual

Tipo: String

Obrigatório somente quando aplicável.

Finalidade: novo estado do processo.

origemEvento

Tipo: String

Finalidade: identificar qual componente originou o evento.

Exemplos:

INGESTAO

REGISTRATION_WORKER

ADAPTER_B3

RETORNO_CLEARING

REPROCESSAMENTO

dataHoraEvento

Tipo: String

Formato recomendado: ISO-8601.

Finalidade: data/hora em que o evento ocorreu.

versaoModelo

Tipo: Number

Finalidade: versionamento do contrato.

dadosEvento

Tipo: Map

Finalidade: armazenar informações específicas do evento.

Exemplo para erro:

codigoErro = B3-001

descricao = Instrumento não encontrado

tentativa = 2

reprocessavel = true

EXEMPLO DE TIMELINE

Para idOperacao OP-000001 poderão existir:

2026-10-06T10:00:10.000Z#EVT-001

tipoEvento = REGISTRO_CRIADO

statusAtual = PENDENTE

2026-10-06T10:01:00.000Z#EVT-002

tipoEvento = STATUS_ALTERADO

statusAnterior = PENDENTE

statusAtual = EM_PROCESSAMENTO

2026-10-06T10:02:00.000Z#EVT-003

tipoEvento = STATUS_ALTERADO

statusAnterior = EM_PROCESSAMENTO

statusAtual = ENVIADO

2026-10-06T10:05:00.000Z#EVT-004

tipoEvento = STATUS_ALTERADO

statusAnterior = ENVIADO

statusAtual = REGISTRADO

RESUMO DAS CHAVES

Tabela OPERACOES

Partition Key: idOperacao

Sort Key: não possui

Tabela REGISTROS_CLEARING

Partition Key: idOperacao

Sort Key: não possui

Tabela EVENTOS_REGISTRO

Partition Key: idOperacao

Sort Key: chaveEvento

GLOBAL SECONDARY INDEXES

REGISTROS_CLEARING deverá possuir inicialmente:

INDICE_ID_EXTERNO

Partition Key: idExterno

Finalidade: correlação dos retornos recebidos das clearings.

Em OPERACOES deverá ser avaliada a criação de:

INDICE_IDEMPOTENCIA

Partition Key: chaveIdempotencia

Finalidade: permitir localizar uma operação através da chave de idempotência.

Importante:

O GSI de idempotência não deverá ser considerado isoladamente uma garantia de unicidade.

O padrão:

Consultar GSI

Não encontrou

Inserir operação

não é suficiente para garantir idempotência em situações concorrentes.

Dois processamentos podem consultar simultaneamente, ambos não encontrarem a operação e ambos tentarem inserir.

A implementação da idempotência deverá utilizar estratégia segura para concorrência, utilizando escrita condicional e/ou mecanismo específico de controle de idempotência.

A estratégia definitiva deverá ser validada durante a POC.

CONSISTÊNCIA ENTRE OPERACOES E REGISTROS_CLEARING

A criação inicial da operação deverá considerar a necessidade de consistência entre OPERACOES e REGISTROS_CLEARING.

O estado indesejado é:

OPERACOES contém OP-000001.

REGISTROS_CLEARING não contém OP-000001.

Quando os dois itens fizerem parte da mesma criação lógica, deverá ser utilizado TransactWriteItems ou mecanismo equivalente aprovado.

O comportamento esperado é:

Ou a operação e o registro são criados.

Ou nenhum dos dois é criado.

CONSISTÊNCIA ENTRE REGISTROS_CLEARING E EVENTOS_REGISTRO

Quando o status de um registro for alterado, a atualização do snapshot e a criação do evento correspondente deverão permanecer consistentes.

Exemplo:

Estado atual:

ENVIADO

Novo estado:

REGISTRADO

A operação deverá realizar conceitualmente:

Atualização de REGISTROS_CLEARING para REGISTRADO.

Inclusão de um novo EVENTOS_REGISTRO contendo ENVIADO -> REGISTRADO.

Deverá ser utilizado TransactWriteItems ou mecanismo equivalente quando a mudança exigir atomicidade entre as duas tabelas.

Não deverá ocorrer como estado final:

REGISTROS_CLEARING = REGISTRADO

sem existir o evento correspondente.

Da mesma forma, não deverá existir um evento informando REGISTRADO enquanto o snapshot permaneça ENVIADO.

DYNAMODB STREAM

Nesta etapa, somente a tabela OPERACOES deverá possuir o DynamoDB Stream necessário para iniciar posteriormente o fluxo assíncrono de registro.

Fluxo previsto:

OPERACOES

DynamoDB Stream

EventBridge Pipes

SQS de registro

Worker de registro

Adapter da clearing

Clearing

As alterações realizadas em REGISTROS_CLEARING e EVENTOS_REGISTRO não deverão provocar automaticamente um novo envio para clearing.

PADRÕES DE CONSULTA DA POC

Consulta 1: buscar uma operação.

Tabela:

OPERACOES

Operação:

GetItem

Chave:

idOperacao = OP-000001

Consulta 2: buscar o estado atual do registro.

Tabela:

REGISTROS_CLEARING

Operação:

GetItem

Chave:

idOperacao = OP-000001

Consulta 3: buscar todo o histórico de uma operação.

Tabela:

EVENTOS_REGISTRO

Operação:

Query

Condição:

idOperacao = OP-000001

O resultado deverá retornar todos os eventos da operação ordenados por chaveEvento.

Consulta 4: buscar o último evento.

Tabela:

EVENTOS_REGISTRO

Operação:

Query

Condição:

idOperacao = OP-000001

Ordenação:

decrescente

Limite:

1

Isso permitirá localizar o evento mais recente sem executar Scan completo.

Consulta 5: localizar uma operação através do identificador externo.

Tabela:

REGISTROS_CLEARING

Índice:

INDICE_ID_EXTERNO

Condição:

idExterno = B3-20261006-000001

CONSULTAS ANALÍTICAS E TELAS

A modelagem desta história deverá priorizar o fluxo transacional de registro.

Não deverão ser criados GSIs indiscriminadamente apenas para atender consultas como:

Listar todas as operações.

Listar operações por produto.

Listar operações por clearing.

Listar todas as operações registradas.

Listar operações com erro.

Contar operações em processamento.

Contar operações registradas.

Contar operações com erro.

Montar dashboards históricos.

Esses padrões de consulta possuem características analíticas diferentes das consultas transacionais.

Caso essas necessidades sejam confirmadas, deverá ser avaliada posteriormente a criação de um Read Model específico.

Uma possível evolução futura poderá utilizar exportação/replicação dos dados para S3 e consultas através do Athena ou outra solução apropriada.

Essa decisão não faz parte do escopo desta história.

VOLUMETRIA E PARTICIONAMENTO

A solução deverá considerar alta volumetria.

A tabela OPERACOES possuirá aproximadamente um item por operação.

REGISTROS_CLEARING possuirá aproximadamente um registro atual por operação.

EVENTOS_REGISTRO possuirá múltiplos itens por operação e deverá apresentar crescimento significativamente superior às demais tabelas.

Exemplo apenas ilustrativo:

20 milhões de operações.

Média de 5 eventos por operação.

EVENTOS_REGISTRO poderá possuir aproximadamente 100 milhões de itens.

Por esse motivo, a tabela histórica foi separada das tabelas de operação e snapshot operacional.

As Partition Keys não deverão utilizar valores de baixa cardinalidade, como:

RDB

CDB

B3

REGISTRADO

ERRO

Esses valores poderiam concentrar grande quantidade de dados na mesma chave lógica.

idOperacao será utilizado como Partition Key principal por possuir alta cardinalidade.

JUSTIFICATIVA DAS TRÊS TABELAS

OPERACOES responde:

O que é essa operação financeira?

REGISTROS_CLEARING responde:

Qual é o estado atual do registro dessa operação?

EVENTOS_REGISTRO responde:

Como o processo chegou ao estado atual?

Portanto:

OPERACOES representa o fato de negócio.

REGISTROS_CLEARING representa o snapshot operacional.

EVENTOS_REGISTRO representa a timeline/auditoria.

FLEXIBILIDADE PARA NOVOS PRODUTOS

Inicialmente será utilizado RDB.

Posteriormente será incluído CDB.

Novos produtos poderão surgir.

A inclusão de um novo produto não deverá exigir a criação de uma nova tabela.

Os atributos comuns e necessários ao funcionamento da plataforma permanecerão no envelope canônico.

Os atributos específicos do produto serão armazenados em dadosOperacao.

Exemplo:

RDB poderá possuir codigoRdb e taxa.

CDB poderá possuir codigoCdb, indexador e percentualIndexador.

Um produto futuro poderá possuir outro conjunto de propriedades.

A aplicação deverá validar dadosOperacao conforme produto e versaoModelo.

FLEXIBILIDADE PARA NOVAS CLEARINGS

A plataforma deverá suportar múltiplas clearings.

Cada operação, entretanto, será destinada a somente uma clearing.

Os atributos necessários ao funcionamento comum permanecerão no modelo canônico.

Informações particulares do processo de uma determinada clearing poderão ser armazenadas em dadosRegistro ou dadosEvento, quando não forem necessárias para chave, índice, roteamento ou correlação.

A estrutura DynamoDB não deverá ser remodelada para refletir diretamente cada novo contrato externo.

MODELO PARA POWERDESIGNER

O modelo lógico deverá representar:

OPERACAO

Relacionamento 1 para 1 com REGISTRO_CLEARING.

REGISTRO_CLEARING

Relacionamento 1 para N com EVENTO_REGISTRO.

Entidade OPERACAO:

idOperacao

produto

tipoOperacao

clearingDestino

chaveIdempotencia

versaoModelo

origem

dataHoraInclusao

dataHoraAlteracao

dadosOperacao

Entidade REGISTRO_CLEARING:

idOperacao

idRegistro

clearing

status

idExterno

tentativa

versaoModelo

dataHoraInclusao

dataHoraAlteracao

dadosRegistro

Entidade EVENTO_REGISTRO:

idOperacao

chaveEvento

idEvento

idRegistro

tipoEvento

statusAnterior

statusAtual

origemEvento

dataHoraEvento

versaoModelo

dadosEvento

No modelo físico deverá ficar explícito:

OPERACOES

PK = idOperacao String

Sem SK.

REGISTROS_CLEARING

PK = idOperacao String

Sem SK.

GSI INDICE_ID_EXTERNO:

PK = idExterno String.

EVENTOS_REGISTRO

PK = idOperacao String.

SK = chaveEvento String.

POC

Antes da implementação definitiva via Terraform, deverá ser realizada uma POC da estrutura.

Para a POC poderão ser criadas manualmente no AWS Console as três tabelas.

Configuração sugerida:

OPERACOES

Partition Key = idOperacao

Tipo = String

Sem Sort Key

Capacity Mode = On-demand

REGISTROS_CLEARING

Partition Key = idOperacao

Tipo = String

Sem Sort Key

Capacity Mode = On-demand

GSI = INDICE_ID_EXTERNO

GSI Partition Key = idExterno

EVENTOS_REGISTRO

Partition Key = idOperacao

Tipo = String

Sort Key = chaveEvento

Tipo da Sort Key = String

Capacity Mode = On-demand

A POC deverá possuir massa suficiente para demonstrar:

Uma aplicação RDB registrada com sucesso.

Um resgate RDB em processamento.

Uma operação com erro.

Operações destinadas à clearing configurada.

Histórico com múltiplas mudanças de status.

Correlação utilizando idExterno.

Consulta da timeline completa.

Consulta do último evento.

Validação de TransactWriteItems.

Teste da estratégia de idempotência.

Inclusão de dados específicos em dadosOperacao, dadosRegistro e dadosEvento.

INFRAESTRUTURA DEFINITIVA

Após a validação da POC e aprovação do modelo, a infraestrutura definitiva deverá ser provisionada através de Terraform.

O Terraform deverá contemplar:

Criação das três tabelas.

Partition Keys.

Sort Key de EVENTOS_REGISTRO.

INDICE_ID_EXTERNO.

INDICE_IDEMPOTENCIA caso seja aprovado após a POC.

DynamoDB Stream de OPERACOES quando aplicável ao fluxo.

Capacity Mode definido pelo projeto.

Criptografia seguindo padrão corporativo.

Tags corporativas.

Backup/PITR conforme padrão corporativo.

IAM seguindo princípio de menor privilégio.

CRITÉRIOS DE ACEITE

CA01 - Devem existir três tabelas distintas: OPERACOES, REGISTROS_CLEARING e EVENTOS_REGISTRO.

CA02 - OPERACOES deve utilizar idOperacao do tipo String como Partition Key e não possuir Sort Key.

CA03 - REGISTROS_CLEARING deve utilizar idOperacao do tipo String como Partition Key e não possuir Sort Key.

CA04 - EVENTOS_REGISTRO deve utilizar idOperacao do tipo String como Partition Key e chaveEvento do tipo String como Sort Key.

CA05 - Deve ser possível recuperar uma operação diretamente através de idOperacao sem executar Scan.

CA06 - Deve ser possível recuperar o estado atual do registro diretamente através de idOperacao sem executar Scan.

CA07 - Deve ser possível recuperar todo o histórico de uma operação através de Query utilizando idOperacao.

CA08 - Os eventos de uma operação devem ser retornados em ordem cronológica através da Sort Key chaveEvento.

CA09 - Deve ser possível recuperar somente o último evento de uma operação utilizando Query com ordenação decrescente e limite igual a 1.

CA10 - REGISTROS_CLEARING deve possuir INDICE_ID_EXTERNO utilizando idExterno como Partition Key.

CA11 - Deve ser possível localizar o registro interno correspondente através de idExterno.

CA12 - OPERACOES deve possuir chaveIdempotencia e a estratégia definitiva de idempotência deve ser validada na POC.

CA13 - A solução de idempotência deve impedir duplicidade mesmo em cenário concorrente.

CA14 - O GSI de idempotência, caso utilizado, não poderá ser considerado isoladamente como garantia de unicidade.

CA15 - A criação lógica de uma nova operação e seu registro inicial não poderá produzir estado parcial quando a regra exigir a existência de ambos.

CA16 - Deve ser validado o uso de TransactWriteItems para operações que necessitem atomicidade entre tabelas.

CA17 - Uma alteração de status deverá atualizar REGISTROS_CLEARING e produzir o respectivo EVENTOS_REGISTRO de maneira consistente.

CA18 - EVENTOS_REGISTRO deverá seguir o conceito append-only.

CA19 - Eventos históricos existentes não deverão ser alterados para representar novos estados.

CA20 - Cada novo estado relevante deverá produzir um novo evento histórico.

CA21 - Deve existir versionamento explícito através de versaoModelo.

CA22 - Os dados específicos de produtos deverão ser armazenados em dadosOperacao.

CA23 - Os dados específicos do processo de registro deverão ser armazenados em dadosRegistro quando não precisarem participar de chave, índice, roteamento ou correlação.

CA24 - Os dados específicos dos eventos deverão ser armazenados em dadosEvento.

CA25 - Deve ser possível armazenar operações RDB e CDB sem criar tabelas diferentes por produto.

CA26 - A inclusão de um novo produto não deverá exigir alteração estrutural das tabelas quando os novos dados forem específicos do produto.

CA27 - A inclusão de uma nova clearing não deverá exigir remodelagem do domínio para reproduzir o contrato externo dessa clearing.

CA28 - Cada operação deverá possuir somente uma clearing de destino.

CA29 - O modelo DynamoDB não deverá depender do layout do CSV.

CA30 - O modelo DynamoDB não deverá depender do contrato Kafka.

CA31 - CSV, Kafka e futuras origens deverão ser adaptados para o mesmo modelo canônico antes da persistência.

CA32 - O modelo DynamoDB não deverá reproduzir diretamente o contrato da B3 ou de qualquer outra clearing.

CA33 - Deve ser possível rastrear a origem de uma operação.

CA34 - A estrutura deverá permitir futura implementação de reprocessamento sem alterar eventos históricos.

CA35 - A estrutura deverá permitir futura implementação de conciliação.

CA36 - A POC deverá demonstrar consultas de operação, registro atual, histórico completo, último evento e correlação por idExterno.

CA37 - A POC deverá demonstrar a flexibilidade dos Maps dadosOperacao, dadosRegistro e dadosEvento.

CA38 - Não deverão ser adicionados GSIs exclusivamente para consultas analíticas sem que exista um padrão de acesso transacional justificado.

CA39 - Consultas analíticas e dashboards deverão ser avaliados posteriormente através de Read Model específico.

CA40 - A infraestrutura definitiva deverá ser criada e versionada através de Terraform.

CA41 - O modelo lógico e físico deverá ser documentado no PowerDesigner conforme normativa da empresa.

CA42 - A configuração deverá seguir os padrões corporativos de segurança, criptografia, backup, tags e IAM.

CA43 - A estrutura deverá estar preparada para a volumetria prevista sem utilizar produto, clearing ou status como Partition Key principal das tabelas transacionais.

CA44 - A POC deverá ser aprovada antes da criação da infraestrutura definitiva.

CA45 - Após a POC, as decisões sobre idempotência, índices e capacidade deverão ser registradas antes da aprovação do modelo definitivo.

RESULTADO ESPERADO

Ao término desta história, deverá existir uma estrutura DynamoDB validada através de POC e documentada no PowerDesigner, composta pelas tabelas OPERACOES, REGISTROS_CLEARING e EVENTOS_REGISTRO.

A estrutura deverá separar claramente o fato financeiro, o estado operacional atual e o histórico do processo de registro.

O modelo deverá permanecer independente das origens de entrada e das APIs das clearings.

A flexibilidade física do DynamoDB deverá ser utilizada através de dadosOperacao, dadosRegistro e dadosEvento, mantendo ao mesmo tempo um envelope canônico controlado e versionado pela plataforma.

A estrutura resultante deverá permitir a evolução da solução para novos produtos, novas clearings, Kafka, reprocessamento, conciliação e futuros modelos de leitura sem exigir remodelagem completa da persistência transacional.
