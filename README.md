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
```