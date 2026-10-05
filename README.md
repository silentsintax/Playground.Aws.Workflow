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