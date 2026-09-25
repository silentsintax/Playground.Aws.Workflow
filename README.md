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

Criar estrutura de persistência DynamoDB para operações e registros em clearing via Terraform

# Objetivo

Provisionar, através de Terraform, as tabelas Amazon DynamoDB necessárias para persistir as operações recebidas pelo sistema, as informações de registro nas diferentes clearings e o histórico de alterações de status.

A infraestrutura deverá suportar o cenário de **multi-clearing**, permitindo que uma operação seja associada à clearing responsável pelo registro e que seu ciclo de vida possa ser rastreado.

Esta história contempla exclusivamente a criação e configuração das estruturas DynamoDB e seus recursos de infraestrutura.

DynamoDB Streams, processamento de eventos, Lambdas, SQS e integrações com clearings não fazem parte desta história.

# Descrição detalhada

O sistema de registro em clearing necessita de uma estrutura de persistência capaz de armazenar as operações recebidas através das diferentes fontes de entrada e acompanhar posteriormente o processo de registro dessas operações nas clearings.

A infraestrutura deverá ser criada utilizando **Terraform**, seguindo os padrões de infraestrutura como código adotados pelo projeto.

Inicialmente deverão ser contempladas três estruturas lógicas:

```text id="9dkt97"
Operations

Registration

Status History
```

A estrutura deverá ser preparada para suportar múltiplas clearings, não existindo dependência fixa da B3 no modelo de dados.

## Operations

Responsável por armazenar os dados da operação recebida pelo sistema.

Modelo lógico inicial:

```text id="fjwrr4"
PK = OPERATION#<OperationId>
SK = METADATA
```

Exemplo:

```text id="ntq5t2"
PK = OPERATION#123456
SK = METADATA

OperationId
OperationType
Clearing
Amount
OperationDate

SourceType
SourceEventId
ImportId
FileName

Status

CreatedAt
UpdatedAt
```

O atributo `Clearing` deverá identificar a clearing de destino da operação.

Exemplos:

```text id="jy78i5"
B3
CLEARING_X
CLEARING_Y
```

Nenhuma regra da estrutura deverá assumir B3 como única clearing possível.

`SourceType` deverá permitir identificar a origem da operação, inicialmente:

```text id="lmt9xr"
FILE
```

e futuramente:

```text id="w8okg0"
KAFKA
```

A estrutura deverá permitir que novas origens sejam adicionadas sem alteração da chave primária.

---

## Registration

Responsável por representar o **estado atual do registro de uma operação em determinada clearing**.

Modelo lógico inicial:

```text id="9yq67h"
PK = OPERATION#<OperationId>

SK = REGISTRATION#<Clearing>
```

Exemplo:

```text id="4csn9q"
PK = OPERATION#123456
SK = REGISTRATION#B3

OperationId
Clearing

Status
Attempt

ExternalProtocol
RequestHash

LastError

SentAt
ResponseAt
CreatedAt
UpdatedAt
```

O modelo deverá permitir que uma mesma operação possua registros associados a diferentes clearings caso essa necessidade exista futuramente.

Exemplo:

```text id="5x1vd5"
OPERATION#123456
    │
    ├── METADATA
    │
    ├── REGISTRATION#B3
    │
    └── REGISTRATION#CLEARING_X
```

---

## Status History

Responsável por manter o histórico das mudanças de status do registro.

Os registros de histórico deverão ser tratados como **append-only**, evitando sobrescrever o histórico anterior.

Modelo lógico inicial:

```text id="2tdrjj"
PK = OPERATION#<OperationId>

SK =
REGISTRATION#<Clearing>
#STATUS#<Timestamp>
#<EventId>
```

Exemplo:

```text id="v6rg9c"
PK = OPERATION#123456

SK =
REGISTRATION#B3
#STATUS#2026-09-25T15:30:00
#EVENT#ABC123
```

A estrutura deverá permitir armazenar:

```text id="wivpm5"
PreviousStatus
Status

Clearing

Reason
Attempt

ExternalProtocol
EventId

Source

CreatedAt
```

O objetivo é permitir posteriormente reconstruir o ciclo de vida de uma operação.

Exemplo:

```text id="w1c7ze"
RECEIVED
   ↓
PENDING
   ↓
SENT
   ↓
REJECTED
   ↓
RETRYING
   ↓
SENT
   ↓
ACCEPTED
```

A implementação desta história não deverá implementar as regras responsáveis por realizar essas transições.

---

# Requisitos

**RF01 — Infraestrutura como código**

Todos os recursos DynamoDB deverão ser provisionados exclusivamente através de Terraform.

Não deverão ser criados ou configurados recursos manualmente através do AWS Console.

---

**RF02 — Ambientes**

A configuração Terraform deverá permitir o provisionamento das estruturas nos ambientes utilizados pelo projeto, seguindo a estratégia existente de configuração por ambiente.

Os nomes físicos dos recursos deverão seguir o padrão corporativo vigente.

---

**RF03 — Estrutura de Operations**

Deverá existir estrutura DynamoDB capaz de armazenar uma operação utilizando:

```text id="o20lhc"
PK = OPERATION#<OperationId>
SK = METADATA
```

---

**RF04 — Estrutura de Registration**

Deverá existir estrutura capaz de armazenar o estado atual do registro utilizando:

```text id="tyq15v"
PK = OPERATION#<OperationId>
SK = REGISTRATION#<Clearing>
```

---

**RF05 — Estrutura de Status History**

Deverá existir estrutura capaz de armazenar eventos de histórico utilizando:

```text id="dklh8g"
PK = OPERATION#<OperationId>

SK =
REGISTRATION#<Clearing>
#STATUS#<Timestamp>
#<EventId>
```

A composição da chave deverá impedir que dois eventos distintos ocorridos no mesmo instante sobrescrevam um ao outro.

---

**RF06 — Multi-clearing**

O modelo não deverá possuir dependência estrutural específica da B3.

A clearing deverá fazer parte dos dados/chaves necessárias para identificar o registro.

---

**RF07 — Capacidade**

O modo de capacidade do DynamoDB deverá ser parametrizável através do Terraform conforme o padrão definido para o projeto.

Caso seja utilizado `PAY_PER_REQUEST`, essa configuração deverá estar explícita no módulo.

Caso seja utilizado provisioned capacity, os valores e eventual autoscaling deverão ser definidos através da infraestrutura como código.

---

**RF08 — Criptografia**

As tabelas deverão possuir criptografia em repouso habilitada conforme o padrão de segurança da organização.

Caso exista uma KMS Key corporativa destinada ao projeto, sua referência deverá ser parametrizada pelo Terraform.

---

**RF09 — Point-in-Time Recovery**

Point-in-Time Recovery deverá ser configurado conforme o padrão de backup e recuperação definido pela organização.

A configuração deverá ser realizada através do Terraform.

---

**RF10 — Tags**

Todos os recursos deverão receber as tags corporativas obrigatórias.

Exemplos, conforme padrão existente:

```text id="m1f4qf"
Environment
Application
Team
CostCenter
ManagedBy = Terraform
```

Os nomes e valores definitivos deverão utilizar o padrão corporativo vigente.

---

**RF11 — Outputs**

O módulo Terraform deverá disponibilizar os outputs necessários para consumo pelos demais componentes da solução.

Exemplos:

```text id="s0rw36"
TableName
TableArn
```

---

**RF12 — Permissões**

A história não contempla a criação de permissões de aplicação além das necessárias ao provisionamento, salvo quando exigido pelo módulo/padrão Terraform existente.

As policies de acesso dos futuros Batch Jobs, Lambdas e demais consumidores deverão ser tratadas nas histórias correspondentes.

---

**RF13 — DynamoDB Streams**

DynamoDB Streams deverá permanecer **desabilitado** nesta entrega.

Não deverão ser criados:

```text id="sp3xk1"
Stream
Event Source Mapping
Lambda de Stream
EventBridge Pipe
```

Esses recursos serão tratados posteriormente.

---

# Critérios de aceite

**CA01**

Dado o código Terraform da solução,

quando `terraform plan` for executado,

então os recursos DynamoDB esperados deverão ser apresentados sem alterações manuais adicionais.

---

**CA02**

Dado um ambiente sem os recursos,

quando o Terraform aprovado for aplicado,

então as estruturas DynamoDB deverão ser criadas com sucesso.

---

**CA03**

Dada uma operação,

deverá ser possível representá-la utilizando:

```text id="whl08w"
PK = OPERATION#<OperationId>
SK = METADATA
```

---

**CA04**

Dada uma operação registrada em determinada clearing,

deverá ser possível representar seu estado atual utilizando:

```text id="uovl19"
PK = OPERATION#<OperationId>
SK = REGISTRATION#<Clearing>
```

---

**CA05**

Dada uma mudança de status,

deverá ser possível armazenar múltiplos eventos de histórico para a mesma operação e clearing sem sobrescrever eventos anteriores.

---

**CA06**

Dada uma mesma operação,

o modelo deverá permitir representar registros de diferentes clearings sem alteração da estrutura da tabela.

---

**CA07**

As configurações de capacidade, criptografia, recuperação e tags deverão estar declaradas no Terraform e seguir os padrões definidos para o projeto.

---

**CA08**

Os outputs necessários para identificação dos recursos, incluindo nome e ARN, deverão estar disponíveis após a aplicação do Terraform.

---

**CA09**

Após o provisionamento, nenhuma configuração manual no AWS Console deverá ser necessária para completar a criação das estruturas DynamoDB.

---

**CA10**

Após a aplicação do Terraform, **DynamoDB Streams deverá permanecer desabilitado**.

Nenhuma Lambda, Event Source Mapping, EventBridge Pipe, SQS ou integração com clearing deverá ser criada por esta história.

---

**CA11**

A execução subsequente de `terraform plan`, sem alteração do código ou das variáveis de infraestrutura, não deverá indicar mudanças inesperadas nos recursos provisionados.

# Fora do escopo

Esta história não contempla:

```text id="w9h0x6"
Leitura do CSV
AWS Batch

DynamoDB Streams
EventBridge Pipes

SQS
Lambda

Envio para B3
Retorno Pismo

Regras de transição de status
Processamento de Registration
Processamento de Status History
```

O objetivo desta entrega é exclusivamente disponibilizar, através de Terraform, a infraestrutura DynamoDB necessária para que as próximas etapas da solução possam utilizar o modelo de persistência definido.