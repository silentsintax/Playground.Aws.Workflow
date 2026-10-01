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

Criar estrutura DynamoDB e modelagem PowerDesigner para operações multi-produto e multi-clearing

# Objetivo

Criar, através de **Terraform**, a estrutura DynamoDB responsável pela persistência das operações processadas pelo sistema de Clearing, incluindo sua respectiva modelagem lógica e física no **PowerDesigner**, conforme normativa corporativa.

A solução deverá suportar duas dimensões independentes de evolução:

- **Produtos:** inicialmente RDB, posteriormente CDB e futuros produtos;
- **Clearings:** inicialmente B3, podendo ser adicionadas novas clearings futuramente.

Por normativa da empresa, o nome da tabela, atributos, índices, entidades e demais elementos pertencentes à aplicação deverão utilizar nomenclatura em **português**.

A estrutura deverá possuir um **modelo canônico próprio**, independente:

- do formato do CSV;
- do contrato Kafka;
- do produto específico;
- da B3;
- da Pismo;
- de futuras fontes;
- de futuras clearings.

---

# Descrição detalhada

## 1. Princípio arquitetural

O sistema de Clearing deverá possuir um modelo de dados próprio.

As fontes externas deverão ser adaptadas para esse modelo antes da persistência.

Fluxo conceitual:

```text
CSV
 │
 ▼
Adaptador CSV
 │
 └──────────────┐
                │
                ▼
          MODELO CANÔNICO
                ▲
                │
 ┌──────────────┘
 │
Adaptador Kafka
 ▲
 │
Kafka
```

Dessa forma, alterações no CSV ou Kafka não deverão determinar diretamente alterações no modelo de persistência.

Exemplo:

```text
CSV

CD_OPERACAO
TP_MOV
VL_FINANCEIRO
```

deverá ser transformado pelo adaptador para conceitos pertencentes ao domínio:

```text
idOperacaoOrigem
tipoOperacao
valor
```

---

# 2. Flexibilidade por produto e clearing

A modelagem deverá considerar **produto e clearing como dimensões independentes**.

Produto caracteriza a operação:

```text
OPERACAO

produto = RDB
produto = CDB
produto = ...
```

Clearing caracteriza o processo de registro:

```text
REGISTRO_CLEARING

clearing = B3
clearing = CLEARING_X
clearing = ...
```

Não deverão ser criadas estruturas combinatórias como:

```text
OPERACAO_RDB_B3
OPERACAO_CDB_B3
OPERACAO_RDB_CLEARING_X
OPERACAO_CDB_CLEARING_X
```

O relacionamento conceitual deverá ser:

```text
                    OPERACAO
                       │
                 produto = RDB
                       │
              dadosProduto {...}
                       │
                       │
             ┌─────────┴─────────┐
             │                   │
             ▼                   ▼
       REGISTRO_CLEARING   REGISTRO_CLEARING
         clearing=B3        clearing=X
             │                   │
       dadosClearing        dadosClearing
             │                   │
             ▼                   ▼
       HISTORICO_STATUS     HISTORICO_STATUS
```

Isso deverá permitir que novos produtos e novas clearings sejam incorporados sem alteração da estrutura principal de chaves.

---

# 3. Modelo lógico — PowerDesigner

O modelo lógico deverá representar os conceitos de negócio, independentemente da estratégia física de armazenamento utilizada pelo DynamoDB.

Deverão existir três entidades lógicas principais:

```text
┌─────────────────────────────┐
│          OPERACAO           │
├─────────────────────────────┤
│ # idOperacao                │
│   tipoOperacao              │
│   produto                   │
│   valor                     │
│   dataOperacao              │
│   tipoOrigem                │
│   idOperacaoOrigem          │
│   idEventoOrigem            │
│   chaveIdempotencia         │
│   dadosProduto              │
│   dataHoraCriacao           │
│   dataHoraAtualizacao       │
└──────────────┬──────────────┘
               │
             1 │
               │
             N │
               ▼
┌─────────────────────────────┐
│      REGISTRO_CLEARING      │
├─────────────────────────────┤
│ # idRegistro                │
│   idOperacao                │
│   clearing                  │
│   idExterno                 │
│   protocoloExterno          │
│   status                    │
│   tentativa                 │
│   codigoUltimoErro          │
│   descricaoUltimoErro       │
│   dadosClearing             │
│   dataHoraEnvio             │
│   dataHoraRetorno           │
│   dataHoraCriacao           │
│   dataHoraAtualizacao       │
└──────────────┬──────────────┘
               │
             1 │
               │
             N │
               ▼
┌─────────────────────────────┐
│      HISTORICO_STATUS       │
├─────────────────────────────┤
│ # idHistorico               │
│   idRegistro                │
│   statusAnterior            │
│   statusAtual               │
│   origemAlteracao           │
│   idEventoExterno           │
│   protocoloExterno          │
│   codigoMotivo              │
│   descricaoMotivo           │
│   dataHoraCriacao           │
└─────────────────────────────┘
```

Relacionamentos:

```text
OPERACAO
   1
   │
   │ possui
   │
   N
REGISTRO_CLEARING
   1
   │
   │ possui
   │
   N
HISTORICO_STATUS
```

---

# 4. Domínios do modelo lógico

Deverão ser documentados no PowerDesigner os principais domínios controlados.

## DM_TIPO_OPERACAO

Inicialmente:

```text
APLICACAO
RESGATE
```

Novos tipos poderão ser adicionados futuramente.

## DM_PRODUTO

Inicialmente:

```text
RDB
```

Previsto:

```text
CDB
```

e demais produtos futuros.

## DM_CLEARING

Inicialmente:

```text
B3
```

Novas clearings poderão ser adicionadas sem alteração da estrutura lógica principal.

## DM_STATUS_REGISTRO

Valores iniciais deverão ser refinados com o domínio.

Exemplo conceitual:

```text
RECEBIDO
PENDENTE
ENVIADO
AGUARDANDO_RETORNO
ACEITO
REJEITADO
ERRO
```

---

# 5. Dicionário lógico — OPERACAO

| Campo | Tipo lógico | Obrigatório | Finalidade |
|---|---|---:|---|
| `idOperacao` | Identificador | Sim | Identificador único interno da operação |
| `tipoOperacao` | Domínio | Sim | Aplicação, resgate ou futuro tipo |
| `produto` | Domínio | Sim | Produto financeiro: RDB, CDB etc. |
| `valor` | Monetário | A definir | Valor financeiro da operação |
| `dataOperacao` | Data | A definir | Data da operação |
| `tipoOrigem` | Domínio | Sim | ARQUIVO, KAFKA ou futura origem |
| `idOperacaoOrigem` | Texto | A definir | Identificador da operação na origem |
| `idEventoOrigem` | Texto | Não | Identificador do evento recebido |
| `chaveIdempotencia` | Texto | A definir | Identificador determinístico para deduplicação |
| `dadosProduto` | Estrutura | Não | Atributos exclusivos do produto |
| `dataHoraCriacao` | Data/Hora | Sim | Momento da criação |
| `dataHoraAtualizacao` | Data/Hora | Sim | Última alteração |

`dadosProduto` deverá conter somente características específicas de determinado produto.

Caso um atributo seja identificado como conceito comum entre produtos, deverá ser promovido para o modelo canônico da operação.

---

# 6. Dicionário lógico — REGISTRO_CLEARING

| Campo | Tipo lógico | Obrigatório | Finalidade |
|---|---|---:|---|
| `idRegistro` | Identificador | Sim | Identificador único do registro |
| `idOperacao` | Identificador | Sim | Operação relacionada |
| `clearing` | Domínio | Sim | Clearing responsável pelo registro |
| `idExterno` | Texto | Sim antes do envio | Identificador gerado pelo sistema para correlação |
| `protocoloExterno` | Texto | Não | Protocolo atribuído pela clearing |
| `status` | Domínio | Sim | Estado atual do registro |
| `tentativa` | Inteiro | Sim | Tentativa de processamento/envio |
| `codigoUltimoErro` | Texto | Não | Código do último erro |
| `descricaoUltimoErro` | Texto | Não | Descrição do último erro |
| `dadosClearing` | Estrutura | Não | Dados exclusivos da clearing |
| `dataHoraEnvio` | Data/Hora | Não | Momento do envio |
| `dataHoraRetorno` | Data/Hora | Não | Momento do retorno |
| `dataHoraCriacao` | Data/Hora | Sim | Criação |
| `dataHoraAtualizacao` | Data/Hora | Sim | Última alteração |

`dadosClearing` deverá conter somente informações realmente específicas de uma clearing.

Conceitos comuns entre diferentes clearings deverão fazer parte do modelo canônico.

---

# 7. Dicionário lógico — HISTORICO_STATUS

| Campo | Tipo lógico | Obrigatório | Finalidade |
|---|---|---:|---|
| `idHistorico` | Identificador | Sim | Identificador lógico do evento |
| `idRegistro` | Identificador | Sim | Registro relacionado |
| `statusAnterior` | Domínio | Não | Estado anterior |
| `statusAtual` | Domínio | Sim | Novo estado |
| `origemAlteracao` | Domínio | Sim | Origem responsável pela alteração |
| `idEventoExterno` | Texto | Não | Identificador do evento externo |
| `protocoloExterno` | Texto | Não | Protocolo externo relacionado |
| `codigoMotivo` | Texto | Não | Código de motivo/rejeição |
| `descricaoMotivo` | Texto | Não | Descrição do motivo |
| `dataHoraCriacao` | Data/Hora | Sim | Momento da alteração |

O histórico deverá possuir comportamento **append-only**.

Uma nova alteração de status deverá gerar um novo evento e não sobrescrever o evento anterior.

---

# 8. Modelo físico — PowerDesigner

Embora o modelo lógico possua três entidades, a implementação física utilizará inicialmente uma única tabela DynamoDB utilizando estratégia de **Single Table Design**.

Mapeamento:

```text
MODELO LÓGICO                      MODELO FÍSICO

OPERACAO ────────────────┐
                         │
REGISTRO_CLEARING ───────┼──────► OPERACOES_CLEARING
                         │           DynamoDB
HISTORICO_STATUS ────────┘
```

A tabela física deverá ser representada no PowerDesigner aproximadamente como:

```text
┌──────────────────────────────────────────────────────────┐
│                 OPERACOES_CLEARING                       │
│                   Amazon DynamoDB                        │
├──────────────────────────────────────────────────────────┤
│ PK  chaveParticao                         String         │
│ SK  chaveOrdenacao                        String         │
│                                                          │
│     tipoEntidade                          String         │
│     idOperacao                            String         │
│     tipoOperacao                          String         │
│     produto                               String         │
│     valor                                 Number         │
│     dataOperacao                          String         │
│     tipoOrigem                            String         │
│     idOperacaoOrigem                      String         │
│     idEventoOrigem                        String         │
│     chaveIdempotencia                     String         │
│     dadosProduto                          Map            │
│                                                          │
│     idRegistro                            String         │
│     clearing                              String         │
│     idExterno                             String         │
│     protocoloExterno                      String         │
│     status                                String         │
│     tentativa                             Number         │
│     codigoUltimoErro                      String         │
│     descricaoUltimoErro                   String         │
│     dadosClearing                         Map            │
│     dataHoraEnvio                         String         │
│     dataHoraRetorno                       String         │
│                                                          │
│     statusAnterior                        String         │
│     statusAtual                           String         │
│     origemAlteracao                       String         │
│     idEventoExterno                       String         │
│     codigoMotivo                          String         │
│     descricaoMotivo                       String         │
│                                                          │
│     dataHoraCriacao                       String         │
│     dataHoraAtualizacao                   String         │
│                                                          │
│ GSI chaveParticaoIndiceIdExterno          String         │
│ GSI chaveOrdenacaoIndiceIdExterno         String         │
└──────────────────────────────────────────────────────────┘
```

Deverá ser adicionada ao modelo físico a seguinte observação:

> **Amazon DynamoDB — Single Table Design:** as entidades lógicas OPERACAO, REGISTRO_CLEARING e HISTORICO_STATUS são armazenadas como diferentes tipos de item na tabela física OPERACOES_CLEARING. A presença dos atributos dependerá do `tipoEntidade`. Apenas atributos pertencentes às chaves primárias e índices compõem obrigatoriamente o schema físico do DynamoDB.

---

# 9. Estrutura física da tabela DynamoDB

Nome lógico:

```text
OPERACOES_CLEARING
```

O nome implantado deverá seguir o padrão corporativo por sistema e ambiente.

A tabela deverá possuir:

```text
Partition Key:
chaveParticao

Sort Key:
chaveOrdenacao
```

Ambas do tipo `String`.

---

# 10. Padrão das chaves

## Operação

```text
chaveParticao =
OPERACAO#<idOperacao>

chaveOrdenacao =
METADADOS
```

## Registro Clearing

```text
chaveParticao =
OPERACAO#<idOperacao>

chaveOrdenacao =
REGISTRO#<idRegistro>
```

## Histórico

```text
chaveParticao =
OPERACAO#<idOperacao>

chaveOrdenacao =
REGISTRO#<idRegistro>
#STATUS#<dataHora>
#EVENTO#<idEvento>
```

Exemplo físico:

```text
chaveParticao = OPERACAO#123

chaveOrdenacao
───────────────────────────────────────────────────────
METADADOS

REGISTRO#REG001

REGISTRO#REG001
#STATUS#2026-09-29T13:31:00Z
#EVENTO#001

REGISTRO#REG001
#STATUS#2026-09-29T13:35:00Z
#EVENTO#002
```

---

# 11. Estrutura do item OPERACAO

Exemplo de uma aplicação de RDB:

```json
{
  "chaveParticao": "OPERACAO#550e8400-e29b-41d4-a716-446655440000",
  "chaveOrdenacao": "METADADOS",

  "tipoEntidade": "OPERACAO",

  "idOperacao": "550e8400-e29b-41d4-a716-446655440000",

  "tipoOperacao": "APLICACAO",
  "produto": "RDB",

  "valor": 15000.50,
  "dataOperacao": "2026-09-29",

  "dadosProduto": {
    "numeroRdb": "RDB-987654",
    "dataEmissao": "2026-09-29",
    "dataVencimento": "2027-09-29",
    "indexador": "CDI",
    "taxa": 102.5
  },

  "tipoOrigem": "ARQUIVO",
  "idOperacaoOrigem": "987654",
  "idEventoOrigem": "IMPORTACAO-123#LINHA-456",

  "chaveIdempotencia": "SISTEMA_ORIGEM#987654",

  "dataHoraCriacao": "2026-09-29T13:30:00Z",
  "dataHoraAtualizacao": "2026-09-29T13:30:00Z"
}
```

A futura inclusão de CDB deverá ser possível sem alteração da estrutura de chaves.

Exemplo conceitual:

```json
{
  "tipoOperacao": "APLICACAO",
  "produto": "CDB",

  "valor": 50000,

  "dadosProduto": {
    "codigoAtivo": "CDB123",
    "emissor": "BANCO_X",
    "dataVencimento": "2028-10-10",
    "indexador": "CDI",
    "taxa": 101.5
  }
}
```

---

# 12. Estrutura do item REGISTRO_CLEARING

Exemplo de registro da operação na B3:

```json
{
  "chaveParticao": "OPERACAO#550e8400-e29b-41d4-a716-446655440000",
  "chaveOrdenacao": "REGISTRO#82c918b4-79ca-4e4e-922c-22ea74cc7601",

  "tipoEntidade": "REGISTRO_CLEARING",

  "idOperacao": "550e8400-e29b-41d4-a716-446655440000",
  "idRegistro": "82c918b4-79ca-4e4e-922c-22ea74cc7601",

  "clearing": "B3",

  "idExterno": "CLR-01JXYZ123456",
  "protocoloExterno": "B3-987654321",

  "status": "ENVIADO",
  "tentativa": 1,

  "dadosClearing": {
    "codigoParticipante": "123",
    "codigoConta": "456789"
  },

  "codigoUltimoErro": null,
  "descricaoUltimoErro": null,

  "dataHoraEnvio": "2026-09-29T13:31:30Z",
  "dataHoraRetorno": null,

  "chaveParticaoIndiceIdExterno": "ID_EXTERNO#CLR-01JXYZ123456",
  "chaveOrdenacaoIndiceIdExterno": "REGISTRO#82c918b4-79ca-4e4e-922c-22ea74cc7601",

  "dataHoraCriacao": "2026-09-29T13:31:00Z",
  "dataHoraAtualizacao": "2026-09-29T13:31:30Z"
}
```

---

# 13. Estrutura do item HISTORICO_STATUS

Exemplo:

```json
{
  "chaveParticao": "OPERACAO#550e8400-e29b-41d4-a716-446655440000",

  "chaveOrdenacao": "REGISTRO#82c918b4#STATUS#2026-09-29T13:35:00.000Z#EVENTO#PISMO-EVT-789",

  "tipoEntidade": "HISTORICO_STATUS",

  "idOperacao": "550e8400-e29b-41d4-a716-446655440000",
  "idRegistro": "82c918b4-79ca-4e4e-922c-22ea74cc7601",

  "clearing": "B3",

  "statusAnterior": "ENVIADO",
  "statusAtual": "ACEITO",

  "origemAlteracao": "PISMO",

  "idEventoExterno": "PISMO-EVT-789",
  "protocoloExterno": "B3-987654321",

  "codigoMotivo": null,
  "descricaoMotivo": null,

  "dataHoraCriacao": "2026-09-29T13:35:00Z"
}
```

---

# 14. Estado atual e histórico

O item `REGISTRO_CLEARING` deverá possuir o estado atual.

Exemplo:

```text
status = ACEITO
```

O `HISTORICO_STATUS` deverá representar as transições:

```text
RECEBIDO
   ↓
PENDENTE
   ↓
ENVIADO
   ↓
AGUARDANDO_RETORNO
   ↓
ACEITO
```

O histórico deverá ser append-only.

---

# 15. Índice por identificador externo

Existe um padrão de acesso conhecido para o processamento dos retornos:

```text
idExterno
    ↓
REGISTRO_CLEARING
    ↓
idOperacao
```

Deverá ser criado um Global Secondary Index.

Nome lógico sugerido:

```text
INDICE_ID_EXTERNO
```

Estrutura:

```text
Partition Key:

chaveParticaoIndiceIdExterno
=
ID_EXTERNO#<idExterno>


Sort Key:

chaveOrdenacaoIndiceIdExterno
=
REGISTRO#<idRegistro>
```

O índice deverá permitir localizar o registro sem executar `Scan`.

Fluxo:

```text
Pismo / Retorno
      │
      │ idExterno
      ▼
INDICE_ID_EXTERNO
      │
      ▼
REGISTRO_CLEARING
      │
      ├── idOperacao
      ├── idRegistro
      ├── clearing
      └── status
```

---

# 16. Dicionário físico consolidado

| Campo | Tipo DynamoDB | Operação | Registro | Histórico | Finalidade |
|---|---|:---:|:---:|:---:|---|
| `chaveParticao` | String | ✓ | ✓ | ✓ | Partition Key |
| `chaveOrdenacao` | String | ✓ | ✓ | ✓ | Sort Key |
| `tipoEntidade` | String | ✓ | ✓ | ✓ | Tipo lógico do item |
| `idOperacao` | String | ✓ | ✓ | ✓ | Identificador interno da operação |
| `tipoOperacao` | String | ✓ | | | Aplicação, resgate etc. |
| `produto` | String | ✓ | | | RDB, CDB e futuros produtos |
| `valor` | Number | ✓ | | | Valor da operação |
| `dataOperacao` | String | ✓ | | | Data da operação |
| `tipoOrigem` | String | ✓ | | | ARQUIVO, KAFKA etc. |
| `idOperacaoOrigem` | String | ✓ | | | Identificador na origem |
| `idEventoOrigem` | String | ✓ | | | Evento da origem |
| `chaveIdempotencia` | String | ✓ | | | Idempotência da entrada |
| `dadosProduto` | Map | ✓ | | | Dados específicos do produto |
| `idRegistro` | String | | ✓ | ✓ | Identificador do registro |
| `clearing` | String | | ✓ | ✓ | Clearing relacionada |
| `idExterno` | String | | ✓ | | Correlação enviada à clearing |
| `protocoloExterno` | String | | ✓ | ✓ | Protocolo externo |
| `status` | String | | ✓ | | Estado atual |
| `tentativa` | Number | | ✓ | | Tentativa de envio |
| `dadosClearing` | Map | | ✓ | | Dados específicos da clearing |
| `codigoUltimoErro` | String | | ✓ | | Último erro |
| `descricaoUltimoErro` | String | | ✓ | | Descrição do último erro |
| `dataHoraEnvio` | String | | ✓ | | Momento do envio |
| `dataHoraRetorno` | String | | ✓ | | Momento do retorno |
| `statusAnterior` | String | | | ✓ | Estado anterior |
| `statusAtual` | String | | | ✓ | Novo estado |
| `origemAlteracao` | String | | | ✓ | Origem da alteração |
| `idEventoExterno` | String | | | ✓ | Identificação do evento externo |
| `codigoMotivo` | String | | | ✓ | Código do motivo/rejeição |
| `descricaoMotivo` | String | | | ✓ | Descrição do motivo |
| `dataHoraCriacao` | String | ✓ | ✓ | ✓ | Auditoria |
| `dataHoraAtualizacao` | String | ✓ | ✓ | | Auditoria |

---

# 17. Idempotência

Deverão ser considerados três contextos independentes.

## Entrada

`idOperacao` não deverá ser utilizado como mecanismo de idempotência.

Deverá existir:

```text
chaveIdempotencia
```

A composição definitiva será definida após confirmação das garantias fornecidas pelas origens.

Exemplo conceitual:

```text
<SISTEMA_ORIGEM>#<ID_OPERACAO_ORIGEM>
```

## Envio para clearing

O `idExterno` deverá ser criado antes do envio e permanecer estável durante retries da mesma solicitação lógica, salvo comportamento específico exigido pela clearing.

## Retorno

Quando disponível deverá ser armazenado:

```text
idEventoExterno
```

para identificação de notificações externas duplicadas.

---

# 18. Infraestrutura Terraform

A tabela deverá ser provisionada integralmente através de Terraform.

O recurso deverá contemplar:

- tabela DynamoDB;
- Partition Key;
- Sort Key;
- GSI de identificador externo;
- configuração de capacidade;
- criptografia;
- Point-in-Time Recovery;
- tags corporativas;
- outputs necessários.

Deverão ser disponibilizados pelo módulo pelo menos:

```text
nomeTabela
arnTabela
nomeIndiceIdExterno
```

Importante: atributos como:

```text
produto
valor
status
dadosProduto
dadosClearing
```

não deverão ser declarados no Terraform apenas por existirem no modelo.

No DynamoDB, o Terraform deverá declarar os atributos necessários para:

- chave primária;
- chave de ordenação;
- índices.

Os demais atributos pertencem ao contrato da aplicação.

---

# Requisitos

**RF01 — PowerDesigner**

Deverão ser criados e/ou atualizados os modelos lógico e físico no PowerDesigner conforme normativa corporativa.

**RF02 — Modelo lógico**

O modelo lógico deverá possuir as entidades:

```text
OPERACAO
REGISTRO_CLEARING
HISTORICO_STATUS
```

com relacionamentos 1:N.

**RF03 — Modelo físico**

O modelo físico deverá representar a tabela DynamoDB `OPERACOES_CLEARING` utilizando Single Table Design.

**RF04 — Terraform**

A infraestrutura deverá ser criada exclusivamente através de Terraform.

**RF05 — Português**

Nomes de tabela, atributos, índices, entidades e elementos pertencentes à aplicação deverão utilizar nomenclatura em português.

**RF06 — Modelo canônico**

O modelo não deverá reproduzir diretamente contratos de CSV, Kafka, B3 ou Pismo.

**RF07 — Multi-produto**

O modelo deverá suportar inicialmente RDB e permitir CDB e futuros produtos sem alteração da estrutura principal de chaves.

**RF08 — Multi-clearing**

O modelo deverá permitir B3 e futuras clearings sem alteração da estrutura principal de chaves.

**RF09 — Separação produto/clearing**

Produto deverá caracterizar a operação e clearing deverá caracterizar o registro.

**RF10 — Dados específicos de produto**

Informações exclusivas de determinado produto deverão poder ser armazenadas em `dadosProduto`.

**RF11 — Dados específicos de clearing**

Informações exclusivas de determinada clearing deverão poder ser armazenadas em `dadosClearing`.

**RF12 — Histórico**

As alterações de status deverão ser persistidas como eventos append-only.

**RF13 — Estado atual**

O estado atual deverá ser mantido no item `REGISTRO_CLEARING`.

**RF14 — Correlação**

O modelo deverá diferenciar `idExterno` e `protocoloExterno`.

**RF15 — Idempotência**

A estrutura deverá estar preparada para idempotência de entrada, envio e retorno.

**RF16 — Consulta de retorno**

Deverá existir GSI permitindo localizar o registro através de `idExterno` sem utilização de Scan.

**RF17 — Segurança**

Criptografia deverá ser configurada através de Terraform conforme padrão corporativo.

**RF18 — Recuperação**

Point-in-Time Recovery deverá ser configurado conforme padrão corporativo.

**RF19 — Capacidade**

A configuração deverá considerar o volume esperado de aproximadamente **20 a 30 milhões de operações por arquivo**, com possibilidade de mais de um arquivo por dia e futura entrada através de Kafka.

**RF20 — Streams**

DynamoDB Streams deverá permanecer desabilitado nesta história.

---

# Critérios de aceite

**CA01**

O modelo lógico deverá estar disponível no PowerDesigner contendo:

```text
OPERACAO
    1:N
REGISTRO_CLEARING
    1:N
HISTORICO_STATUS
```

**CA02**

O modelo lógico deverá representar `produto` como característica da operação e `clearing` como característica do registro.

**CA03**

O modelo físico deverá representar uma única tabela DynamoDB utilizando Single Table Design.

**CA04**

O modelo físico deverá documentar os padrões de `chaveParticao` e `chaveOrdenacao`.

**CA05**

O modelo físico deverá documentar os três tipos de item:

```text
OPERACAO
REGISTRO_CLEARING
HISTORICO_STATUS
```

**CA06**

O modelo físico deverá documentar o `INDICE_ID_EXTERNO`.

**CA07**

O Terraform deverá criar a tabela utilizando:

```text
chaveParticao
chaveOrdenacao
```

**CA08**

Deverá ser possível persistir RDB utilizando:

```text
produto = RDB
```

sem existir tabela específica para RDB.

**CA09**

A inclusão futura de CDB deverá ser possível utilizando:

```text
produto = CDB
```

sem alteração das chaves da tabela.

**CA10**

A inclusão de uma nova clearing deverá ser possível sem alteração das chaves da tabela.

**CA11**

Deverá ser possível persistir dados específicos de produto através de `dadosProduto`.

**CA12**

Deverá ser possível persistir dados específicos de clearing através de `dadosClearing`.

**CA13**

Uma operação deverá poder possuir mais de um `REGISTRO_CLEARING`.

**CA14**

Um registro deverá poder possuir múltiplos itens `HISTORICO_STATUS`.

**CA15**

Os eventos históricos não deverão ser sobrescritos por alterações posteriores.

**CA16**

Dado um `idExterno`, deverá ser possível localizar o respectivo registro utilizando o GSI, sem executar Scan.

**CA17**

O modelo deverá distinguir:

```text
idOperacao
idRegistro
idExterno
protocoloExterno
chaveIdempotencia
idEventoExterno
```

**CA18**

O modelo não deverá possuir dependência estrutural exclusiva de:

```text
RDB
B3
CSV
Pismo
```

**CA19**

Ao executar `terraform plan`, deverão ser apresentados os recursos previstos nesta história sem configurações manuais adicionais.

**CA20**

Uma segunda execução de `terraform plan`, sem alterações, não deverá apresentar mudanças inesperadas.

**CA21**

DynamoDB Streams deverá permanecer desabilitado após a implantação desta história.

---

# Decisões pendentes

Os seguintes pontos deverão ser refinados posteriormente:

1. Campos definitivos do modelo canônico da operação;
2. Campos específicos de RDB;
3. Campos específicos de CDB;
4. Identificação de atributos comuns entre diferentes produtos;
5. Composição definitiva da `chaveIdempotencia`;
6. Garantias de unicidade fornecidas pelas origens;
7. Estratégia de idempotência de cada clearing;
8. Garantia de unicidade do `idEventoExterno` recebido da Pismo;
9. Status definitivos e transições permitidas;
10. Necessidade de consultas por produto;
11. Necessidade de consultas por clearing;
12. Necessidade de consultas por status;
13. Necessidade de consultas por período;
14. Necessidade de consulta por `protocoloExterno`;
15. Necessidade de novos GSIs.

Novos GSIs deverão ser criados somente após confirmação de padrões reais de acesso, considerando impacto de escrita, armazenamento e custo sobre o volume esperado.

---

# Fora do escopo

Esta história não contempla:

```text
AWS Batch
leitura do CSV

Kafka Consumer

Adaptador CSV
Adaptador Kafka

DynamoDB Streams
EventBridge Pipes

SQS
Lambda

Registration Worker

Adaptador B3
Adaptador Pismo

envio para clearing
processamento do retorno

implementação das regras de negócio
implementação das transições de status
```

A entrega desta história deverá disponibilizar:

```text
1. Modelo lógico PowerDesigner
2. Modelo físico PowerDesigner
3. Estrutura DynamoDB
4. GSI de idExterno
5. Infraestrutura Terraform
```

preparando a persistência para as próximas etapas do projeto.

==================================================================

# Título

Processar arquivo de operações e persistir modelo canônico através do AWS Batch

# Objetivo

Implementar um processo de ingestão em **AWS Batch**, desenvolvido em **.NET**, responsável por processar arquivos CSV contendo operações financeiras e persistir essas operações no DynamoDB utilizando o modelo canônico definido pelo sistema de Clearing.

Inicialmente os arquivos conterão operações relacionadas ao produto **RDB**, podendo conter operações de:

- aplicação;
- resgate.

O processo deverá ser projetado para suportar arquivos de grande volume, atualmente estimados entre **20 e 30 milhões de registros por arquivo**, normalmente recebidos uma vez ao dia, podendo ocorrer duas ou mais execuções conforme necessidade operacional.

O processamento não deverá acoplar o domínio do sistema ao formato do CSV.

O AWS Batch deverá utilizar um **Adaptador CSV** para transformar cada registro recebido no modelo canônico `OPERACAO` antes da persistência.

A arquitetura deverá estar preparada para que futuramente outras fontes, como Kafka, produzam o mesmo modelo canônico sem alterar o domínio de Clearing.

---

# Descrição detalhada

## 1. Contexto

O sistema receberá arquivos CSV contendo operações financeiras que deverão posteriormente ser registradas em uma clearing.

O fluxo inicial será:

```text
Sistema produtor
      │
      │ CSV
      ▼
 Amazon S3
      │
      │ evento de criação
      ▼
 EventBridge
      │
      ▼
  AWS Batch
      │
      │ leitura / parse / validação / adaptação
      ▼
 Modelo Canônico
      │
      ▼
  DynamoDB
```

O arquivo será disponibilizado em bucket S3 previamente definido.

A criação do arquivo deverá gerar o evento responsável por iniciar o processo de ingestão.

O EventBridge deverá identificar o evento e iniciar o Job correspondente no AWS Batch.

---

# 2. Responsabilidade do AWS Batch

O AWS Batch será responsável por:

1. receber as informações necessárias para identificar o arquivo no S3;
2. abrir o arquivo utilizando processamento por streaming;
3. percorrer os registros sem carregar o arquivo completo em memória;
4. realizar o parse de cada registro;
5. validar requisitos mínimos necessários para construção da operação;
6. transformar o contrato CSV no modelo canônico;
7. identificar o produto da operação;
8. construir os atributos específicos do produto;
9. gerar ou determinar a chave de idempotência;
10. persistir as operações no DynamoDB;
11. registrar métricas e logs do processamento;
12. controlar falhas de registros individuais;
13. disponibilizar informações suficientes para rastrear o arquivo e a execução;
14. encerrar a execução indicando sucesso ou falha do processamento.

O Batch não deverá possuir regras de integração com nenhuma clearing.

Portanto, não é responsabilidade desta etapa:

```text
B3
Pismo
Clearing X
Registration Worker
envio de operações
retorno das clearings
```

---

# 3. Modelo canônico

O formato do CSV não deverá ser persistido diretamente.

O fluxo deverá ser:

```text
Linha CSV
   │
   ▼
Parse
   │
   ▼
Registro CSV
   │
   ▼
Adaptador CSV
   │
   ▼
Operacao
Modelo Canônico
   │
   ▼
DynamoDB
```

Exemplo conceitual de entrada:

```text
CD_OPERACAO
TP_MOV
VL_FINANCEIRO
CD_PRODUTO
...
```

Não deverão ser utilizados diretamente esses nomes no modelo persistido apenas porque fazem parte do contrato externo.

O Adaptador CSV deverá realizar o mapeamento:

```text
CD_OPERACAO
      ↓
idOperacaoOrigem

TP_MOV
      ↓
tipoOperacao

VL_FINANCEIRO
      ↓
valor

CD_PRODUTO
      ↓
produto
```

O mapeamento definitivo dependerá do layout oficial do arquivo.

---

# 4. Separação de responsabilidades

A implementação deverá manter separadas as responsabilidades de leitura, parsing, adaptação, validação e persistência.

Estrutura conceitual:

```text
AWS Batch (.NET)
│
├── LeitorArquivoS3
│
├── ParserCsv
│
├── AdaptadorCsv
│
├── MapeadorProduto
│   └── RDB
│
├── ValidadorOperacao
│
├── GeradorIdempotencia
│
├── RepositorioOperacao
│
└── Telemetria
```

O objetivo é evitar que o código responsável pela leitura do arquivo conheça detalhes do DynamoDB ou regras específicas de produto.

---

# 5. Adaptador CSV

O `AdaptadorCsv` será responsável por transformar o contrato externo recebido no modelo utilizado pelo domínio.

Exemplo:

```text
RegistroCsv
      │
      ▼
AdaptadorCsv
      │
      ▼
Operacao
```

O Adaptador deverá conhecer o layout do CSV.

O restante do domínio não deverá conhecer:

```text
posição das colunas;
nomes das colunas;
separador;
formatação específica;
códigos específicos do arquivo.
```

Caso futuramente o contrato do arquivo seja alterado, o impacto deverá ficar concentrado no parser/adaptador.

---

# 6. Produto

O processamento deverá identificar o produto associado à operação.

Inicialmente:

```text
produto = RDB
```

A arquitetura deverá permitir futuramente:

```text
produto = CDB
```

e novos produtos sem necessidade de reescrever o pipeline de ingestão.

O produto deverá ser representado como atributo do modelo canônico.

Exemplo:

```json
{
  "tipoOperacao": "APLICACAO",
  "produto": "RDB"
}
```

---

# 7. Dados específicos do produto

Informações realmente específicas de RDB deverão ser armazenadas através da estrutura:

```text
dadosProduto
```

Exemplo conceitual:

```json
{
  "produto": "RDB",

  "dadosProduto": {
    "numeroRdb": "RDB-987654",
    "dataEmissao": "2026-09-29",
    "dataVencimento": "2027-09-29",
    "indexador": "CDI",
    "taxa": 102.5
  }
}
```

Os campos definitivos dependerão do layout do arquivo e da definição do modelo RDB.

`dadosProduto` não deverá ser utilizado indiscriminadamente para atributos que pertencem ao modelo canônico.

Caso determinado atributo seja comum a RDB, CDB e demais produtos, ele deverá ser avaliado como atributo da própria `OPERACAO`.

---

# 8. Modelo persistido

Após adaptação, deverá ser persistido um item do tipo:

```text
OPERACAO
```

na tabela:

```text
OPERACOES_CLEARING
```

Estrutura física:

```text
chaveParticao =
OPERACAO#<idOperacao>

chaveOrdenacao =
METADADOS
```

Exemplo:

```json
{
  "chaveParticao": "OPERACAO#550e8400-e29b-41d4-a716-446655440000",
  "chaveOrdenacao": "METADADOS",

  "tipoEntidade": "OPERACAO",

  "idOperacao": "550e8400-e29b-41d4-a716-446655440000",

  "tipoOperacao": "APLICACAO",
  "produto": "RDB",

  "valor": 15000.50,
  "dataOperacao": "2026-09-29",

  "dadosProduto": {
    "numeroRdb": "RDB-987654",
    "dataEmissao": "2026-09-29",
    "dataVencimento": "2027-09-29",
    "indexador": "CDI",
    "taxa": 102.5
  },

  "tipoOrigem": "ARQUIVO",
  "idOperacaoOrigem": "987654",
  "idEventoOrigem": "ARQUIVO-20260929#LINHA-456",

  "chaveIdempotencia": "SISTEMA_ORIGEM#987654",

  "dataHoraCriacao": "2026-09-29T13:30:00Z",
  "dataHoraAtualizacao": "2026-09-29T13:30:00Z"
}
```

O AWS Batch deverá persistir somente a `OPERACAO`.

A criação de:

```text
REGISTRO_CLEARING
HISTORICO_STATUS
```

não deverá ser responsabilidade desta história, salvo decisão arquitetural posterior em contrário.

---

# 9. Identificação da operação

Cada operação deverá receber:

```text
idOperacao
```

como identificador interno do sistema de Clearing.

O identificador deverá ser único.

Inicialmente poderá ser utilizado UUID/GUID.

Exemplo:

```text
550e8400-e29b-41d4-a716-446655440000
```

Entretanto, `idOperacao` não deverá ser considerado mecanismo de idempotência.

---

# 10. Rastreabilidade da origem

Toda operação recebida através do arquivo deverá permitir identificar sua origem.

Deverão ser preenchidos, conforme disponibilidade:

| Campo | Finalidade |
|---|---|
| `tipoOrigem` | Identifica que a operação veio de arquivo |
| `idOperacaoOrigem` | Identificador da operação no sistema produtor |
| `idEventoOrigem` | Identificação do registro/evento específico recebido |
| `chaveIdempotencia` | Identificação lógica da operação para deduplicação |

Para esta ingestão:

```text
tipoOrigem = ARQUIVO
```

O `idEventoOrigem` deverá permitir, sempre que possível, relacionar a operação ao arquivo e registro original.

Exemplo conceitual:

```text
ARQUIVO#20260929_001#LINHA#000000456
```

A composição definitiva deverá considerar as informações disponíveis no contrato do arquivo.

---

# 11. Idempotência

O processo deverá ser desenvolvido considerando que:

- o mesmo arquivo pode ser entregue novamente;
- um job pode falhar após persistir parcialmente o arquivo;
- um job pode ser reexecutado;
- uma mesma operação pode aparecer novamente em uma nova execução.

Portanto:

```text
idOperacao = Guid.NewGuid()
```

não resolve o problema de duplicidade.

Deverá existir:

```text
chaveIdempotencia
```

A composição definitiva dependerá das garantias fornecidas pelo sistema produtor.

Possível exemplo:

```text
<SISTEMA_ORIGEM>#<ID_OPERACAO_ORIGEM>
```

ou, caso seja necessário incluir outras dimensões:

```text
<SISTEMA_ORIGEM>
#<PRODUTO>
#<ID_OPERACAO_ORIGEM>
#<TIPO_OPERACAO>
```

A composição deverá ser definida com base na identidade real da operação no negócio e não simplesmente pelo número da linha do arquivo.

O número da linha poderá ser utilizado para rastreabilidade, mas não deverá automaticamente ser considerado a identidade da operação.

---

# 12. Persistência idempotente

A implementação deverá evitar que duas execuções concorrentes persistam a mesma operação como registros diferentes.

A estratégia definitiva deverá ser definida juntamente com a modelagem de idempotência.

Deverão ser avaliadas técnicas compatíveis com DynamoDB, como:

```text
Conditional Write

Item dedicado de idempotência

TransactWriteItems
```

Não deverá ser implementada uma estratégia baseada em:

```text
Query
   ↓
não encontrou
   ↓
Put
```

como única proteção contra duplicidade, pois duas execuções concorrentes podem realizar a consulta simultaneamente.

---

# 13. Processamento do arquivo

O arquivo poderá possuir aproximadamente:

```text
20.000.000
a
30.000.000
```

de registros.

O processamento deverá ocorrer de forma streaming.

Não deverá ser realizado:

```text
File.ReadAllLines()
```

ou qualquer estratégia equivalente que carregue todo o arquivo em memória.

O comportamento esperado é:

```text
Abrir stream S3
      │
      ▼
Ler registro
      │
      ▼
Parse
      │
      ▼
Validar
      │
      ▼
Adaptar
      │
      ▼
Persistir
      │
      ▼
Próximo registro
```

---

# 14. Escrita no DynamoDB

Considerando o volume esperado, não deverá ser realizada necessariamente uma chamada individual ao DynamoDB para cada linha quando houver alternativa mais eficiente.

A implementação deverá avaliar utilização de operações em lote compatíveis com a estratégia de idempotência adotada.

O processamento deverá respeitar:

- limites das APIs do DynamoDB;
- tamanho máximo de item;
- throttling;
- capacidade configurada;
- retries;
- backoff;
- limites de concorrência.

O grau de paralelismo deverá ser configurável.

Não deverá existir paralelismo ilimitado baseado no número total de registros.

---

# 15. Controle de backpressure

O Batch deverá evitar produzir operações para o DynamoDB em velocidade superior à capacidade segura de persistência.

Deverá existir controle de concorrência.

Fluxo esperado:

```text
S3
 │
 ▼
Leitura
 │
 ▼
Parsing
 │
 ▼
Adaptação
 │
 ▼
Fila interna limitada
 │
 ▼
Workers de persistência
 │
 ▼
DynamoDB
```

A fila interna deverá possuir tamanho limitado para impedir crescimento descontrolado de memória.

Parâmetros como:

```text
quantidadeWorkers
tamanhoBuffer
tamanhoLote
numeroMaximoRetries
```

deverão ser configuráveis.

---

# 16. Falhas transitórias

Falhas transitórias de infraestrutura não deverão automaticamente invalidar uma operação.

Exemplos:

```text
throttling do DynamoDB;
timeout;
falha temporária de rede;
erro transitório AWS.
```

Deverá existir política de retry com backoff.

A política deverá possuir quantidade máxima de tentativas para impedir retry infinito.

Após esgotamento das tentativas, a falha deverá ser registrada e tratada conforme estratégia definida para falhas de processamento.

---

# 17. Registro inválido

Uma linha inválida não deverá necessariamente provocar a perda das demais milhões de operações válidas.

Deverão ser diferenciadas:

```text
falha de negócio/dado

e

falha técnica
```

Exemplos de dado inválido:

```text
tipoOperacao inexistente;
produto não suportado;
valor inválido;
data inválida;
campo obrigatório ausente;
layout incompatível.
```

A estratégia deverá permitir identificar:

- arquivo;
- linha;
- motivo;
- campo, quando aplicável.

---

# 18. Tratamento de registros rejeitados

Registros que não possam ser transformados no modelo canônico deverão ser contabilizados como rejeitados.

A implementação deverá permitir rastrear o erro sem depender apenas dos logs da aplicação.

Poderá ser utilizado artefato de saída no S3, conforme padrão definido para o projeto, contendo os registros rejeitados e seus respectivos motivos.

Exemplo conceitual:

```text
/processados/
   /2026-09-29/
      /<idExecucao>/
         resumo.json
         rejeitados.csv
```

A estrutura definitiva do bucket deverá seguir o padrão corporativo.

Dados sensíveis não deverão ser expostos desnecessariamente em arquivos ou logs de erro.

---

# 19. Falha estrutural do arquivo

Erros estruturais deverão poder interromper o processamento.

Exemplos:

```text
arquivo vazio;
cabeçalho incompatível;
versão de layout não suportada;
encoding inválido;
arquivo corrompido;
colunas obrigatórias inexistentes.
```

Nesses casos, o job deverá ser encerrado como falha e nenhum processamento adicional deverá continuar quando não for possível interpretar o contrato recebido de forma segura.

---

# 20. Identificação do arquivo

Cada execução deverá conhecer pelo menos:

```text
bucket
chaveObjeto
```

Quando disponíveis, também deverão ser utilizados:

```text
versionId
eTag
```

para identificar de forma inequívoca o objeto processado.

Não deverá ser assumido que apenas o nome do arquivo representa necessariamente uma versão única.

---

# 21. Identificação da execução

Cada processamento deverá possuir um identificador de execução.

Exemplo:

```text
idExecucao
```

Esse identificador deverá ser utilizado para correlação de:

- logs;
- métricas;
- arquivo;
- registros rejeitados;
- resumo da execução.

A identificação do AWS Batch Job poderá fazer parte dessa correlação.

---

# 22. Resumo de processamento

Ao final do processamento deverá ser possível obter um resumo semelhante a:

```json
{
  "idExecucao": "EXEC-20260929-001",
  "arquivo": "operacoes-20260929.csv",

  "totalRegistros": 30000000,
  "totalProcessados": 29999850,
  "totalPersistidos": 29999700,
  "totalDuplicados": 100,
  "totalRejeitados": 50,

  "produto": "RDB",

  "dataHoraInicio": "2026-09-29T10:00:00Z",
  "dataHoraFim": "2026-09-29T10:42:15Z",

  "status": "CONCLUIDO_COM_REJEICOES"
}
```

Os números acima são apenas ilustrativos.

---

# 23. Estados da execução

A execução deverá possuir resultado claramente identificável.

Exemplo conceitual:

```text
INICIADO

PROCESSANDO

CONCLUIDO

CONCLUIDO_COM_REJEICOES

FALHA
```

A implementação concreta do controle da execução deverá ser definida conforme padrões de observabilidade do projeto.

---

# 24. Observabilidade

O processo deverá gerar logs estruturados.

Cada log relevante deverá possuir, quando aplicável:

```text
idExecucao
bucket
chaveObjeto
produto
tipoOperacao
idOperacaoOrigem
idOperacao
etapa
```

Não deverão ser registrados payloads completos indiscriminadamente.

Os logs deverão permitir responder perguntas como:

```text
Qual arquivo foi processado?

Quantas linhas foram lidas?

Quantas operações foram persistidas?

Quantas eram duplicadas?

Quantas foram rejeitadas?

Por que determinada linha falhou?

Quanto tempo o processamento levou?

Houve throttling no DynamoDB?

Houve retries?

Em qual etapa ocorreu a falha?
```

---

# 25. Métricas

Deverão ser disponibilizadas métricas para acompanhamento do processamento.

No mínimo:

```text
registrosLidos

registrosValidos

registrosInvalidos

registrosPersistidos

registrosDuplicados

errosPersistencia

retriesDynamoDb

tempoProcessamento

registrosPorSegundo
```

Também deverá ser possível identificar falha completa do Job.

---

# 26. Segurança

O AWS Batch deverá utilizar IAM Role própria seguindo princípio de menor privilégio.

A role deverá possuir somente as permissões necessárias, incluindo, conforme implementação:

```text
leitura do bucket/prefixo S3;

escrita no DynamoDB;

escrita de logs/métricas;

acesso a KMS quando necessário;

escrita de artefatos de rejeição/resumo,
caso essa estratégia seja adotada.
```

Não deverão ser armazenadas credenciais AWS no código ou configuração da aplicação.

---

# 27. Configuração

Valores operacionais deverão ser externos ao código quando apropriado.

Exemplos:

```text
nomeTabela

quantidadeWorkers

tamanhoBuffer

tamanhoLote

numeroMaximoRetries

timeout

produto/layout suportado
```

Configurações específicas por ambiente não deverão exigir recompilação da aplicação.

---

# 28. Infraestrutura como código

Os recursos AWS necessários ao AWS Batch deverão ser provisionados através de **Terraform**, seguindo o padrão corporativo.

Conforme a arquitetura definida, isso poderá contemplar:

```text
AWS Batch Job Definition

Compute Environment

Job Queue

IAM Roles

CloudWatch Logs

EventBridge Rule / Target

configurações necessárias de rede

Security Groups, quando aplicável
```

Nenhuma configuração manual deverá ser necessária para implantação normal entre ambientes.

---

# 29. Evolução para novos produtos

O primeiro produto suportado será:

```text
RDB
```

Entretanto, a arquitetura deverá permitir posteriormente:

```text
CDB
LCI
LCA
DEBENTURE
...
```

sem duplicação do pipeline inteiro.

Conceitualmente:

```text
                   CSV
                    │
                    ▼
                Parser CSV
                    │
                    ▼
              Adaptador CSV
                    │
                    ▼
             Modelo Canônico
                    │
           ┌────────┼─────────┐
           │        │         │
           ▼        ▼         ▼
          RDB      CDB      Futuro
```

A inclusão de um novo produto deverá concentrar alterações nas regras/mapeamentos específicos daquele produto, preservando o fluxo genérico de leitura, controle, observabilidade e persistência.

---

# 30. Evolução para Kafka

Futuramente operações também serão recebidas através de Kafka.

O fluxo será conceitualmente:

```text
CSV ──► Adaptador CSV ───┐
                         │
                         ▼
                   OPERACAO CANÔNICA
                         ▲
                         │
Kafka ► Adaptador Kafka ─┘
```

O AWS Batch não será responsável por consumir Kafka.

A futura entrada Kafka deverá produzir o mesmo modelo canônico utilizado pelo processamento do arquivo.

Isso significa que o DynamoDB não deverá distinguir estruturalmente uma operação apenas porque ela veio de arquivo ou Kafka.

A origem deverá ser metadata:

```text
tipoOrigem = ARQUIVO
```

ou:

```text
tipoOrigem = KAFKA
```

---

# Requisitos

**RF01 — AWS Batch**

O processamento do arquivo deverá ser executado através de AWS Batch.

**RF02 — .NET**

A aplicação executada pelo Batch deverá ser desenvolvida em .NET conforme versão homologada pelo projeto.

**RF03 — S3**

O processo deverá consumir o arquivo diretamente do Amazon S3.

**RF04 — EventBridge**

A criação/disponibilização do arquivo deverá iniciar o fluxo através do mecanismo EventBridge definido na arquitetura.

**RF05 — Streaming**

O arquivo deverá ser processado em streaming, sem carregamento integral em memória.

**RF06 — Alto volume**

A solução deverá suportar arquivos da ordem de 20 a 30 milhões de registros.

**RF07 — Modelo canônico**

Nenhum contrato externo deverá ser persistido diretamente como modelo de domínio.

**RF08 — Adaptador**

O contrato CSV deverá ser transformado para o modelo canônico através de componente de adaptação.

**RF09 — Produto**

Cada operação deverá possuir identificação do produto.

Inicialmente:

```text
RDB
```

**RF10 — Evolução de produtos**

A arquitetura deverá permitir inclusão futura de CDB e demais produtos sem duplicação do pipeline completo.

**RF11 — Dados de produto**

Dados exclusivos do produto deverão ser armazenados através de `dadosProduto` quando não fizerem parte do modelo canônico.

**RF12 — Tipo da operação**

Deverão ser suportados inicialmente:

```text
APLICACAO
RESGATE
```

**RF13 — Origem**

Operações provenientes do arquivo deverão possuir:

```text
tipoOrigem = ARQUIVO
```

**RF14 — Rastreabilidade**

A operação deverá permitir correlação com o registro recebido no arquivo.

**RF15 — Idempotência**

O processamento deverá estar preparado para reexecução do arquivo sem gerar duplicidade de operações.

**RF16 — Concorrência**

A estratégia de idempotência deverá considerar execuções concorrentes.

**RF17 — Persistência**

As operações deverão ser persistidas na tabela `OPERACOES_CLEARING` utilizando o padrão de chave definido no modelo físico.

**RF18 — Backpressure**

O processo deverá possuir limite configurável de concorrência/buffer de persistência.

**RF19 — Retry**

Falhas transitórias deverão possuir política de retry com backoff e limite máximo de tentativas.

**RF20 — Registros inválidos**

Registros inválidos deverão ser identificáveis individualmente.

**RF21 — Falha estrutural**

Arquivos incompatíveis com o layout esperado deverão causar falha controlada da execução.

**RF22 — Observabilidade**

O processo deverá possuir logs estruturados e métricas suficientes para acompanhamento operacional.

**RF23 — Segurança**

O Job deverá utilizar IAM Role com menor privilégio.

**RF24 — Terraform**

A infraestrutura deverá ser provisionada através de Terraform.

**RF25 — Clearing**

O Batch não deverá conter regras específicas de B3 ou qualquer outra clearing.

**RF26 — Registro Clearing**

A criação/processamento de `REGISTRO_CLEARING` não faz parte da responsabilidade deste Job.

**RF27 — Histórico**

A criação de `HISTORICO_STATUS` não faz parte desta história.

---

# Critérios de aceite

**CA01**

Dado um arquivo válido disponibilizado no S3,

quando o evento correspondente for identificado,

então deverá ser iniciado o AWS Batch Job responsável pelo processamento.

**CA02**

O arquivo deverá ser processado em streaming sem carregamento completo em memória.

**CA03**

Cada linha válida deverá ser transformada do contrato CSV para o modelo canônico antes da persistência.

**CA04**

O modelo persistido não deverá utilizar diretamente nomes e estrutura do CSV como contrato principal da operação.

**CA05**

Uma operação RDB deverá ser persistida contendo:

```text
tipoEntidade = OPERACAO
produto = RDB
tipoOrigem = ARQUIVO
```

**CA06**

Uma operação deverá ser persistida utilizando:

```text
chaveParticao =
OPERACAO#<idOperacao>

chaveOrdenacao =
METADADOS
```

**CA07**

Cada operação deverá possuir `idOperacao` único.

**CA08**

Cada operação deverá possuir informação suficiente para rastrear sua origem.

**CA09**

A reexecução do mesmo arquivo não deverá gerar novas operações para registros já processados, conforme estratégia de idempotência definida.

**CA10**

Duas execuções concorrentes não deverão conseguir persistir duas representações diferentes da mesma operação lógica.

**CA11**

Uma linha inválida deverá ser identificada com informações suficientes para determinar arquivo, registro e motivo da rejeição.

**CA12**

Uma linha inválida não deverá, isoladamente, interromper o processamento de todas as demais linhas válidas, salvo quando o erro indicar comprometimento estrutural do arquivo.

**CA13**

Um arquivo com layout incompatível deverá provocar falha controlada antes da continuidade do processamento.

**CA14**

Falhas transitórias de persistência deverão executar retry conforme política configurada.

**CA15**

Após esgotamento dos retries, a falha deverá ser registrada e refletida no resultado do processamento.

**CA16**

Ao final da execução deverá ser possível determinar:

```text
total de registros lidos;
total válido;
total persistido;
total duplicado;
total rejeitado;
total de erros de persistência;
tempo total;
status final.
```

**CA17**

Os logs deverão permitir correlacionar uma ocorrência com o arquivo e a execução correspondente.

**CA18**

O nível de paralelismo deverá ser configurável sem alteração do código.

**CA19**

O processo não deverá possuir dependência de contrato específico da B3.

**CA20**

A inclusão futura de uma nova clearing não deverá exigir alteração no processamento do arquivo.

**CA21**

A arquitetura deverá permitir inclusão futura de CDB sem duplicação completa do pipeline de ingestão.

**CA22**

A futura entrada Kafka deverá poder utilizar o mesmo modelo canônico persistido pelo Batch.

**CA23**

O AWS Batch deverá possuir apenas as permissões IAM necessárias para execução de suas responsabilidades.

**CA24**

Os recursos de infraestrutura previstos nesta história deverão estar declarados em Terraform.

**CA25**

Uma nova execução de `terraform plan`, sem alteração de configuração, não deverá apresentar mudanças inesperadas.

---

# Decisões pendentes

Os seguintes pontos deverão ser definidos/refinados antes ou durante o desenvolvimento:

1. layout definitivo do CSV;
2. separador, encoding e presença de cabeçalho;
3. campos obrigatórios;
4. campos canônicos definitivos da operação;
5. campos específicos de RDB;
6. mecanismo de identificação do produto no arquivo;
7. composição definitiva da `chaveIdempotencia`;
8. garantia de unicidade de `idOperacaoOrigem`;
9. estratégia definitiva de persistência idempotente no DynamoDB;
10. tratamento definitivo de registros rejeitados;
11. localização/formato do relatório de rejeições;
12. critérios para `CONCLUIDO_COM_REJEICOES` versus `FALHA`;
13. limites aceitáveis de rejeição;
14. quantidade inicial de workers;
15. tamanho do buffer interno;
16. estratégia de escrita em lote;
17. capacidade DynamoDB durante a janela de ingestão;
18. timeout máximo do Job;
19. política de retry;
20. alarmes operacionais;
21. nomenclatura definitiva dos recursos AWS.

---

# Fora do escopo

Esta história não contempla:

```text
Kafka Consumer

DynamoDB Streams

EventBridge Pipes

SQS de registro

Registration Worker

REGISTRO_CLEARING

HISTORICO_STATUS

Adaptador B3

integração B3

Pismo

SNS de retorno

SQS de retorno

processamento do retorno

regras específicas de clearing
```

A responsabilidade desta história termina em:

```text
                ARQUIVO CSV
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
             ┌───────┴────────┐
             │                │
             ▼                ▼
         Parser CSV       Validação
             │
             ▼
        Adaptador CSV
             │
             ▼
       Modelo Canônico
             │
       produto = RDB
             │
             ▼
     Idempotência / Persistência
             │
             ▼
           DynamoDB
             │
             ▼
          OPERACAO
```

O processamento posterior da operação para determinar, preparar e executar o registro na B3 ou em qualquer outra clearing pertence às próximas etapas da arquitetura.



#!/bin/bash

set -e

TABELA="OPERACOES_CLEARING"

echo "=========================================="
echo " Criando POC DynamoDB - Multi Clearing"
echo "=========================================="

echo ""
echo "1. Criando tabela $TABELA..."

aws dynamodb create-table \
  --table-name "$TABELA" \
  --attribute-definitions \
      AttributeName=chaveParticao,AttributeType=S \
      AttributeName=chaveOrdenacao,AttributeType=S \
      AttributeName=chaveParticaoIndiceIdExterno,AttributeType=S \
      AttributeName=chaveOrdenacaoIndiceIdExterno,AttributeType=S \
  --key-schema \
      AttributeName=chaveParticao,KeyType=HASH \
      AttributeName=chaveOrdenacao,KeyType=RANGE \
  --global-secondary-indexes '[
    {
      "IndexName": "INDICE_ID_EXTERNO",
      "KeySchema": [
        {
          "AttributeName": "chaveParticaoIndiceIdExterno",
          "KeyType": "HASH"
        },
        {
          "AttributeName": "chaveOrdenacaoIndiceIdExterno",
          "KeyType": "RANGE"
        }
      ],
      "Projection": {
        "ProjectionType": "ALL"
      }
    }
  ]' \
  --billing-mode PAY_PER_REQUEST

echo ""
echo "2. Aguardando tabela ficar disponível..."

aws dynamodb wait table-exists \
  --table-name "$TABELA"

echo ""
echo "Tabela criada com sucesso."

# -------------------------------------------------------
# IDs utilizados na POC
# -------------------------------------------------------

ID_OPERACAO="OP-000001"
ID_REGISTRO="REG-000001"
ID_EXTERNO="CLEARING-000001"

echo ""
echo "=========================================="
echo " Inserindo cenário da POC"
echo "=========================================="

# -------------------------------------------------------
# OPERAÇÃO
# -------------------------------------------------------

echo ""
echo "3. Inserindo OPERACAO..."

aws dynamodb put-item \
  --table-name "$TABELA" \
  --item '{
    "chaveParticao": {
      "S": "OPERACAO#OP-000001"
    },
    "chaveOrdenacao": {
      "S": "METADADOS"
    },
    "tipoEntidade": {
      "S": "OPERACAO"
    },
    "idOperacao": {
      "S": "OP-000001"
    },
    "tipoOperacao": {
      "S": "APLICACAO"
    },
    "produto": {
      "S": "RDB"
    },
    "valor": {
      "N": "15000.50"
    },
    "dataOperacao": {
      "S": "2026-10-01"
    },
    "tipoOrigem": {
      "S": "ARQUIVO"
    },
    "idOperacaoOrigem": {
      "S": "OPERACAO-SISTEMA-987654"
    },
    "chaveIdempotencia": {
      "S": "SISTEMA_ORIGEM#OPERACAO-SISTEMA-987654"
    },
    "dadosProduto": {
      "M": {
        "numeroRdb": {
          "S": "RDB-987654"
        },
        "indexador": {
          "S": "CDI"
        },
        "taxa": {
          "N": "102.5"
        },
        "dataVencimento": {
          "S": "2027-10-01"
        }
      }
    },
    "dataHoraCriacao": {
      "S": "2026-10-01T10:00:00Z"
    }
  }'

# -------------------------------------------------------
# REGISTRO CLEARING
# -------------------------------------------------------

echo ""
echo "4. Inserindo REGISTRO_CLEARING..."

aws dynamodb put-item \
  --table-name "$TABELA" \
  --item '{
    "chaveParticao": {
      "S": "OPERACAO#OP-000001"
    },
    "chaveOrdenacao": {
      "S": "REGISTRO#REG-000001"
    },
    "tipoEntidade": {
      "S": "REGISTRO_CLEARING"
    },
    "idOperacao": {
      "S": "OP-000001"
    },
    "idRegistro": {
      "S": "REG-000001"
    },
    "clearing": {
      "S": "B3"
    },
    "idExterno": {
      "S": "CLEARING-000001"
    },
    "status": {
      "S": "ACEITO"
    },
    "tentativa": {
      "N": "1"
    },
    "protocoloExterno": {
      "S": "B3-PROT-987654"
    },
    "chaveParticaoIndiceIdExterno": {
      "S": "ID_EXTERNO#CLEARING-000001"
    },
    "chaveOrdenacaoIndiceIdExterno": {
      "S": "REGISTRO#REG-000001"
    },
    "dataHoraEnvio": {
      "S": "2026-10-01T10:01:00Z"
    },
    "dataHoraRetorno": {
      "S": "2026-10-01T10:05:00Z"
    },
    "dataHoraAtualizacao": {
      "S": "2026-10-01T10:05:00Z"
    }
  }'

# -------------------------------------------------------
# EVENTO 1
# -------------------------------------------------------

echo ""
echo "5. Inserindo evento REGISTRO_CRIADO..."

aws dynamodb put-item \
  --table-name "$TABELA" \
  --item '{
    "chaveParticao": {
      "S": "OPERACAO#OP-000001"
    },
    "chaveOrdenacao": {
      "S": "REGISTRO#REG-000001#EVENTO#20261001T100000.000Z#EVT-001"
    },
    "tipoEntidade": {
      "S": "EVENTO_REGISTRO"
    },
    "idRegistro": {
      "S": "REG-000001"
    },
    "tipoEvento": {
      "S": "REGISTRO_CRIADO"
    },
    "statusAtual": {
      "S": "PENDENTE"
    },
    "dataHoraEvento": {
      "S": "2026-10-01T10:00:00Z"
    }
  }'

# -------------------------------------------------------
# EVENTO 2
# -------------------------------------------------------

echo ""
echo "6. Inserindo evento ENVIO_REALIZADO..."

aws dynamodb put-item \
  --table-name "$TABELA" \
  --item '{
    "chaveParticao": {
      "S": "OPERACAO#OP-000001"
    },
    "chaveOrdenacao": {
      "S": "REGISTRO#REG-000001#EVENTO#20261001T100100.000Z#EVT-002"
    },
    "tipoEntidade": {
      "S": "EVENTO_REGISTRO"
    },
    "idRegistro": {
      "S": "REG-000001"
    },
    "tipoEvento": {
      "S": "ENVIO_REALIZADO"
    },
    "statusAnterior": {
      "S": "PENDENTE"
    },
    "statusAtual": {
      "S": "ENVIADO"
    },
    "dataHoraEvento": {
      "S": "2026-10-01T10:01:00Z"
    }
  }'

# -------------------------------------------------------
# EVENTO 3
# -------------------------------------------------------

echo ""
echo "7. Inserindo evento RETORNO_RECEBIDO..."

aws dynamodb put-item \
  --table-name "$TABELA" \
  --item '{
    "chaveParticao": {
      "S": "OPERACAO#OP-000001"
    },
    "chaveOrdenacao": {
      "S": "REGISTRO#REG-000001#EVENTO#20261001T100500.000Z#EVT-003"
    },
    "tipoEntidade": {
      "S": "EVENTO_REGISTRO"
    },
    "idRegistro": {
      "S": "REG-000001"
    },
    "tipoEvento": {
      "S": "RETORNO_RECEBIDO"
    },
    "origemEvento": {
      "S": "PISMO"
    },
    "idEventoExterno": {
      "S": "PISMO-EVT-789"
    },
    "dataHoraEvento": {
      "S": "2026-10-01T10:05:00Z"
    }
  }'

# -------------------------------------------------------
# EVENTO 4
# -------------------------------------------------------

echo ""
echo "8. Inserindo evento STATUS_ALTERADO..."

aws dynamodb put-item \
  --table-name "$TABELA" \
  --item '{
    "chaveParticao": {
      "S": "OPERACAO#OP-000001"
    },
    "chaveOrdenacao": {
      "S": "REGISTRO#REG-000001#EVENTO#20261001T100501.000Z#EVT-004"
    },
    "tipoEntidade": {
      "S": "EVENTO_REGISTRO"
    },
    "idRegistro": {
      "S": "REG-000001"
    },
    "tipoEvento": {
      "S": "STATUS_ALTERADO"
    },
    "statusAnterior": {
      "S": "ENVIADO"
    },
    "statusAtual": {
      "S": "ACEITO"
    },
    "protocoloExterno": {
      "S": "B3-PROT-987654"
    },
    "dataHoraEvento": {
      "S": "2026-10-01T10:05:01Z"
    }
  }'

echo ""
echo "=========================================="
echo " POC criada com sucesso!"
echo "=========================================="

echo ""
echo "Estrutura:"
echo ""
echo "OPERACAO#OP-000001"
echo " |"
echo " +-- METADADOS"
echo " |"
echo " +-- REGISTRO#REG-000001"
echo " |"
echo " +-- EVENTO REGISTRO_CRIADO"
echo " +-- EVENTO ENVIO_REALIZADO"
echo " +-- EVENTO RETORNO_RECEBIDO"
echo " +-- EVENTO STATUS_ALTERADO"
echo ""

echo "Consultando operação completa..."

aws dynamodb query \
  --table-name "$TABELA" \
  --key-condition-expression "chaveParticao = :pk" \
  --expression-attribute-values '{
    ":pk": {
      "S": "OPERACAO#OP-000001"
    }
  }'

echo ""
echo "Consultando pelo ID externo através do GSI..."

aws dynamodb query \
  --table-name "$TABELA" \
  --index-name "INDICE_ID_EXTERNO" \
  --key-condition-expression "chaveParticaoIndiceIdExterno = :pk" \
  --expression-attribute-values '{
    ":pk": {
      "S": "ID_EXTERNO#CLEARING-000001"
    }
  }'

echo ""
echo "Fim da POC."