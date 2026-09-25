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

Processar arquivo de operações do S3 e persistir no DynamoDB utilizando AWS Batch

# Objetivo

Implementar o processamento de arquivos CSV contendo operações de aplicações e resgates disponibilizados em um bucket S3.

O processamento deverá ser executado utilizando AWS Batch, permitindo o tratamento de arquivos de grande volume, atualmente estimados entre **20 e 30 milhões de registros por arquivo**.

Cada registro válido deverá ser transformado em uma operação e persistido na tabela de operações do DynamoDB.

O escopo desta história termina após a persistência da operação no DynamoDB. O processamento de eventos do DynamoDB Streams e o posterior envio das operações para as clearings não fazem parte desta história.

# Descrição detalhada

Um arquivo CSV contendo operações financeiras será disponibilizado em um bucket Amazon S3.

A criação do arquivo deverá gerar um evento que iniciará o processamento através do AWS Batch.

O arquivo poderá conter aproximadamente **20 milhões de registros, podendo atingir cerca de 30 milhões de registros**, sendo normalmente disponibilizado uma vez ao dia, eventualmente duas vezes no mesmo dia.

O processamento deverá ser realizado sem a necessidade de carregar o arquivo completo em memória.

O AWS Batch deverá ler o arquivo de maneira incremental/streaming e transformar cada linha válida em uma operação do domínio.

Cada operação deverá conter as informações existentes no arquivo necessárias para identificação e processamento posterior, incluindo a identificação da clearing de destino quando essa informação estiver disponível no arquivo.

Exemplo conceitual:

```text
S3
 │
 │ CSV
 │ 20–30 milhões de registros
 ▼
EventBridge
 │
 ▼
AWS Batch
 │
 ├── leitura streaming
 ├── parsing CSV
 ├── validação estrutural
 ├── transformação
 ├── identificação da operação
 └── persistência em lote
          │
          ▼
      DynamoDB
      Operations
```

O processamento deverá suportar paralelização do arquivo quando necessário para atingir o throughput esperado. A estratégia de particionamento deverá garantir que um registro CSV não seja dividido incorretamente entre dois workers.

A implementação deverá considerar que um Batch Job pode ser interrompido ou executado novamente. Portanto, o processamento deverá ser **idempotente**, evitando a criação de operações duplicadas em caso de retry.

A identificação da operação deverá utilizar preferencialmente uma chave única proveniente do negócio. Caso o arquivo não forneça um identificador único, deverá ser definida uma estratégia determinística para geração da chave de idempotência.

A persistência no DynamoDB deverá utilizar operações em lote sempre que aplicável, respeitando os limites da API e realizando retry dos itens eventualmente não processados.

Uma falha de validação em uma linha individual não deverá interromper o processamento de todo o arquivo.

Falhas técnicas que impossibilitem a continuidade do processamento deverão resultar em falha do job ou da respectiva partição, permitindo posterior retry.

# Requisitos

**RF01 — Detecção do arquivo**

A criação de um arquivo elegível no bucket S3 deverá iniciar o fluxo responsável pela execução do AWS Batch.

**RF02 — Processamento de grandes volumes**

A solução deverá suportar arquivos contendo pelo menos **30 milhões de registros**.

**RF03 — Leitura incremental**

O arquivo deverá ser processado através de streaming ou mecanismo equivalente, não sendo permitido carregar o conteúdo completo do arquivo em memória.

**RF04 — Parsing**

Cada registro deverá ser interpretado conforme o layout oficial do CSV.

O parser deverá respeitar corretamente delimitadores, encoding, cabeçalho, campos obrigatórios e regras de escape definidas no contrato do arquivo.

**RF05 — Validação**

Cada registro deverá passar pelas validações estruturais necessárias antes da persistência.

Um registro inválido não deverá interromper o processamento dos demais registros.

**RF06 — Persistência**

Cada registro válido deverá gerar uma operação na tabela `Operations` do DynamoDB.

A operação deverá possuir, no mínimo, informações equivalentes a:

```text
OperationId
OperationType
Clearing
SourceType = FILE
SourceEventId / IdempotencyKey
FileName
ImportId
Status
CreatedAt
UpdatedAt
```

Os nomes definitivos dos atributos deverão seguir o modelo de domínio aprovado para a tabela.

**RF07 — Clearing**

Quando a clearing de destino estiver presente no arquivo, essa informação deverá ser persistida juntamente com a operação.

Exemplo:

```text
clearing = B3
```

A implementação não deverá possuir regra fixa assumindo que todas as operações pertencem à B3, pois a solução deverá suportar múltiplas clearings.

**RF08 — Idempotência**

O reprocessamento de uma linha já persistida não deverá resultar na criação de uma nova operação equivalente.

A identificação utilizada para idempotência deverá ser determinística.

**RF09 — Escrita em lote**

A aplicação deverá utilizar escrita em lote no DynamoDB sempre que aplicável.

Itens retornados como não processados pelo DynamoDB deverão possuir estratégia de retry com backoff.

**RF10 — Paralelização**

A solução deverá permitir que o processamento do arquivo seja dividido entre múltiplos workers/jobs quando necessário.

A estratégia utilizada não poderá provocar perda, duplicação ou quebra de registros devido à divisão do arquivo.

**RF11 — Tratamento de erro**

Erros referentes a registros individuais deverão ser tratados sem interromper todo o processamento.

Falhas técnicas que impeçam a continuidade do processamento deverão ser registradas e provocar falha da unidade de processamento correspondente.

**RF12 — Rastreabilidade**

Os logs deverão permitir identificar, no mínimo:

```text
ImportId
FileName
Chunk/Worker, quando aplicável
Quantidade processada
Quantidade persistida
Quantidade rejeitada
Quantidade de erros
Tempo de processamento
```

Não deverão ser registrados em log dados financeiros ou informações sensíveis desnecessárias para diagnóstico.

**RF13 — Escopo**

O processamento será considerado concluído, para esta história, após a persistência das operações no DynamoDB.

Estão explicitamente fora do escopo:

```text
DynamoDB Streams
EventBridge Pipes
SQS das clearings
Registration Worker
Envio para B3
Retorno da Pismo
Atualização do Registration/Status History
```

Esses componentes pertencem às etapas posteriores do fluxo de registro.

# Critérios de aceite

**CA01**

Dado que um arquivo CSV válido seja criado no bucket S3 configurado,

quando o evento de criação do arquivo for identificado,

então o processamento correspondente deverá ser iniciado no AWS Batch.

**CA02**

Dado um arquivo contendo operações válidas,

quando o AWS Batch processar o arquivo,

então cada operação deverá ser persistida corretamente na tabela `Operations` do DynamoDB.

**CA03**

Dado um arquivo cujo registro informe a clearing de destino,

quando o registro for persistido,

então a operação deverá manter a clearing informada no arquivo, sem assumir B3 como valor fixo.

**CA04**

Dado um arquivo de grande volume,

quando ele for processado,

então a aplicação não deverá carregar o arquivo completo em memória e deverá realizar sua leitura de forma incremental.

**CA05**

Dado um arquivo contendo pelo menos um registro inválido,

quando o registro inválido for encontrado,

então o erro deverá ser registrado e os demais registros válidos deverão continuar sendo processados.

**CA06**

Dado que uma unidade de processamento seja executada novamente após uma falha,

quando registros anteriormente persistidos forem encontrados,

então eles não deverão resultar na criação de operações duplicadas.

**CA07**

Dado que uma operação de escrita em lote no DynamoDB retorne itens não processados,

quando isso ocorrer,

então o sistema deverá realizar novas tentativas conforme a política de retry definida.

**CA08**

Dado que o processamento esteja configurado para execução paralela,

quando diferentes workers processarem partes do arquivo,

então nenhum registro poderá ser perdido ou dividido incorretamente em decorrência do particionamento.

**CA09**

Dado um arquivo representativo do volume de produção,

quando for executado o teste de carga com **30 milhões de registros**,

então todos os registros válidos deverão ser processados sem perda e o tempo total, throughput, consumo de recursos e quantidade de erros deverão ser registrados para validação da capacidade da solução.

**CA10**

Dado que o AWS Batch tenha persistido uma operação com sucesso no DynamoDB,

então nenhuma chamada para B3, Pismo ou qualquer outra clearing deverá ser realizada por esse componente.