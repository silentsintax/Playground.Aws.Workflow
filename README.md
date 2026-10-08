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

TÍTULO

H02 - Implementar ingestão de operações financeiras via AWS Batch com adaptação para o modelo canônico e publicação no Amazon SQS

DESCRIÇÃO

Como plataforma de registro de operações financeiras em múltiplas clearings, precisamos implementar um processo de ingestão em lote utilizando AWS Batch, responsável por receber, interpretar, validar, transformar e publicar operações financeiras provenientes de arquivos disponibilizados no Amazon S3.

O processo deverá suportar inicialmente arquivos CSV contendo operações de aplicações e resgates de produtos financeiros, começando pelo produto RDB, com possibilidade de expansão para CDB e outros produtos.

A volumetria esperada é de aproximadamente 2 a 3 milhões de operações por dia, podendo existir mais de um arquivo no mesmo dia.

O processamento deverá ser desenvolvido em .NET 8 ou versão homologada pelo projeto, executado em contêiner Docker no AWS Batch.

A solução deverá permitir a inclusão de novos produtos e formatos de entrada sem necessidade de alterar o fluxo principal de ingestão.

As operações deverão ser transformadas para o modelo canônico definido pelo projeto e publicadas em uma fila Amazon SQS destinada à ingestão de operações.

O AWS Batch não será responsável pela persistência direta das operações no DynamoDB.

A responsabilidade de consumir as mensagens da fila, validar a idempotência de persistência e gravar os registros nas tabelas OPERACOES e REGISTROS_CLEARING será de um componente consumidor independente, contemplado em história específica.

O fluxo de registro nas clearings permanecerá desacoplado da ingestão, sendo iniciado após a persistência das operações, conforme a arquitetura definida para o projeto.

OBJETIVO

Implementar um processo robusto e escalável de ingestão de arquivos financeiros que permita interpretar diferentes layouts, transformar registros para um modelo canônico e publicar as operações no Amazon SQS com segurança, rastreabilidade e mecanismos de recuperação.

A solução deverá garantir que falhas de publicação sejam identificadas e tratadas, evitando perda silenciosa de operações.

ARQUITETURA PROPOSTA

Fluxo de ingestão:

Amazon S3
→ Evento de criação do arquivo
→ EventBridge / mecanismo de orquestração
→ AWS Batch
→ Leitura streaming do CSV
→ Parser específico do layout
→ Adaptador do produto
→ Validação da operação
→ Modelo canônico
→ Publicação em lote no Amazon SQS

Fluxo posterior, fora do escopo desta história:

Amazon SQS de ingestão
→ Lambda ou serviço consumidor
→ Validação de idempotência
→ DynamoDB OPERACOES
→ DynamoDB REGISTROS_CLEARING
→ DynamoDB Streams
→ EventBridge Pipes
→ SQS de registro
→ Worker de integração com a clearing

A fila de ingestão deverá ser distinta da fila utilizada para o envio de operações às clearings.

ESCOPO FUNCIONAL

1. RECEBIMENTO E IDENTIFICAÇÃO DOS ARQUIVOS

O processo deverá ser iniciado a partir da disponibilização de um arquivo CSV em um bucket S3 previamente configurado.

O evento de criação do objeto deverá acionar o mecanismo de orquestração responsável por iniciar o job do AWS Batch.

O job deverá receber, no mínimo:

- Bucket de origem.
- Chave do objeto no S3.
- Identificador único do arquivo.
- Tipo de produto financeiro.
- Versão do layout.
- Data de referência do processamento.
- Identificador de correlação da execução.

O identificador do arquivo deverá permitir reconhecer reprocessamentos e relacionar as operações à sua origem.

A identificação do objeto deverá considerar mecanismos que permitam distinguir versões diferentes de um arquivo quando necessário.

O sistema deverá validar a existência e a acessibilidade do arquivo antes de iniciar sua leitura.

Arquivos inexistentes, inacessíveis, vazios ou com layout incompatível deverão gerar falha controlada.

2. LEITURA EFICIENTE DO CSV

A leitura deverá ocorrer em streaming, evitando carregar todo o arquivo em memória.

O processo deverá suportar arquivos contendo aproximadamente 2 a 3 milhões de registros e permitir crescimento futuro da volumetria.

A implementação deverá utilizar um parser CSV compatível com o layout recebido, respeitando corretamente delimitadores, cabeçalhos, campos opcionais, campos entre aspas, caracteres especiais e codificação.

A leitura deverá ser incremental.

O processamento não poderá depender do carregamento integral do arquivo em memória.

Deverão ser identificadas e tratadas situações como:

- Arquivo sem cabeçalho obrigatório.
- Colunas ausentes.
- Quantidade inválida de colunas.
- Campos obrigatórios vazios.
- Valores numéricos inválidos.
- Datas inválidas.
- Linhas malformadas.
- Codificação incompatível.
- Arquivo interrompido ou corrompido.

Erros isolados de conteúdo não deverão necessariamente interromper o processamento completo, desde que seja possível identificar e isolar a linha inválida.

Erros estruturais que impeçam a interpretação confiável do arquivo deverão interromper o processamento.

3. ARQUITETURA DE ADAPTADORES POR PRODUTO

A implementação deverá separar a leitura física do arquivo da interpretação de negócio dos registros.

O fluxo principal do AWS Batch não deverá conter regras específicas de RDB, CDB ou de qualquer outro produto financeiro.

Deverá existir um mecanismo de resolução de adaptadores com base no produto e na versão do layout.

Cada adaptador será responsável por interpretar os campos específicos do produto e transformá-los para o modelo canônico.

Inicialmente, deverá ser implementado o adaptador para RDB.

A arquitetura deverá permitir a inclusão posterior de adaptadores para CDB e outros produtos, sem necessidade de alterar o mecanismo principal de leitura ou publicação.

Responsabilidades sugeridas:

Parser: interpreta a estrutura física do CSV e produz um registro de entrada.

Adapter: transforma o registro específico do produto em uma operação canônica.

Validator: valida as regras estruturais e de negócio aplicáveis à operação.

SqsPublisher: publica as operações canônicas no Amazon SQS.

ProcessingCoordinator: controla leitura, concorrência, contabilização, publicação e tratamento de falhas.

4. MODELO CANÔNICO

Toda operação válida deverá ser convertida para o modelo canônico definido para a plataforma.

O modelo deverá conter, no mínimo:

idOperacao: identificador único da operação.

produto: identificação do produto financeiro, inicialmente RDB.

tipoOperacao: APLICACAO ou RESGATE.

clearingDestino: identificação da clearing responsável pelo registro.

chaveIdempotencia: identificador determinístico utilizado para prevenir processamento duplicado.

versaoModelo: versão do contrato canônico.

origem: estrutura contendo o tipo da origem e o identificador do arquivo.

dataHoraInclusao: data e hora de criação da operação no contexto da ingestão.

dadosOperacao: estrutura flexível contendo os atributos específicos do produto.

O contrato da mensagem deverá incluir os metadados necessários para rastreabilidade e correlação.

A operação deverá possuir apenas uma clearing de destino.

O modelo deverá ser versionado para permitir evolução sem quebra de compatibilidade com consumidores existentes.

O Batch deverá produzir mensagens compatíveis com o contrato esperado pelo consumidor da fila.

5. VALIDAÇÃO DAS OPERAÇÕES

As validações deverão ocorrer antes da publicação no SQS.

Deverão existir validações estruturais comuns a todos os produtos e validações específicas implementadas pelo respectivo adaptador ou validador.

As validações comuns deverão contemplar, quando aplicáveis:

- Presença de identificadores obrigatórios.
- Produto reconhecido.
- Tipo de operação permitido.
- Clearing de destino informada e suportada.
- Datas em formato válido.
- Valores monetários em formato válido.
- Identificação da origem.
- Versão do modelo suportada.
- Chave de idempotência válida.

As validações específicas de RDB deverão ser definidas conforme o contrato de entrada homologado com a área de negócio.

O sistema não deverá presumir regras financeiras que não tenham sido formalmente aprovadas.

Registros inválidos não deverão ser publicados na fila de ingestão.

Esses registros deverão ser contabilizados e associados a um motivo de rejeição.

O processo deverá permitir distinguir erros de validação, erros de transformação e falhas técnicas de publicação.

6. IDENTIFICAÇÃO E IDEMPOTÊNCIA

O processo deverá ser seguro para reexecução.

O reprocessamento de um arquivo poderá resultar na republicação de mensagens já enviadas anteriormente, especialmente em cenários de falha parcial ou interrupção do job.

Por esse motivo, cada operação deverá possuir uma chave de idempotência estável e determinística, conforme as regras de identificação de negócio.

A chave deverá permanecer igual para a mesma operação, independentemente da execução do Batch.

O identificador de execução não deverá compor a identidade de negócio de maneira que gere uma nova chave a cada reprocessamento.

O AWS Batch não deverá depender de consultas ao DynamoDB para verificar a existência prévia da operação.

A deduplicação definitiva deverá ser realizada pelo consumidor responsável pela persistência.

A solução deverá assumir que mensagens podem ser entregues mais de uma vez pelo SQS.

Caso seja utilizada uma fila SQS Standard, não haverá garantia de ordenação global ou de entrega exatamente uma vez.

A estratégia de idempotência deverá considerar explicitamente essas características.

7. PUBLICAÇÃO NO AMAZON SQS

O AWS Batch deverá publicar as operações canônicas em uma fila Amazon SQS específica para ingestão.

A publicação deverá utilizar o AWS SDK for .NET.

Deverá ser priorizado o uso de SendMessageBatch para reduzir a quantidade de chamadas à API e melhorar a eficiência.

Cada chamada SendMessageBatch poderá conter até dez mensagens, respeitando os limites de tamanho da operação e das mensagens.

A implementação deverá validar o tamanho do payload antes da publicação.

O processo não deverá tentar publicar mensagens acima do limite permitido pelo SQS.

A estrutura da mensagem deverá conter os dados canônicos necessários para a persistência posterior, evitando a necessidade de o consumidor reler o CSV original.

Metadados de correlação poderão ser transmitidos por atributos de mensagem ou pelo envelope, respeitando os limites do serviço.

O publisher deverá tratar individualmente o resultado de cada mensagem enviada em lote.

Uma resposta HTTP de sucesso para SendMessageBatch não significa necessariamente que todas as mensagens do lote foram aceitas.

A implementação deverá verificar as coleções de mensagens aceitas e rejeitadas retornadas pela API.

Somente mensagens confirmadas como aceitas poderão ser contabilizadas como publicadas.

Mensagens rejeitadas deverão ser submetidas à política de retry ou registradas como falha definitiva.

8. CONTROLE DE CONCORRÊNCIA E THROUGHPUT

O processo deverá permitir configuração do número máximo de operações em transformação e publicação simultânea.

A implementação deverá utilizar concorrência controlada e buffers com capacidade limitada.

O processamento não poderá acumular milhões de objetos em memória enquanto aguarda a publicação.

O número de workers, o tamanho dos lotes, a quantidade de requisições concorrentes e os limites de processamento deverão ser configuráveis por ambiente.

O processo deverá aplicar backpressure quando houver aumento de latência, throttling ou falhas na publicação para o SQS.

Deverão ser implementadas tentativas com exponential backoff e jitter para falhas transitórias.

O objetivo de throughput deverá ser validado em testes de carga com 2 e 3 milhões de operações.

Como referência, publicar 3 milhões de mensagens em duas horas exige uma média aproximada de 417 mensagens por segundo.

Com lotes completos de dez mensagens, isso representa aproximadamente 42 chamadas SendMessageBatch por segundo, desconsiderando retries e mensagens rejeitadas.

A capacidade do consumidor e o crescimento do backlog da fila deverão ser avaliados em conjunto com a taxa de publicação.

O Batch não deverá assumir que a aceitação das mensagens pelo SQS significa que o processamento completo da operação foi concluído.

9. TRATAMENTO DE FALHAS NA PUBLICAÇÃO

O processo deverá distinguir:

Falhas de leitura do S3.

Falhas estruturais do arquivo.

Falhas de validação.

Falhas de transformação para o modelo canônico.

Falhas transitórias do SQS.

Rejeições individuais em SendMessageBatch.

Falhas permanentes de publicação.

Falhas de infraestrutura ou interrupção do job.

Falhas transitórias deverão ser submetidas a tentativas controladas.

Não deverão existir retries indefinidos.

A implementação deverá considerar que uma chamada pode ter sido aceita pelo SQS mesmo quando o cliente não recebeu a confirmação devido a uma falha de comunicação.

Nesses casos, a repetição da publicação poderá produzir mensagens duplicadas.

A recuperação deverá preservar a chave de idempotência original.

Falhas permanentes de publicação não poderão ser contabilizadas como sucesso.

O job deverá finalizar com falha ou estado de execução parcialmente concluída, conforme a política formalmente definida, caso existam operações válidas que não tenham sido publicadas após as tentativas permitidas.

10. REPROCESSAMENTO E RECUPERAÇÃO

O processo deverá permitir reprocessamento integral do arquivo.

O reprocessamento deverá utilizar as mesmas regras de identificação e transformação aplicadas à execução original.

Operações já publicadas poderão ser republicadas.

A solução deverá depender da idempotência do consumidor para impedir persistência duplicada e registros financeiros duplicados.

Deverá existir mecanismo para identificar a execução original e suas tentativas posteriores.

A necessidade de checkpoints por linha ou por bloco deverá ser avaliada durante a POC.

Não deverá ser implementado um mecanismo complexo de checkpoint sem evidência de necessidade.

O processo deverá permitir recuperação após falhas ocorridas depois da publicação parcial do arquivo.

11. RASTREABILIDADE DO ARQUIVO

Cada execução deverá possuir um identificador único.

O processamento deverá registrar, no mínimo:

Identificador da execução.

Identificador do arquivo.

Bucket e chave do objeto.

Produto.

Versão do layout.

Data de referência.

Horário de início.

Horário de término.

Quantidade total de linhas lidas.

Quantidade de operações válidas.

Quantidade de operações publicadas com confirmação de aceite pelo SQS.

Quantidade de registros rejeitados por validação.

Quantidade de falhas de transformação.

Quantidade de falhas de publicação.

Quantidade de mensagens submetidas a retry.

Quantidade de operações não publicadas após esgotamento das tentativas.

Status final da execução.

Os totais deverão permitir conciliação entre o conteúdo do arquivo e o resultado da publicação.

12. RELATÓRIO DE PROCESSAMENTO

Ao final de cada execução, deverá ser produzido um relatório contendo o resultado da ingestão.

O relatório deverá apresentar os totais de linhas processadas, operações válidas, mensagens publicadas, rejeições e falhas técnicas.

As inconsistências deverão possuir informações suficientes para identificação da linha e da causa do problema.

O armazenamento de arquivos de rejeição no S3 poderá ser utilizado conforme política de segurança e retenção definida pelo projeto.

O relatório deverá distinguir claramente:

Operação publicada no SQS.

Operação persistida no DynamoDB.

Operação enviada à clearing.

Operação registrada na clearing.

Esta história será responsável apenas pela confirmação da primeira etapa.

Os demais estados pertencem aos componentes posteriores do fluxo.

13. OBSERVABILIDADE

A solução deverá disponibilizar logs estruturados e métricas operacionais.

Os logs deverão conter identificadores de correlação, arquivo, execução e operação, quando aplicável.

Deverão ser disponibilizadas métricas para:

Linhas lidas por segundo.

Operações transformadas por segundo.

Mensagens publicadas por segundo.

Quantidade de chamadas SendMessageBatch.

Quantidade de mensagens por lote.

Tempo médio e percentis de publicação.

Quantidade de mensagens rejeitadas.

Quantidade de retries.

Throttling do SQS.

Tempo total de execução.

Utilização de CPU e memória do job.

Deverá ser possível correlacionar uma execução do Batch com as mensagens publicadas.

A solução deverá permitir integração com as ferramentas de observabilidade adotadas pela empresa.

Não deverão ser gerados logs individuais de sucesso para cada operação em nível INFO, salvo necessidade justificada, evitando custos elevados de observabilidade.

14. SEGURANÇA

O job deverá utilizar IAM Role com princípio do menor privilégio.

As permissões deverão limitar o acesso ao bucket S3 de origem e à fila SQS de ingestão.

O AWS Batch não deverá receber permissões de escrita no DynamoDB, salvo outra necessidade explicitamente aprovada e fora deste escopo.

Não deverão existir credenciais AWS fixas no código ou na imagem Docker.

A comunicação com os serviços AWS deverá utilizar conexões seguras.

A configuração deverá ser fornecida por variáveis de ambiente, parâmetros gerenciados ou mecanismos equivalentes.

A solução deverá respeitar as políticas de criptografia, proteção de dados e retenção estabelecidas pela empresa.

15. INFRAESTRUTURA AWS

A infraestrutura deverá contemplar:

Definição do AWS Batch Job Definition.

Ambiente computacional adequado à execução do contêiner.

Fila de jobs do AWS Batch.

Imagem Docker armazenada no Amazon ECR.

IAM Roles e políticas necessárias.

Integração com o bucket S3 de entrada.

Mecanismo de acionamento a partir do evento de criação do objeto.

Fila SQS de ingestão de operações.

Configuração de logs.

Parâmetros de CPU, memória, concorrência, timeout e retries.

Configurações de rede necessárias ao acesso ao S3 e SQS.

A fila deverá possuir política de retenção compatível com o tempo máximo esperado de indisponibilidade dos consumidores e com a estratégia de recuperação.

A necessidade de uma DLQ para a fila de ingestão deverá ser tratada na história do consumidor, incluindo política de redrive.

A infraestrutura definitiva deverá ser implementada por Terraform, seguindo os padrões adotados pela empresa.

16. TESTES

Deverão ser realizados testes unitários, de integração e de carga.

Os testes deverão contemplar:

Leitura de CSV válido.

Leitura de arquivo vazio.

Layout inválido.

Campos obrigatórios ausentes.

Valores monetários inválidos.

Datas inválidas.

Produto não suportado.

Transformação correta de RDB.

Geração do modelo canônico.

Geração estável da chave de idempotência.

Publicação de mensagem válida no SQS.

Publicação em lote com até dez mensagens.

Falha parcial de SendMessageBatch.

Throttling do SQS.

Timeout na publicação.

Retry com backoff.

Reprocessamento integral do arquivo.

Interrupção do Batch após publicação parcial.

Consumo de memória durante processamento de arquivos grandes.

Teste de publicação de 2 milhões de operações.

Teste de publicação de 3 milhões de operações.

17. FORA DO ESCOPO

Não fazem parte desta história:

Persistência direta das operações no DynamoDB.

Implementação do consumidor SQS responsável por criar OPERACOES e REGISTROS_CLEARING.

Criação ou atualização de EVENTOS_REGISTRO.

Envio das operações para a clearing.

Processamento de retornos da clearing.

Atualização posterior de status de registro.

Conciliação financeira com a clearing.

Implementação da futura ingestão via Kafka.

Consultas operacionais e relatórios sobre as operações persistidas.

CRITÉRIOS DE ACEITE

CA01 - O AWS Batch deverá ser iniciado a partir de um arquivo disponibilizado no S3, por meio do mecanismo de orquestração definido.

CA02 - O job deverá receber os parâmetros necessários para identificar arquivo, produto, versão de layout e execução.

CA03 - A leitura do arquivo deverá ocorrer em streaming, sem carregamento integral em memória.

CA04 - A solução deverá processar corretamente arquivos CSV válidos do produto RDB.

CA05 - O parser deverá identificar arquivos estruturalmente inválidos e produzir diagnóstico apropriado.

CA06 - O fluxo principal de ingestão deverá ser independente das regras específicas de cada produto financeiro.

CA07 - A implementação deverá utilizar adaptadores para transformação dos registros de entrada em operações canônicas.

CA08 - A inclusão futura de novos adaptadores não deverá exigir alteração do fluxo principal de leitura e publicação.

CA09 - Toda operação válida deverá ser transformada para o contrato canônico homologado.

CA10 - O contrato canônico deverá conter produto, tipo de operação, clearing de destino, identificadores, origem, versão e dados específicos.

CA11 - O contrato deverá possuir mecanismo explícito de versionamento.

CA12 - As validações comuns e específicas do produto deverão ocorrer antes da publicação.

CA13 - Registros inválidos não deverão ser publicados na fila SQS de ingestão.

CA14 - Registros rejeitados deverão ser contabilizados e possuir motivo identificável.

CA15 - O AWS Batch deverá publicar as operações canônicas em uma fila Amazon SQS específica para ingestão.

CA16 - O AWS Batch não deverá realizar persistência direta nas tabelas DynamoDB.

CA17 - A publicação deverá utilizar o AWS SDK for .NET.

CA18 - A solução deverá suportar publicação em lotes por SendMessageBatch.

CA19 - O publisher deverá respeitar os limites de quantidade e tamanho de mensagens estabelecidos pelo SQS.

CA20 - O publisher deverá verificar individualmente o resultado das mensagens em SendMessageBatch.

CA21 - Mensagens rejeitadas em um lote não poderão ser contabilizadas como publicadas.

CA22 - Falhas transitórias deverão utilizar retries controlados com exponential backoff e jitter.

CA23 - O processo deverá possuir limite configurável de tentativas de publicação.

CA24 - Falhas definitivas de publicação deverão ser registradas e refletidas no resultado da execução.

CA25 - A solução deverá permitir configurar a concorrência e o tamanho dos lotes de publicação.

CA26 - A implementação deverá utilizar buffers limitados e mecanismos de backpressure para evitar consumo excessivo de memória.

CA27 - A chave de idempotência de uma operação deverá permanecer estável entre execuções e reprocessamentos do arquivo.

CA28 - O reprocessamento de um arquivo deverá ser suportado sem alteração indevida da identidade de negócio das operações.

CA29 - A solução deverá admitir a possibilidade de mensagens duplicadas, sem assumir garantia de entrega exatamente uma vez pelo SQS.

CA30 - A responsabilidade pela deduplicação definitiva deverá pertencer ao consumidor de persistência.

CA31 - O AWS Batch deverá permitir rastrear cada mensagem até seu arquivo e execução de origem.

CA32 - A execução deverá registrar totais de linhas lidas, operações válidas, mensagens publicadas, rejeições e falhas.

CA33 - O processo deverá produzir relatório final de ingestão com informações suficientes para conciliação dos registros do arquivo.

CA34 - O relatório deverá distinguir mensagens aceitas pelo SQS de operações efetivamente persistidas no DynamoDB.

CA35 - O job não poderá ser considerado integralmente bem-sucedido caso existam operações válidas não publicadas após esgotamento das tentativas.

CA36 - O processo deverá disponibilizar logs estruturados e métricas de throughput, latência, retries, falhas e utilização de recursos.

CA37 - Os logs não deverão expor indiscriminadamente dados sensíveis das operações financeiras.

CA38 - O job deverá utilizar IAM Role com permissões restritas aos recursos necessários.

CA39 - O código e a imagem Docker não deverão conter credenciais AWS fixas.

CA40 - A infraestrutura deverá ser provisionada por Terraform conforme os padrões da empresa.

CA41 - Deverá existir imagem Docker versionada e publicada no Amazon ECR.

CA42 - A implementação deverá possuir testes unitários para parser, adaptadores, validadores e publisher.

CA43 - Deverão existir testes de integração para publicação no SQS, incluindo falhas parciais de SendMessageBatch.

CA44 - Deverá ser validado o comportamento do processo em situações de throttling, timeout e falhas transitórias do SQS.

CA45 - Deverá ser validado o reprocessamento de um arquivo após interrupção com publicação parcial.

CA46 - Deverá ser realizado teste de carga com 2 milhões de operações.

CA47 - Deverá ser realizado teste de carga com 3 milhões de operações.

CA48 - O teste de carga deverá registrar throughput, duração total, consumo de CPU, memória, latência e quantidade de retries.

CA49 - A solução deverá demonstrar que consegue processar o volume esperado dentro da janela de ingestão definida pelo projeto, quando essa janela for homologada.

CA50 - O fluxo de ingestão deverá permanecer desacoplado do fluxo de persistência e registro nas clearings.

CA51 - A publicação bem-sucedida no SQS deverá representar exclusivamente a confirmação de aceite da mensagem pela fila, não a confirmação de persistência ou registro financeiro.

CA52 - A implementação deverá possuir documentação técnica de execução, configuração, monitoramento, tratamento de falhas e reprocessamento.

DEFINIÇÃO DE PRONTO

A história será considerada concluída quando o AWS Batch conseguir receber um arquivo CSV do S3, interpretar seus registros, aplicar o adaptador RDB, validar as operações, gerar o modelo canônico e publicar todas as operações válidas no SQS, com rastreabilidade e tratamento adequado de falhas.

A solução deverá possuir testes automatizados, infraestrutura provisionada, observabilidade operacional e evidências de teste de carga.

A persistência no DynamoDB e o registro efetivo nas clearings não serão requisitos para conclusão desta história, pois pertencem a etapas posteriores da arquitetura.