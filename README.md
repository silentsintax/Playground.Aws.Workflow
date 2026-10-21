TÍTULO

H04 - Implementar consumidor de operações do Amazon SQS com persistência transacional no DynamoDB

DESCRIÇÃO

Como plataforma de registro de operações financeiras em múltiplas clearings, precisamos implementar um componente responsável por consumir operações financeiras disponibilizadas em uma fila Amazon SQS e realizar sua persistência no Amazon DynamoDB.

As mensagens serão publicadas inicialmente pelo processo de ingestão via AWS Batch, responsável pela leitura dos arquivos CSV, validação e transformação dos registros para o modelo canônico.

O consumidor deverá receber essas mensagens, validar sua estrutura, aplicar os mecanismos de idempotência e persistir as informações nas tabelas OPERACOES e REGISTROS_CLEARING.

A criação dos registros deverá ocorrer de maneira transacional, garantindo que a operação financeira e seu registro inicial de clearing sejam persistidos conjuntamente.

A solução deverá suportar uma volumetria de aproximadamente 2 a 3 milhões de operações por dia, considerando picos de processamento decorrentes da ingestão de arquivos.

O componente deverá ser implementado utilizando AWS Lambda e .NET 8, ou versão homologada pelo projeto.

O processamento deverá ser resiliente a falhas, mensagens duplicadas, entregas fora de ordem, reprocessamentos e indisponibilidades temporárias do DynamoDB.

Após a persistência bem-sucedida, o DynamoDB Streams da tabela OPERACOES será responsável por disponibilizar o evento de criação para o fluxo posterior de registro na clearing.

OBJETIVO

Implementar um consumidor desacoplado e escalável que permita persistir operações financeiras recebidas pelo Amazon SQS, assegurando consistência transacional, idempotência, rastreabilidade e capacidade de recuperação.

ARQUITETURA

Fluxo de responsabilidade desta história:

AWS Batch
→ Amazon SQS de ingestão
→ AWS Lambda .NET
→ Validação da mensagem
→ Verificação de idempotência
→ DynamoDB TransactWriteItems
→ OPERACOES + REGISTROS_CLEARING

Fluxo posterior, fora do escopo:

DynamoDB Streams de OPERACOES
→ EventBridge Pipes
→ SQS de registro por clearing
→ Worker de registro
→ API externa da clearing

A fila de ingestão deverá ser distinta das filas de integração com as clearings.

ESCOPO FUNCIONAL

1. INTEGRAÇÃO COM O AMAZON SQS

Implementar uma AWS Lambda acionada por eventos de uma fila SQS destinada à ingestão de operações financeiras.

A Lambda deverá utilizar Event Source Mapping para receber lotes de mensagens.

A quantidade de mensagens por lote deverá ser configurável, respeitando os limites suportados pelo serviço.

O consumidor deverá processar cada mensagem de forma independente, garantindo que uma falha isolada não obrigue o reprocessamento de mensagens concluídas com sucesso.

A integração deverá utilizar a funcionalidade ReportBatchItemFailures para informar ao serviço quais mensagens não foram processadas com sucesso.

As mensagens processadas com sucesso deverão ser removidas da fila pelo mecanismo gerenciado de integração SQS/Lambda.

O código da aplicação não deverá executar exclusão manual das mensagens, salvo decisão arquitetural específica.

A solução deverá considerar a semântica de entrega pelo menos uma vez do SQS Standard.

Não deverá existir dependência de ordenação global das mensagens.

2. CONTRATO DAS MENSAGENS

O consumidor deverá receber mensagens compatíveis com o modelo canônico definido para a plataforma.

Cada mensagem deverá conter, no mínimo:

idOperacao.

produto.

tipoOperacao.

clearingDestino.

chaveIdempotencia.

versaoModelo.

origem.

dataHoraInclusao.

dadosOperacao.

A estrutura origem deverá permitir identificar o sistema ou arquivo responsável pela geração da operação.

Exemplo:

origem.tipo = ARQUIVO

origem.identificador = ARQ-20261021-001

O consumidor deverá ser preparado para receber futuramente operações provenientes de outros mecanismos de ingestão, como Kafka, desde que utilizem o contrato canônico homologado.

O contrato deverá possuir versionamento explícito.

Mensagens com versões não suportadas deverão ser rejeitadas de forma controlada.

3. VALIDAÇÃO DAS MENSAGENS

Antes da persistência, o consumidor deverá validar a estrutura da mensagem.

Deverão ser realizadas validações como:

- JSON válido.
- Campos obrigatórios presentes.
- Identificadores válidos.
- Produto reconhecido.
- Tipo de operação permitido.
- Clearing de destino suportada.
- Versão do modelo compatível.
- Estrutura de dadosOperacao válida.
- Chave de idempotência presente.
- Tamanho dos dados compatível com os limites do DynamoDB.

O consumidor não deverá repetir integralmente todas as validações financeiras executadas pelo AWS Batch.

Entretanto, deverá validar as invariantes necessárias para proteger a integridade do banco de dados.

O consumidor não deverá confiar cegamente na origem da mensagem.

Mensagens inválidas deverão ser classificadas como falhas permanentes, evitando tentativas infinitas.

4. MAPEAMENTO PARA O DYNAMODB

O consumidor deverá converter a mensagem recebida para as estruturas persistidas nas tabelas DynamoDB.

Tabela OPERACOES

Chave primária:

PK: idOperacao

Atributos:

idOperacao.

produto.

tipoOperacao.

clearingDestino.

chaveIdempotencia.

versaoModelo.

origem.

dataHoraInclusao.

dataHoraAlteracao.

dadosOperacao.

Tabela REGISTROS_CLEARING

Chave primária:

PK: idOperacao

Atributos:

idOperacao.

idRegistro.

clearing.

status.

tentativa.

versaoModelo.

dataHoraInclusao.

dataHoraAlteracao.

dadosRegistro.

O registro inicial de clearing deverá possuir:

status = PENDENTE

tentativa = 0

clearing = clearingDestino da operação.

O idRegistro deverá ser gerado de maneira consistente com a estratégia de identificação adotada pelo projeto.

O campo idExterno não será obrigatório no momento da criação.

Os timestamps de inclusão deverão seguir uma convenção padronizada, preferencialmente UTC.

5. PERSISTÊNCIA TRANSACIONAL

A criação da operação e do registro inicial de clearing deverá ocorrer de maneira atômica.

O consumidor deverá utilizar TransactWriteItems do DynamoDB.

A transação deverá conter, no mínimo:

PutItem na tabela OPERACOES.

PutItem na tabela REGISTROS_CLEARING.

Ambas as gravações deverão utilizar condições de inexistência dos respectivos itens.

Exemplo conceitual:

Put OPERACOES
ConditionExpression: attribute_not_exists(idOperacao)

Put REGISTROS_CLEARING
ConditionExpression: attribute_not_exists(idOperacao)

O consumidor não deverá utilizar duas chamadas PutItem independentes para criar os registros.

A transação deverá assegurar que ambos os registros sejam criados ou que nenhum deles seja criado.

Falhas de transação deverão ser identificadas e classificadas.

O consumidor deverá distinguir conflitos de idempotência de erros transitórios do DynamoDB.

6. CONTROLE DE IDEMPOTÊNCIA

O processamento deverá ser idempotente.

O SQS poderá entregar a mesma mensagem mais de uma vez.

O AWS Batch também poderá republicar operações durante reprocessamentos de arquivos.

A solução deverá impedir que essas situações criem registros duplicados.

A estratégia principal deverá utilizar um identificador estável de operação, associado à identidade de negócio.

A chave idOperacao deverá ser determinística ou possuir correspondência unívoca com a chaveIdempotencia.

A implementação deverá garantir que duas mensagens representando a mesma operação de negócio não possam gerar identificadores diferentes e, consequentemente, registros duplicados.

A existência de um GSI por chaveIdempotencia não deverá ser considerada suficiente para garantir unicidade.

As condições de gravação deverão ser avaliadas atomicamente.

Quando uma operação já existir, o consumidor deverá verificar se o registro existente corresponde à mesma identidade e ao mesmo conteúdo de negócio esperado.

Se a mensagem representar uma repetição legítima de uma operação já persistida, deverá ser tratada como duplicidade idempotente.

Nesse caso, a mensagem poderá ser considerada processada com sucesso, sem sobrescrever os registros existentes.

Caso exista divergência de conteúdo para a mesma identidade, o consumidor deverá classificar a ocorrência como conflito de idempotência.

Conflitos não deverão ser tratados automaticamente como sucesso.

7. CONTROLE DE CONCORRÊNCIA

O consumidor deverá suportar múltiplas execuções simultâneas.

A concorrência da Lambda deverá ser configurável por ambiente.

O processamento deverá considerar o throughput de gravação do DynamoDB e a capacidade de consumo da fila.

A solução deverá evitar concorrência descontrolada que provoque throttling excessivo.

Deverão ser avaliados:

- Batch size do Event Source Mapping.
- Quantidade máxima de instâncias concorrentes.
- Tempo médio de processamento.
- Capacidade de escrita do DynamoDB.
- Latência das transações.
- Quantidade de mensagens acumuladas na fila.

A solução deverá permitir ajuste de concorrência sem necessidade de alteração do código.

8. TRATAMENTO DE FALHAS

As falhas deverão ser classificadas em transitórias e permanentes.

Falhas transitórias:

Throttling do DynamoDB.

Timeout.

Falhas temporárias de comunicação.

Indisponibilidade temporária de serviços AWS.

Conflitos transacionais transitórios.

Falhas permanentes:

Mensagem JSON inválida.

Campos obrigatórios ausentes.

Versão de contrato não suportada.

Produto ou clearing inválidos.

Payload incompatível com o modelo.

Conflito definitivo de idempotência.

Falhas transitórias deverão utilizar retries controlados com exponential backoff e jitter, respeitando o tempo disponível da execução Lambda.

Caso a operação não seja concluída, a mensagem deverá ser devolvida ao fluxo de reprocessamento do SQS por meio do retorno de falha parcial.

Falhas permanentes deverão seguir uma política explícita de quarentena ou encaminhamento para análise.

O consumidor não deverá simplesmente descartar mensagens inválidas.

A fila deverá possuir uma Dead Letter Queue configurada com política de redrive.

Mensagens que excederem o número máximo de tentativas deverão ser encaminhadas à DLQ.

A implementação deverá garantir que uma mensagem inválida não provoque reprocessamento indefinido.

9. PROCESSAMENTO PARCIAL DE LOTES

O consumidor deverá utilizar ReportBatchItemFailures.

Cada mensagem deverá possuir resultado independente.

Quando uma mensagem falhar, seu identificador deverá ser incluído na resposta de falha parcial.

As mensagens processadas com sucesso não deverão ser incluídas nessa resposta.

A implementação deverá tratar adequadamente exceções individuais, evitando que uma falha inesperada cause o retorno de sucesso para mensagens não processadas.

O processamento deverá garantir que uma mensagem só seja considerada bem-sucedida após confirmação da persistência transacional ou identificação comprovada de duplicidade legítima.

10. INTEGRAÇÃO COM DYNAMODB STREAMS

A criação de um novo item na tabela OPERACOES deverá produzir um evento no DynamoDB Streams, conforme a configuração definida na infraestrutura.

O consumidor não deverá publicar diretamente mensagens na fila de registro da clearing.

A responsabilidade de encaminhamento para as filas de registro continuará pertencendo ao fluxo DynamoDB Streams → EventBridge Pipes → SQS.

A implementação deverá evitar que duplicidades idempotentes gerem novas gravações na tabela OPERACOES e, consequentemente, novos eventos de criação.

O processamento posterior deverá considerar que eventos de Streams podem ser entregues mais de uma vez.

Também deverá considerar que a disponibilidade do snapshot de REGISTROS_CLEARING para outros consumidores precisa ser tratada corretamente, mesmo quando os registros forem criados na mesma transação.

11. HISTÓRICO INICIAL

Deverá ser confirmada a regra de negócio para criação do evento histórico inicial.

Caso a plataforma exija um evento de criação na tabela EVENTOS_REGISTRO, esse evento deverá ser persistido na mesma transação que cria a operação e o registro inicial.

Nesse cenário, a transação passará a conter três gravações:

Put OPERACOES.

Put REGISTROS_CLEARING.

Put EVENTOS_REGISTRO.

O evento inicial deverá possuir identificador único e tipo de evento padronizado.

A regra deverá ser única para todos os produtos e clearings.

Não deverá existir duplicidade de eventos históricos em reprocessamentos idempotentes.

12. RASTREABILIDADE

Cada mensagem deverá permitir identificar:

idOperacao.

chaveIdempotencia.

produto.

clearingDestino.

Arquivo ou sistema de origem.

Identificador de correlação.

Identificador da mensagem SQS.

Horário de recebimento.

Resultado do processamento.

O consumidor deverá produzir informações suficientes para rastrear uma operação desde a publicação no SQS até sua persistência no DynamoDB.

13. OBSERVABILIDADE

A solução deverá disponibilizar logs estruturados e métricas operacionais.

Deverão ser monitorados:

Mensagens recebidas.

Mensagens persistidas.

Duplicidades identificadas.

Conflitos de idempotência.

Mensagens inválidas.

Falhas transitórias.

Falhas permanentes.

Tentativas de retry.

Tempo médio de processamento.

Latência das transações DynamoDB.

ConsumedCapacity das operações de escrita.

Throttling do DynamoDB.

Quantidade de mensagens na fila.

Idade da mensagem mais antiga.

Mensagens encaminhadas à DLQ.

Quantidade de execuções concorrentes da Lambda.

Duração e erros da Lambda.

Os logs deverão conter identificadores de correlação.

Dados financeiros sensíveis não deverão ser registrados indiscriminadamente.

Deverão ser disponibilizados indicadores para identificar crescimento anormal do backlog e interrupções no processamento.

14. SEGURANÇA

A Lambda deverá utilizar IAM Role com princípio do menor privilégio.

As permissões deverão contemplar:

Leitura das mensagens da fila SQS.

Exclusão das mensagens pelo mecanismo de integração.

Escrita nas tabelas DynamoDB necessárias.

Leitura pontual para verificação de idempotência.

Publicação de logs e métricas.

A Lambda não deverá possuir permissões de acesso às APIs das clearings.

Não deverão existir credenciais AWS fixas no código.

A comunicação com os serviços deverá utilizar conexões seguras.

A solução deverá respeitar as políticas de criptografia, segurança e proteção de dados da empresa.

15. INFRAESTRUTURA AWS

A infraestrutura deverá contemplar:

AWS Lambda .NET.

Event Source Mapping SQS/Lambda.

Fila SQS de ingestão.

Dead Letter Queue.

Política de redrive.

IAM Role e políticas.

Configuração de memória e timeout.

Configuração de concorrência.

Configuração de batch size.

Habilitação de ReportBatchItemFailures.

Configuração de logs e métricas.

Permissões de acesso às tabelas DynamoDB.

A infraestrutura deverá ser provisionada por Terraform, seguindo os padrões adotados pela empresa.

16. TESTES

Deverão ser implementados testes unitários, de integração e de carga.

Cenários obrigatórios:

Recebimento de mensagem válida.

Desserialização do modelo canônico.

Mensagem JSON inválida.

Campo obrigatório ausente.

Versão de contrato não suportada.

Produto inválido.

Clearing inválida.

Persistência transacional bem-sucedida.

Falha na criação de OPERACOES.

Falha na criação de REGISTROS_CLEARING.

Rollback transacional.

Mensagem duplicada.

Mensagens duplicadas processadas simultaneamente.

Conflito de idempotência.

Falha temporária do DynamoDB.

Throttling.

Timeout.

Retry.

Falha parcial de lote SQS.

Encaminhamento para DLQ.

Reprocessamento de mensagem após falha.

Verificação de que duplicidades não produzem novas gravações.

Verificação da geração de eventos DynamoDB Streams para novas operações.

Teste de carga com 2 milhões de operações.

Teste de carga com 3 milhões de operações.

17. FORA DO ESCOPO

Não fazem parte desta história:

Leitura de arquivos CSV.

Execução do AWS Batch.

Transformação de layouts específicos para o modelo canônico.

Publicação inicial das mensagens no SQS.

Implementação das tabelas DynamoDB, caso já contemplada pela H03.

Implementação do EventBridge Pipes.

Implementação das filas de registro por clearing.

Envio das operações para a B3.

Processamento de retornos da clearing.

Conciliação financeira.

Consultas do portal operacional.

CRITÉRIOS DE ACEITE

CA01 - A Lambda deverá ser acionada automaticamente por mensagens da fila SQS de ingestão.

CA02 - A solução deverá utilizar .NET 8 ou versão homologada pelo projeto.

CA03 - O consumidor deverá receber mensagens compatíveis com o modelo canônico.

CA04 - O contrato deverá possuir versão explícita.

CA05 - Mensagens inválidas deverão ser identificadas e rejeitadas de maneira controlada.

CA06 - O consumidor deverá validar os campos obrigatórios e as invariantes necessárias para persistência.

CA07 - O consumidor deverá mapear corretamente a mensagem para a tabela OPERACOES.

CA08 - O consumidor deverá criar o snapshot inicial na tabela REGISTROS_CLEARING.

CA09 - O status inicial do registro deverá ser PENDENTE.

CA10 - O número inicial de tentativas deverá ser zero.

CA11 - A criação dos registros deverá utilizar TransactWriteItems.

CA12 - A criação da operação e do registro inicial deverá ser atômica.

CA13 - A transação deverá utilizar condições de inexistência para impedir sobrescritas indevidas.

CA14 - A solução deverá impedir duplicidades de operações financeiras durante reprocessamentos.

CA15 - A implementação não deverá depender exclusivamente de GSI para garantir unicidade.

CA16 - Mensagens duplicadas legítimas deverão ser tratadas como processamento idempotente.

CA17 - Conflitos de identidade ou conteúdo deverão ser identificados e não poderão ser tratados como duplicidades legítimas.

CA18 - O consumidor deverá suportar processamento concorrente de mensagens.

CA19 - A concorrência deverá ser configurável por ambiente.

CA20 - O processamento deverá utilizar retries controlados para falhas transitórias.

CA21 - A implementação deverá diferenciar falhas transitórias de falhas permanentes.

CA22 - A integração SQS/Lambda deverá utilizar ReportBatchItemFailures.

CA23 - Uma falha individual não deverá provocar reprocessamento desnecessário das demais mensagens do lote.

CA24 - Mensagens somente poderão ser consideradas bem-sucedidas após confirmação da transação ou comprovação de duplicidade legítima.

CA25 - A fila deverá possuir DLQ configurada.

CA26 - Mensagens que excederem a política de tentativas deverão ser encaminhadas à DLQ.

CA27 - Mensagens inválidas não poderão ser descartadas silenciosamente.

CA28 - A criação de novas operações deverá produzir eventos no DynamoDB Streams.

CA29 - O consumidor não deverá publicar diretamente mensagens nas filas de registro das clearings.

CA30 - O processamento de mensagens duplicadas não deverá produzir novos eventos de criação da operação.

CA31 - A solução deverá permitir rastrear a origem e o resultado de processamento de cada mensagem.

CA32 - A implementação deverá disponibilizar logs estruturados.

CA33 - A implementação deverá disponibilizar métricas de throughput, latência, falhas e duplicidades.

CA34 - Deverão existir métricas e alertas para backlog e mensagens na DLQ.

CA35 - As permissões IAM deverão seguir o princípio do menor privilégio.

CA36 - A infraestrutura deverá ser provisionada por Terraform.

CA37 - Deverão existir testes unitários para validação, mapeamento e idempotência.

CA38 - Deverão existir testes de integração para persistência transacional.

CA39 - Deverão existir testes para mensagens duplicadas e processamento concorrente.

CA40 - Deverão existir testes para falhas parciais de lotes SQS.

CA41 - Deverá ser validado o comportamento em cenários de throttling e indisponibilidade temporária do DynamoDB.

CA42 - Deverá ser realizado teste de carga com 2 milhões de operações.

CA43 - Deverá ser realizado teste de carga com 3 milhões de operações.

CA44 - Os testes de carga deverão registrar throughput, latência, duração, consumo de capacidade e taxa de falhas.

CA45 - A solução deverá demonstrar capacidade de consumir o volume esperado dentro da janela operacional homologada.

CA46 - A solução deverá possuir documentação técnica de configuração, execução, monitoramento, tratamento de falhas e reprocessamento.

DEFINIÇÃO DE PRONTO

A história será considerada concluída quando uma mensagem válida publicada na fila SQS de ingestão for consumida pela AWS Lambda, validada e persistida transacionalmente nas tabelas OPERACOES e REGISTROS_CLEARING.

O processamento deverá impedir duplicidades, preservar a consistência entre as tabelas, tratar falhas individuais e permitir recuperação por meio de retries e DLQ.

A solução deverá possuir infraestrutura provisionada, testes automatizados, observabilidade e evidências de testes de carga compatíveis com a volumetria esperada.
