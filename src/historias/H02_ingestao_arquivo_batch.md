# H02 — Ingerir arquivo de operações e persistir no DynamoDB

## Objetivo
Implementar o fluxo de ingestão desde a criação do CSV no S3 até a transformação das linhas para o modelo canônico e persistência no DynamoDB, utilizando EventBridge e AWS Batch. O processamento deverá suportar arquivos de aproximadamente 20 a 30 milhões de registros.

## Descrição detalhada
Fluxo:

```text
CSV → S3 → EventBridge → AWS Batch → Leitura streaming → Validação
    → Adapter CSV → Modelo canônico → DynamoDB
```

O EventBridge deverá identificar somente arquivos pertencentes ao processo e disparar o Batch com `bucket`, `objectKey` e informações necessárias para localizar o objeto.

O Batch deverá possuir Compute Environment, Job Queue, Job Definition, IAM Role, imagem de container e logs. CPU, memória e paralelismo deverão ser definidos após teste com volume representativo.

O arquivo deverá ser lido por streaming, sem carregamento integral em memória.

O contrato CSV deverá ser isolado do domínio:

```text
Contrato CSV → Adapter CSV → Modelo canônico → Persistência
```

O Adapter deverá produzir dados como `idOperacao`, `tipoOperacao`, `produto`, `clearingDestino`, `valor`, `dataOperacao`, `tipoOrigem`, `idOperacaoOrigem`, `dadosProduto` e `chaveIdempotencia`. Cada operação terá exatamente uma clearing de destino.

A persistência utilizará `OPERACOES_CLEARING`.

Operação:
```text
identificadorAgregado = OPERACAO#<idOperacao>
chaveEntidade = OPERACAO
```

Registro inicial:
```text
identificadorAgregado = OPERACAO#<idOperacao>
chaveEntidade = REGISTRO
```

Exemplo de operação:
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
  "tipoOrigem": "ARQUIVO",
  "idOperacaoOrigem": "ARQ-987654",
  "chaveIdempotencia": "ARQUIVO#ARQ-987654"
}
```

Exemplo do registro inicial:
```json
{
  "identificadorAgregado": "OPERACAO#OP-000001",
  "chaveEntidade": "REGISTRO",
  "tipoEntidade": "REGISTRO_CLEARING",
  "idOperacao": "OP-000001",
  "idRegistro": "REG-000001",
  "clearing": "B3",
  "status": "PENDENTE",
  "tentativa": 0
}
```

O processamento deverá ser idempotente. Reprocessar a mesma identidade lógica não poderá criar operação financeira duplicada. Linhas inválidas deverão ser contabilizadas e investigáveis.

Deverão existir métricas de arquivo iniciado/finalizado, linhas lidas, válidas, persistidas, rejeitadas, com erro e tempo total.

## Requisitos
- RF01 — Evento de criação inicia automaticamente o fluxo.
- RF02 — EventBridge considera somente arquivos do processo.
- RF03 — Batch recebe identificação do arquivo.
- RF04 — Leitura por streaming.
- RF05 — Arquivo não é carregado integralmente em memória.
- RF06 — CSV convertido por Adapter.
- RF07 — Domínio independente do layout CSV.
- RF08 — Uma clearing por operação.
- RF09 — Persistência em `OPERACOES_CLEARING`.
- RF10 — Criação do `REGISTRO` inicial.
- RF11 — Idempotência.
- RF12 — Linhas inválidas identificáveis.
- RF13 — Logs e métricas.
- RF14 — Infraestrutura via Terraform.

## Critérios de aceite
- CA01 — Arquivo válido no S3 inicia processamento.
- CA02 — Arquivo fora do padrão não inicia processamento.
- CA03 — Batch identifica bucket e object key.
- CA04 — Processamento sem carregar arquivo inteiro em memória.
- CA05 — Linha válida passa pelo Adapter CSV.
- CA06 — Persistência segue modelo canônico.
- CA07 — Operação usa `OPERACAO#<idOperacao>` / `OPERACAO`.
- CA08 — Registro usa `OPERACAO#<idOperacao>` / `REGISTRO`.
- CA09 — Reprocessamento não cria duplicidade.
- CA10 — Linha inválida é contabilizada.
- CA11 — Totais do processamento são observáveis.
- CA12 — Recursos do Batch validados com volume representativo.
- CA13 — Infraestrutura declarada em Terraform.
