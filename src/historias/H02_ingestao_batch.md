# H02 — Ingerir arquivo de operações e persistir no DynamoDB

## Objetivo
Implementar o fluxo CSV → S3 → EventBridge → AWS Batch → Adapter CSV → modelo canônico → DynamoDB, suportando aproximadamente 20–30 milhões de registros.

## Descrição detalhada
O arquivo será lido por streaming. O contrato CSV será adaptado ao modelo canônico, mantendo o domínio independente da origem. Cada operação terá exatamente uma clearing.

Persistência:
- `OPERACOES`: PK `idOperacao`.
- `REGISTROS_CLEARING`: PK `idOperacao`.

Para criação consistente de operação e registro, avaliar/usar `TransactWriteItems`. O reprocessamento deverá respeitar `chaveIdempotencia`.

Fluxo:
```text
CSV → S3 → EventBridge → Batch → Adapter CSV
                              ├→ OPERACOES
                              └→ REGISTROS_CLEARING
```

## Requisitos
- EventBridge inicia Batch.
- Leitura streaming.
- Adapter CSV → modelo canônico.
- Uma clearing por operação.
- Persistência nas duas tabelas.
- Idempotência.
- Consistência entre operação/registro.
- Logs e métricas.
- Terraform.

## Critérios de aceite
- Arquivo válido inicia processamento.
- Não há carga integral do arquivo em memória.
- Operação é criada em `OPERACOES`.
- Estado inicial é criado em `REGISTROS_CLEARING`.
- Ambas usam `idOperacao` como PK.
- Reprocessamento não gera duplicidade lógica.
- Falha parcial não deixa estado inconsistente.
