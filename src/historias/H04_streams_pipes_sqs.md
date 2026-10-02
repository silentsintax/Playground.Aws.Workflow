# H04 — Disponibilizar novas operações para processamento

## Objetivo
Identificar novas operações em `OPERACOES` e disponibilizá-las assincronamente.

## Descrição detalhada
```text
OPERACOES → DynamoDB Streams → EventBridge Pipes → SQS REGISTRO → DLQ
```
Somente `OPERACOES` inicia o fluxo. Mensagem mínima:
```json
{"idOperacao":"OP-000001"}
```

## Requisitos
Stream em `OPERACOES`, Pipe, SQS, DLQ, redrive, visibility timeout e Terraform.

## Critérios de aceite
Nova operação gera mensagem; alterações em `REGISTROS_CLEARING` ou `EVENTOS_REGISTRO` não geram novo envio; falhas recorrentes seguem para DLQ.
