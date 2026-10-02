# H05 — Implementar Registration Worker e roteamento de clearing

## Objetivo
Consumir SQS, recuperar operação e registro atual e encaminhar à clearing.

## Descrição detalhada
```text
SQS → Registration Worker
       ├→ OPERACOES
       └→ REGISTROS_CLEARING
              ↓
       Clearing Router → Adapter B3 → B3
```
Consulta ambas as tabelas por `idOperacao`. Transições atualizam `REGISTROS_CLEARING` e adicionam `EVENTOS_REGISTRO`, usando `TransactWriteItems` quando necessário. Processamento idempotente e tratamento de falhas transitórias, definitivas e resultado indeterminado.

## Requisitos
Consumir SQS; consultar as duas tabelas; roteamento; B3 inicial; atualizar registro; criar histórico; transação; idempotência.

## Critérios de aceite
Recuperação correta por ID; roteamento B3; snapshot e histórico consistentes; duplicidade não gera envio financeiro duplicado.
