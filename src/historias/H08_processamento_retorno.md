# H08 — Processar retorno B3 recebido através da Pismo

## Objetivo
Consumir retorno, correlacionar por `idExterno`, atualizar estado e histórico.

## Descrição detalhada
```text
SQS RETORNO → Return Worker → INDICE_ID_EXTERNO
            → idOperacao
            → UPDATE REGISTROS_CLEARING
            + PUT EVENTOS_REGISTRO
```
Usar `TransactWriteItems` quando aplicável e garantir idempotência do retorno.

## Requisitos
Consumir SQS; extrair id externo; consultar GSI; atualizar registro; criar evento append-only; transação; idempotência; tratar retorno sem correlação.

## Critérios de aceite
Retorno conhecido localiza operação; estado e histórico atualizados atomicamente; duplicidade não duplica transição; falhas seguem retry/DLQ.
