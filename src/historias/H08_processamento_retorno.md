# H08 — Processar retorno das operações B3

## Objetivo
Consumir retornos da SQS, correlacioná-los às operações e atualizar o estado atual e o histórico.

## Descrição detalhada
```text
SQS RETORNO → Return Worker → idExterno → INDICE_ID_EXTERNO
            → REGISTRO_CLEARING → atualizar snapshot + adicionar EVENTO_REGISTRO
```

Consulta:
```text
identificadorExterno = ID_EXTERNO#<idExterno>
```

O item `REGISTRO` deverá ser atualizado e um evento append-only criado:

```text
REGISTRO#EVENTO#<timestamp>#<idEvento>
```

Quando necessário, utilizar transação DynamoDB para atualizar snapshot e inserir evento atomicamente.

O retorno deverá ser idempotente. Entrega duplicada não pode produzir transições funcionais duplicadas.

## Requisitos
- RF01 — Consumir SQS de retorno.
- RF02 — Extrair id externo.
- RF03 — Consultar `INDICE_ID_EXTERNO`.
- RF04 — Identificar operação.
- RF05 — Atualizar `REGISTRO_CLEARING`.
- RF06 — Criar `EVENTO_REGISTRO`.
- RF07 — Histórico append-only.
- RF08 — Idempotência do retorno.
- RF09 — Retorno sem correlação tratado.

## Critérios de aceite
- CA01 — Retorno conhecido localiza operação.
- CA02 — Status atual atualizado.
- CA03 — Alteração gera histórico.
- CA04 — Evento existente não é sobrescrito.
- CA05 — Notificação duplicada não duplica transição.
- CA06 — Retorno sem correlação é investigável.
- CA07 — Falha segue retry/DLQ.
