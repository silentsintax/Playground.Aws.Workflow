# H05 — Implementar Registration Worker e roteamento de clearing

## Objetivo
Implementar Lambda .NET que consome a fila de registro, recupera os dados no DynamoDB e encaminha cada operação à integração da clearing de destino.

## Descrição detalhada
```text
SQS → Registration Worker → DynamoDB → Clearing Router
                                      ├─ B3 → Adapter B3
                                      └─ futuras clearings
```

Inicialmente somente B3 será suportada. O núcleo do worker não deverá conhecer detalhes do contrato B3. Deve existir abstração equivalente a `IClearingAdapter`.

Fluxo do worker:
1. Consumir mensagem.
2. Identificar operação.
3. Recuperar operação e registro.
4. Validar estado.
5. Selecionar adapter por `clearingDestino`.
6. Solicitar registro.
7. Atualizar estado.
8. Adicionar `EVENTO_REGISTRO`.

O processamento deverá ser idempotente. Falhas deverão ser classificadas em transitórias, definitivas e resultado indeterminado. Timeout após envio não significa automaticamente que a clearing não recebeu a operação.

## Requisitos
- RF01 — .NET.
- RF02 — Consumo SQS.
- RF03 — Suporte inicial B3.
- RF04 — Roteamento por `clearingDestino`.
- RF05 — Adapters isolam contratos externos.
- RF06 — Idempotência.
- RF07 — Mudanças relevantes geram histórico.
- RF08 — Classificação de erros.
- RF09 — Mensagem removida da fila somente após conclusão adequada.

## Critérios de aceite
- CA01 — Operação B3 roteada ao Adapter B3.
- CA02 — Clearing não suportada gera erro controlado.
- CA03 — Mensagem duplicada não gera registro financeiro duplicado.
- CA04 — Falha transitória segue retry.
- CA05 — Falha definitiva gera evidência.
- CA06 — Resultado indeterminado não gera reenvio cego.
- CA07 — Histórico identifica tentativas e transições.
