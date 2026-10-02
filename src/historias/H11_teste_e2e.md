# H11 — Validar fluxo integrado RDB B3 ponta a ponta

## Objetivo
Validar o fluxo desde a entrada do arquivo RDB até o retorno B3 recebido pela Pismo.

## Descrição detalhada
```text
CSV → S3 → EventBridge → Batch → Adapter CSV → DynamoDB
    → Streams → Pipes → SQS Registro → Registration Worker
    → Adapter B3 → B3 → Pismo → SNS → SQS Retorno
    → Return Worker → DynamoDB
```

Cenários mínimos:
- operação aceita;
- operação rejeitada;
- linha inválida;
- mensagem duplicada;
- falha transitória;
- retorno duplicado;
- retorno sem correlação;
- DLQ.

Ao final, deverá ser possível consultar `OPERACAO`, `REGISTRO` atual e `EVENTOS` por:

```text
identificadorAgregado = OPERACAO#<idOperacao>
```

## Requisitos
Testes em ambiente controlado/homologação, com evidências dos cenários. AWS Console pode ser usado na POC para inspeção, mas sucesso em escala não deverá depender de consultas manuais.

## Critérios de aceite
- CA01 — Arquivo válido cria operações esperadas.
- CA02 — Operação chega à integração B3.
- CA03 — Retorno é correlacionado.
- CA04 — `REGISTRO` reflete estado final.
- CA05 — Histórico reflete transições.
- CA06 — Duplicidade de mensagem não gera duplicidade financeira.
- CA07 — Falhas previstas têm comportamento conhecido.
- CA08 — DLQs e alarmes validados.
- CA09 — Caminho da operação reconstruível por correlação.
