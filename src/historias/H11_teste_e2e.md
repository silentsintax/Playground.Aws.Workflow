# H11 — Validar fluxo integrado RDB B3 ponta a ponta

## Objetivo
Validar o fluxo completo com três tabelas.

## Descrição detalhada
```text
CSV → S3 → EventBridge → Batch → Adapter
    → OPERACOES + REGISTROS_CLEARING
    → Stream OPERACOES → Pipes → SQS
    → Registration Worker → B3 → Pismo → SNS → SQS Retorno
    → Return Worker → REGISTROS_CLEARING + EVENTOS_REGISTRO
```

Validar operação aceita/rejeitada, linha inválida, duplicidades, falha transitória, resultado indeterminado, retorno duplicado/sem correlação e DLQ. Validar consistência entre status atual e histórico.

## Critérios de aceite
Operação e registro inicial criados; Stream inicia processamento; B3 recebe; retorno correlaciona por `idExterno`; estado final e histórico corretos; duplicidades não geram duplicidade financeira; retry/DLQ e rastreabilidade por `idOperacao` validados.
