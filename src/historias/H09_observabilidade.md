# H09 — Implementar observabilidade

## Objetivo
Garantir rastreabilidade ponta a ponta no modelo de três tabelas.

## Descrição detalhada
Correlacionar `idOperacao`, `idRegistro`, `idExterno`, clearing, produto, correlationId e batchJobId. A investigação por ID deverá relacionar `OPERACOES`, `REGISTROS_CLEARING` e `EVENTOS_REGISTRO`.

Métricas: operações recebidas/persistidas/duplicadas/pendentes; enviadas/aceitas/rejeitadas/erro/indeterminadas; eventos; filas/DLQs; retornos correlacionados e sem correlação.

## Requisitos
Logs estruturados, correlação, métricas, alarmes e monitoramento de DLQ.

## Critérios de aceite
Operação pesquisável por ID; três estruturas correlacionáveis; filas/DLQs observáveis; falhas externas diagnosticáveis.
