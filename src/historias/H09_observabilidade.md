# H09 — Implementar observabilidade do fluxo de registro

## Objetivo
Garantir rastreabilidade operacional ponta a ponta.

## Descrição detalhada
Logs estruturados deverão permitir correlação por `idOperacao`, `idRegistro`, `idExterno`, `clearing`, `produto`, `correlationId` e `batchJobId`.

Métricas mínimas:
- arquivos recebidos;
- operações lidas, persistidas e rejeitadas;
- profundidade e idade das filas;
- mensagens em DLQ;
- operações enviadas, aceitas e rejeitadas;
- erros técnicos e resultados indeterminados;
- retornos recebidos e sem correlação.

Alarmes deverão cobrir filas, DLQs e falhas recorrentes. Dados sensíveis não deverão ser registrados desnecessariamente.

## Requisitos
- RF01 — Logs estruturados.
- RF02 — Correlação ponta a ponta.
- RF03 — Métricas operacionais.
- RF04 — Alarmes.
- RF05 — Monitoramento de DLQs.
- RF06 — Proteção de dados sensíveis.

## Critérios de aceite
- CA01 — Pesquisa por `idOperacao`.
- CA02 — Rastreamento pelos componentes.
- CA03 — Crescimento anormal de filas gera alerta.
- CA04 — DLQ é observável.
- CA05 — Falhas externas têm contexto para diagnóstico.
