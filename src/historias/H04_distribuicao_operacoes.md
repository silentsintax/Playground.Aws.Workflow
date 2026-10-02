# H04 — Disponibilizar operações para processamento de registro

## Objetivo
Criar o mecanismo assíncrono que identifica novas operações no DynamoDB e as disponibiliza para processamento.

## Descrição detalhada
```text
DynamoDB → DynamoDB Streams → EventBridge Pipes → SQS REGISTRO → DLQ
```

O Pipe deverá encaminhar somente eventos elegíveis de criação de `OPERACAO`. Inserções de `EVENTO_REGISTRO` e atualizações de `REGISTRO` não poderão iniciar novo registro, evitando loops.

Mensagem sugerida:
```json
{
  "idOperacao": "OP-000001",
  "identificadorAgregado": "OPERACAO#OP-000001",
  "clearingDestino": "B3"
}
```

A SQS deverá possuir DLQ, redrive policy e visibility timeout compatível com o consumidor.

## Requisitos
- RF01 — DynamoDB Streams habilitado.
- RF02 — EventBridge Pipes consumindo o Stream.
- RF03 — Somente operações elegíveis encaminhadas.
- RF04 — Eventos históricos não provocam novo registro.
- RF05 — Atualização de `REGISTRO` não cria loop.
- RF06 — SQS de registro.
- RF07 — DLQ.
- RF08 — Terraform.

## Critérios de aceite
- CA01 — Nova `OPERACAO` produz mensagem na SQS.
- CA02 — `EVENTO_REGISTRO` não produz mensagem.
- CA03 — Atualização de `REGISTRO` não produz mensagem.
- CA04 — Mensagem identifica inequivocamente a operação.
- CA05 — Falhas recorrentes seguem política de DLQ.
- CA06 — Infraestrutura em Terraform.
