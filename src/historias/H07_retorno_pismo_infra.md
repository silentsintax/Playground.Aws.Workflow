# H07 — Receber retornos B3 através da Pismo

## Objetivo
Criar infraestrutura para receber de forma segura e desacoplada as notificações de resultado B3 encaminhadas pela Pismo via SNS.

## Descrição detalhada
```text
B3 → Pismo → SNS (conta Pismo) → SQS RETORNO (nossa conta) → DLQ
```

A Queue Policy deverá permitir somente o tópico/conta autorizados. A fila terá DLQ e redrive policy. Quando aplicável, deverão ser tratadas permissões KMS no cenário cross-account.

## Requisitos
- RF01 — SQS de retorno.
- RF02 — DLQ de retorno.
- RF03 — Subscription SNS → SQS.
- RF04 — Queue Policy cross-account.
- RF05 — Origem restrita ao tópico autorizado.
- RF06 — Redrive policy.
- RF07 — Recursos próprios via Terraform.

## Critérios de aceite
- CA01 — SNS autorizado entrega mensagem.
- CA02 — Origem não autorizada não publica.
- CA03 — Falhas recorrentes chegam à DLQ.
- CA04 — Mensagem preservada para rastreabilidade conforme política.
- CA05 — Recursos próprios em Terraform.
