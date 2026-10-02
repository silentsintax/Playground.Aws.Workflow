# H10 — Implementar segurança e permissões da plataforma

## Objetivo
Garantir least privilege, proteção de credenciais e segurança das integrações.

## Descrição detalhada
Permissões esperadas:

```text
Batch: leitura S3 + escrita DynamoDB
Pipes: leitura Streams + envio SQS
Registration Worker: leitura SQS + leitura/escrita DynamoDB + segredo B3
Return Worker: leitura SQS retorno + consulta GSI + escrita DynamoDB
```

Evitar `Action: *` / `Resource: *` sem justificativa formal.

Credenciais externas deverão utilizar mecanismo aprovado, como Secrets Manager quando aplicável. A integração cross-account com Pismo deverá aceitar somente recursos autorizados.

## Requisitos
- RF01 — Least privilege.
- RF02 — Roles separadas por responsabilidade quando aplicável.
- RF03 — Credenciais protegidas.
- RF04 — Cross-account restrito.
- RF05 — Criptografia considerada.
- RF06 — IAM/infra via Terraform.

## Critérios de aceite
- CA01 — Batch acessa somente recursos necessários.
- CA02 — Workers possuem somente permissões necessárias.
- CA03 — Credenciais B3 não estão no código.
- CA04 — SNS não autorizado não publica.
- CA05 — IAM revisável via Terraform.
