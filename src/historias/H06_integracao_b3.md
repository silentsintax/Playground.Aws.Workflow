# H06 — Implementar integração com a clearing B3

## Objetivo
Implementar Adapter B3 responsável por transformar o modelo canônico no contrato da B3 e realizar o envio.

## Descrição detalhada
```text
Modelo canônico → Adapter B3 → Contrato B3 → API B3
```

O adapter deverá realizar mapeamento, validação específica, serialização, chamada da API e interpretação da resposta.

Tratar 2xx, 4xx, 5xx, timeout, falha de conexão e resposta inválida.

O identificador externo de correlação deverá ser persistido. Protocolos síncronos retornados pela B3 deverão ser armazenados quando existentes. Credenciais não poderão ficar no código ou em configuração versionada.

## Requisitos
- RF01 — Adapter B3.
- RF02 — Conversão do modelo canônico.
- RF03 — Tratamento HTTP.
- RF04 — Timeouts explícitos.
- RF05 — Identificadores de correlação persistidos.
- RF06 — Segredos em mecanismo aprovado.
- RF07 — Domínio desacoplado do contrato B3.

## Critérios de aceite
- CA01 — RDB válido convertido para contrato B3.
- CA02 — Envio possui correlação.
- CA03 — Resposta válida atualiza processamento.
- CA04 — Erro funcional distinguível de erro técnico.
- CA05 — Timeout não pressupõe que a B3 não processou.
- CA06 — Logs não expõem informações sensíveis.
