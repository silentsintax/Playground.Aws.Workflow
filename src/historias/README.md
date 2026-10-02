# Histórias — Plataforma Multi Clearing

Arquivos separados por história:

- H02 — Ingestão do arquivo: EventBridge → Batch → Adapter CSV → DynamoDB
- H04 — DynamoDB Streams → EventBridge Pipes → SQS
- H05 — Registration Worker e roteamento
- H06 — Integração/Adapter B3
- H07 — Retorno Pismo: SNS cross-account → SQS
- H08 — Processamento do retorno
- H09 — Observabilidade
- H10 — Segurança e permissões
- H11 — Teste E2E

H01 (S3) e H03 (estrutura DynamoDB) já haviam sido elaboradas anteriormente e não foram recriadas neste pacote.
