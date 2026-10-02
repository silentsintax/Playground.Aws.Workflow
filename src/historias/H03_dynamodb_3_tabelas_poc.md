# H03 — Criar estrutura DynamoDB para operações, registros e histórico

## Objetivo
Criar a persistência da plataforma em três tabelas:
1. `OPERACOES`
2. `REGISTROS_CLEARING`
3. `EVENTOS_REGISTRO`

A solução deverá suportar alta volumetria, múltiplos produtos, múltiplas clearings, uma clearing por operação, idempotência, correlação externa e histórico. A infraestrutura oficial será criada via Terraform. Esta história inclui POC manual no AWS Console.

# Modelagem

```text
OPERACOES 1 ── 1 REGISTROS_CLEARING 1 ── N EVENTOS_REGISTRO
```

## OPERACOES
PK: `idOperacao` (String). Sem SK inicialmente.

| Campo | Tipo | Finalidade |
|---|---|---|
| idOperacao | String | PK |
| tipoOperacao | String | APLICACAO/RESGATE |
| produto | String | RDB/CDB/etc |
| clearingDestino | String | Clearing destino |
| valor | Number | Valor |
| dataOperacao | String | Data |
| tipoOrigem | String | ARQUIVO/KAFKA |
| idOperacaoOrigem | String | ID da origem |
| chaveIdempotencia | String | Identidade lógica |
| dadosProduto | Map | Campos específicos |
| dataHoraCriacao | String | Criação |

`dadosProduto` mantém flexibilidade para RDB, CDB e futuros produtos.

GSI candidato: `INDICE_IDEMPOTENCIA`, PK `chaveIdempotencia`. O GSI não garante unicidade sozinho; a estratégia definitiva deve considerar escrita condicional/item de controle.

## REGISTROS_CLEARING
PK: `idOperacao` (String). Sem SK.

| Campo | Tipo | Finalidade |
|---|---|---|
| idOperacao | String | PK |
| idRegistro | String | ID interno |
| clearing | String | Clearing |
| status | String | Estado atual |
| idExterno | String | Correlação externa |
| protocoloExterno | String | Protocolo |
| tentativa | Number | Tentativa |
| dataHoraCriacao | String | Criação |
| dataHoraAtualizacao | String | Atualização |

GSI obrigatório: `INDICE_ID_EXTERNO`, PK `idExterno`.

## EVENTOS_REGISTRO
Histórico append-only.

PK: `idOperacao`  
SK: `chaveEvento`

Formato da SK:
```text
<dataHoraEvento>#<idEvento>
```

| Campo | Tipo | Finalidade |
|---|---|---|
| idOperacao | String | PK |
| chaveEvento | String | SK cronológica |
| idEvento | String | ID evento |
| idRegistro | String | Registro relacionado |
| tipoEvento | String | Tipo |
| statusAnterior | String | Estado anterior |
| statusAtual | String | Novo estado |
| origemEvento | String | Origem |
| tentativa | Number | Tentativa |
| dataHoraEvento | String | Momento |
| detalhes | Map | Dados adicionais |

## Atomicidade
Mudanças de estado que também criam histórico deverão utilizar `TransactWriteItems` quando necessário:
```text
UPDATE REGISTROS_CLEARING
+
PUT EVENTOS_REGISTRO
```

## Streams
Somente `OPERACOES` deverá possuir o Stream que inicia o fluxo:
```text
OPERACOES → DynamoDB Streams → EventBridge Pipes → SQS REGISTRO
```

# Configuração manual da POC

## OPERACOES
| Configuração | Valor |
|---|---|
| Table name | OPERACOES |
| Partition key | idOperacao |
| Tipo | String |
| Sort key | Não |
| Capacity | On-demand |
| Stream | New image / configuração do Pipe |
| GSI opcional | INDICE_IDEMPOTENCIA |
| GSI PK | chaveIdempotencia |

## REGISTROS_CLEARING
| Configuração | Valor |
|---|---|
| Table name | REGISTROS_CLEARING |
| Partition key | idOperacao |
| Tipo | String |
| Sort key | Não |
| Capacity | On-demand |
| GSI | INDICE_ID_EXTERNO |
| GSI PK | idExterno |

## EVENTOS_REGISTRO
| Configuração | Valor |
|---|---|
| Table name | EVENTOS_REGISTRO |
| Partition key | idOperacao |
| PK type | String |
| Sort key | chaveEvento |
| SK type | String |
| Capacity | On-demand |
| GSI | Nenhum inicialmente |
| Stream | Não |

# Massa da POC
```text
OP-000001 | RDB | APLICACAO | B3 | REGISTRADO
OP-000002 | RDB | RESGATE   | B3 | EM_PROCESSAMENTO
OP-000003 | CDB | APLICACAO | B3 | ERRO
OP-000004 | RDB | APLICACAO | B3 | PENDENTE
```

## OPERACOES
```json
{"idOperacao":"OP-000001","tipoOperacao":"APLICACAO","produto":"RDB","clearingDestino":"B3","valor":15000.50,"dataOperacao":"2026-10-02","tipoOrigem":"ARQUIVO","idOperacaoOrigem":"ARQ-000001","chaveIdempotencia":"ARQUIVO#20261002#000001","dadosProduto":{"codigoRdb":"RDB001","dataVencimento":"2028-10-01"},"dataHoraCriacao":"2026-10-02T10:00:00.000Z"}
```
```json
{"idOperacao":"OP-000002","tipoOperacao":"RESGATE","produto":"RDB","clearingDestino":"B3","valor":5000.00,"dataOperacao":"2026-10-02","tipoOrigem":"ARQUIVO","idOperacaoOrigem":"ARQ-000002","chaveIdempotencia":"ARQUIVO#20261002#000002","dadosProduto":{"codigoRdb":"RDB002"},"dataHoraCriacao":"2026-10-02T10:01:00.000Z"}
```
```json
{"idOperacao":"OP-000003","tipoOperacao":"APLICACAO","produto":"CDB","clearingDestino":"B3","valor":25000.00,"dataOperacao":"2026-10-02","tipoOrigem":"ARQUIVO","idOperacaoOrigem":"ARQ-000003","chaveIdempotencia":"ARQUIVO#20261002#000003","dadosProduto":{"codigoCdb":"CDB001","indexador":"CDI","percentualIndexador":105},"dataHoraCriacao":"2026-10-02T10:02:00.000Z"}
```
```json
{"idOperacao":"OP-000004","tipoOperacao":"APLICACAO","produto":"RDB","clearingDestino":"B3","valor":7500.00,"dataOperacao":"2026-10-02","tipoOrigem":"ARQUIVO","idOperacaoOrigem":"ARQ-000004","chaveIdempotencia":"ARQUIVO#20261002#000004","dadosProduto":{"codigoRdb":"RDB004"},"dataHoraCriacao":"2026-10-02T10:03:00.000Z"}
```

## REGISTROS_CLEARING
```json
{"idOperacao":"OP-000001","idRegistro":"REG-000001","clearing":"B3","status":"REGISTRADO","idExterno":"B3-20261002-000001","protocoloExterno":"PROTOCOLO-B3-001","tentativa":1,"dataHoraCriacao":"2026-10-02T10:00:10.000Z","dataHoraAtualizacao":"2026-10-02T10:05:00.000Z"}
```
```json
{"idOperacao":"OP-000002","idRegistro":"REG-000002","clearing":"B3","status":"EM_PROCESSAMENTO","idExterno":"B3-20261002-000002","tentativa":1,"dataHoraCriacao":"2026-10-02T10:01:10.000Z","dataHoraAtualizacao":"2026-10-02T10:03:00.000Z"}
```
```json
{"idOperacao":"OP-000003","idRegistro":"REG-000003","clearing":"B3","status":"ERRO","idExterno":"B3-20261002-000003","tentativa":2,"dataHoraCriacao":"2026-10-02T10:02:10.000Z","dataHoraAtualizacao":"2026-10-02T10:07:00.000Z"}
```
```json
{"idOperacao":"OP-000004","idRegistro":"REG-000004","clearing":"B3","status":"PENDENTE","tentativa":0,"dataHoraCriacao":"2026-10-02T10:03:10.000Z","dataHoraAtualizacao":"2026-10-02T10:03:10.000Z"}
```

## EVENTOS_REGISTRO — timeline OP-000001
```json
{"idOperacao":"OP-000001","chaveEvento":"2026-10-02T10:00:10.000Z#EVT-001","idEvento":"EVT-001","idRegistro":"REG-000001","tipoEvento":"REGISTRO_CRIADO","statusAtual":"PENDENTE","origemEvento":"INGESTAO","dataHoraEvento":"2026-10-02T10:00:10.000Z"}
```
```json
{"idOperacao":"OP-000001","chaveEvento":"2026-10-02T10:01:00.000Z#EVT-002","idEvento":"EVT-002","idRegistro":"REG-000001","tipoEvento":"ENVIO_INICIADO","statusAnterior":"PENDENTE","statusAtual":"EM_PROCESSAMENTO","origemEvento":"REGISTRATION_WORKER","tentativa":1,"dataHoraEvento":"2026-10-02T10:01:00.000Z"}
```
```json
{"idOperacao":"OP-000001","chaveEvento":"2026-10-02T10:02:00.000Z#EVT-003","idEvento":"EVT-003","idRegistro":"REG-000001","tipoEvento":"ENVIADO_CLEARING","statusAnterior":"EM_PROCESSAMENTO","statusAtual":"ENVIADO","origemEvento":"ADAPTER_B3","tentativa":1,"dataHoraEvento":"2026-10-02T10:02:00.000Z"}
```
```json
{"idOperacao":"OP-000001","chaveEvento":"2026-10-02T10:05:00.000Z#EVT-004","idEvento":"EVT-004","idRegistro":"REG-000001","tipoEvento":"STATUS_ALTERADO","statusAnterior":"ENVIADO","statusAtual":"REGISTRADO","origemEvento":"RETORNO_PISMO","tentativa":1,"dataHoraEvento":"2026-10-02T10:05:00.000Z"}
```

## Evento de erro OP-000003
```json
{"idOperacao":"OP-000003","chaveEvento":"2026-10-02T10:07:00.000Z#EVT-010","idEvento":"EVT-010","idRegistro":"REG-000003","tipoEvento":"ERRO_REGISTRO","statusAnterior":"EM_PROCESSAMENTO","statusAtual":"ERRO","origemEvento":"ADAPTER_B3","tentativa":2,"dataHoraEvento":"2026-10-02T10:07:00.000Z","detalhes":{"codigoErro":"B3-001","descricao":"Erro de exemplo utilizado exclusivamente na POC"}}
```

# Consultas da POC
1. `GetItem` em `OPERACOES`: `idOperacao = OP-000001`.
2. `GetItem` em `REGISTROS_CLEARING`: `idOperacao = OP-000001`.
3. `Query` em `EVENTOS_REGISTRO`: `idOperacao = OP-000001`.
4. Último evento: Query anterior com `ScanIndexForward=false`, `Limit=1`.
5. Retorno Pismo: GSI `INDICE_ID_EXTERNO`, `idExterno = B3-20261002-000001`.
6. Se habilitado, GSI `INDICE_IDEMPOTENCIA`, `chaveIdempotencia = ARQUIVO#20261002#000001`.

Consultas globais por produto, clearing, status e dashboards não são objetivo deste modelo transacional; poderão ser atendidas futuramente por read model/S3/Athena.

# Cenários mínimos da POC
- Inserir/recuperar operação.
- Recuperar estado atual.
- Timeline cronológica.
- Último evento.
- Correlação por id externo.
- RDB e CDB sem remodelagem.
- Histórico append-only.
- Idempotência.
- `TransactWriteItems` entre registro e evento.
- Stream somente em `OPERACOES`.

# Volumetria
Exemplo conceitual com 20M operações/dia:
- `OPERACOES`: ~20M inserts.
- `REGISTROS_CLEARING`: ~20M itens + updates.
- `EVENTOS_REGISTRO`: N eventos por operação; 5 eventos implicariam ~100M eventos/dia.

Evitar PKs de baixa cardinalidade como `B3`, `RDB` ou `REGISTRADO`; utilizar `idOperacao`.

# Requisitos
- Criar as três tabelas com chaves descritas.
- Criar `INDICE_ID_EXTERNO`.
- Avaliar `INDICE_IDEMPOTENCIA`.
- Histórico append-only.
- Suportar `dadosProduto`.
- Uma clearing por operação.
- Transações quando necessário.
- Somente `OPERACOES` inicia fluxo por Stream.
- Infra oficial via Terraform.
- Seguir padrões corporativos de criptografia, PITR e tags.

# Critérios de aceite
- Três tabelas disponíveis.
- Operação/registro recuperáveis por `idOperacao`.
- Histórico consultável e ordenado.
- Último evento sem Scan completo.
- Retorno externo localiza registro via GSI.
- RDB/CDB coexistem sem remodelagem.
- Histórico permanece append-only.
- Idempotência validada.
- Snapshot/histórico consistentes.
- POC demonstra transação.
- Stream de `OPERACOES` validado.
- Configuração definitiva representada em Terraform.
