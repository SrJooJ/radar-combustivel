# Como funciona o streaming MongoDB -> Redis neste projeto

Este projeto usa um consumidor Python para capturar eventos do Radar Combustível em tempo real no MongoDB e atualizar rankings, preços e métricas no Redis.

## Visao geral do fluxo

1. Eventos sao inseridos na colecao `radar_combustivel.eventos` (ex.: `init/mongo_seed.py`).
2. O script `pipeline/mongodb_consumer.py` conecta no MongoDB e no Redis.
3. Ele processa um backfill opcional (eventos ja existentes).
4. Em seguida, abre um Change Stream com `col.watch(...)` para escutar novos `insert`.
5. Cada evento e normalizado (`pipeline/event_transformer.py`) e aplicado no Redis.

## Pre-requisito: Replica Set no MongoDB

O Change Stream so funciona com MongoDB em Replica Set (`rs0`).

No projeto:
- `docker-compose.yml` sobe o Mongo com `--replSet rs0`
- o servico `mongo-init` inicializa o ReplicaSet

## Entrada no MongoDB

O `init/mongo_seed.py` cria eventos fake de postos de combustivel e grava em `radar_combustivel.eventos` com `insert_many`.
No modo stress, ele insere novos eventos para simular carga continua.

### Tipos de eventos

| Tipo | Descricao | Peso |
|------|-----------|------|
| `busca` | Usuario busca um posto ou combustivel | 40% |
| `atualizacao_preco` | Posto atualiza preco de um combustivel | 20% |
| `abastecimento` | Usuario registra abastecimento no posto | 25% |
| `avaliacao` | Usuario avalia o posto com nota 1-5 | 15% |

## Consumo em tempo real

No `pipeline/mongodb_consumer.py`:
- `MongoClient(MONGO_URI)` abre conexao com Mongo
- `Redis(...)` abre conexao com Redis
- `col.watch([{"$match": {"operationType": "insert"}}], full_document="updateLookup")` escuta inserts
- para cada `change`, processa `change["fullDocument"]`

## Transformacao de evento

No `pipeline/event_transformer.py`, o evento e padronizado:
- normaliza tipo (`busca`, `atualizacao_preco`, `abastecimento`, `avaliacao`)
- converte tipos (`ts`, `lat`, `lon`, `preco`, `nota`)
- extrai id numerico do posto para as chaves Redis

## Escrita no Redis

Para cada evento, dependendo do tipo:

### busca
- Sorted Set `ranking:postos:buscas` — ZINCRBY +1 no posto
- Sorted Set `ranking:combustivel:buscas` — ZINCRBY +1 no combustivel
- Sorted Set `ranking:bairro:buscas` — ZINCRBY +1 no bairro
- TimeSeries `ts:posto:{id}:buscas` — registra busca

### atualizacao_preco
- Hash `posto:{id}` — atualiza campo `preco_{combustivel}`
- Sorted Set `ranking:preco:{combustivel}` — ZADD com score = preco
- TimeSeries `ts:posto:{id}:preco:{combustivel}` — registra evolucao de preco

### abastecimento
- Sorted Set `ranking:postos:abastecimentos` — ZINCRBY +1
- TimeSeries `ts:posto:{id}:abastecimentos` — registra abastecimento

### avaliacao
- Hash `posto:{id}` — recalcula media (`nota_sum / nota_count → nota`)

## Resiliencia

O consumidor roda em loop infinito:
- se houver erro no stream, ele loga e tenta reconectar apos 2 segundos.

## Como executar

1. Suba infraestrutura:
   - `docker-compose up -d`
2. Popule dados:
   - `python init/mongo_seed.py`
3. Crie indices Redis:
   - `python init/redis_indexes.py`
4. Inicie consumer:
   - `python pipeline/mongodb_consumer.py`
5. Gere carga:
   - `python init/mongo_seed.py --stress --events 1000`
6. Dashboard:
   - `python -m streamlit run queries/data-view.py`
