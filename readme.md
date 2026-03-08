# Lab encontro2: Pipeline Streaming MongoDB → Redis
## Caso de Uso: Marketplace de Restaurantes (iFood-like)

> **Encontro 2 — Bancos de Dados In-Memory | FIAP MBA em Tecnologia**
> Prof. Daniel Lemeszenski · Março de 2026

---

## 🎯 Objetivo

Construir um pipeline de streaming em tempo real que captura eventos de um marketplace de restaurantes (buscas, cliques, pedidos) do MongoDB e os propaga para o Redis, mantendo métricas atualizadas de:

- **Restaurantes mais visitados** (ranking em tempo real)
- **Pratos mais buscados** (full-text search + contador)
- **Métricas por restaurante** (views, pedidos, nota média)
- **Top bairros** (geo + agregação)

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────┐
│                    EVENTO DE ORIGEM                     │
│  App Mobile/Web → MongoDB (eventos brutos)              │
└──────────────────────────┬──────────────────────────────┘
                           │ Change Stream (oplog)
                           ▼
┌─────────────────────────────────────────────────────────┐
│              PIPELINE PYTHON (Consumer)                 │
│  mongodb_consumer.py                                    │
│  - Lê Change Stream do MongoDB                          │
│  - Transforma evento                                    │
│  - Publica no Redis                                     │
└──────────────────────────┬──────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │ Sorted Sets │  │ RediSearch  │  │ TimeSeries  │
   │ (Rankings)  │  │ (Busca)     │  │ (Métricas)  │
   └─────────────┘  └─────────────┘  └─────────────┘
          │                │                │
          └────────────────┼────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────┐
│                   SERVING LAYER                         │
│  redis_reader.py — Consultas <10ms                      │
└─────────────────────────────────────────────────────────┘
```

---

## 📦 Estrutura do Repositório

```
lab-encontro2/
├── docker-compose.yml          # MongoDB + Redis + App
├── requirements.txt            # Dependências Python
├── .env.example                # Variáveis de ambiente
├── init/
│   ├── mongo_seed.py           # Popula MongoDB com dados fake
│   └── redis_indexes.py        # Cria índices RediSearch
├── pipeline/
│   ├── mongodb_consumer.py      # Lê Change Stream e publica no Redis
│   └── event_transformer.py     # Transforma eventos brutos
├── queries/
│   └── redis_reader.py         # Consultas de demonstração
└── readme.md
```

---

## 🗃️ Modelo de Dados

### MongoDB — Coleção `events`
```json
{
  "_id": "ObjectId",
  "type": "view | search | order | rating",
  "ts": 1710010203000,
  "user_id": "usr_291",
  "restaurant_id": "resto_245",
  "restaurant_name": "Sushi Pinheiros",
  "dish_name": "Temaki Salmão",
  "dish_id": "dish_88",
  "neighborhood": "Pinheiros",
  "lat": -23.5505,
  "lon": -46.6333,
  "stars": 4.8
}
```

### Redis — Estruturas de Destino

| Chave | Tipo | Descrição |
|-------|------|-----------|
| `ranking:restaurants:views` | Sorted Set | Score = total de views |
| `ranking:restaurants:orders` | Sorted Set | Score = total de pedidos |
| `ranking:dishes:searches` | Sorted Set | Score = total de buscas |
| `resto:{id}` | Hash | Metadados do restaurante |
| `idx:restaurants` | RediSearch Index | Busca full-text + geo |
| `ts:resto:{id}:views` | TimeSeries | Views por minuto |
| `ts:resto:{id}:orders` | TimeSeries | Pedidos por minuto |

---

## 🔧 Configuração do Ambiente

### Passo 1: Pré-requisitos
- Docker + Docker Compose
- Python 3.10+

### Passo 2: Variáveis de Ambiente

Copie o arquivo de exemplo:

```bash
cp .env.example .env
```

```env
MONGO_URI=mongodb://localhost:27017/?directConnection=true
MONGO_DB=marketplace
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
```

### Passo 3: Subir o ambiente
```bash
git clone https://github.com/commithouse/lab-streaming-mongo-redis
cd lab-streaming-mongo-redis
docker-compose up -d
```

### Passo 4: Instalar dependências
```bash
pip install -r requirements.txt
```

### Passo 5: Inicializar dados e índices
```bash
# 1. Popula MongoDB com 500 restaurantes e 10K eventos fake
python init/mongo_seed.py

# 2. Cria índices no Redis (RediSearch + TimeSeries)
python init/redis_indexes.py
```

---

## 🚀 Executando o Pipeline

### Passo 6: Terminal 1 — Consumidor (Change Stream)
```bash
python pipeline/mongodb_consumer.py
```

Saída esperada:
```
[CONSUMER] Conectado ao MongoDB Change Stream
[CONSUMER] Aguardando eventos...
[EVENT] view | resto_245 | Sushi Pinheiros | Pinheiros
[REDIS] ZINCRBY ranking:restaurants:views 1 resto_245 → score: 142
[REDIS] TS.ADD ts:resto:245:views * 1
[EVENT] search | dish: temaki salmão
[REDIS] ZINCRBY ranking:dishes:searches 1 dish_88 → score: 37
```

### Passo 7: Terminal 2 — Consultas em tempo real
```bash
python queries/redis_reader.py
```

---

## 📊 Queries de Demonstração

### Top 10 restaurantes mais visitados
```python
# redis_reader.py
top = redis.zrevrange("ranking:restaurants:views", 0, 9, withscores=True)
```

### Top 5 pratos mais buscados
```python
top_dishes = redis.zrevrange("ranking:dishes:searches", 0, 4, withscores=True)
```

### Restaurantes de pizza em Pinheiros com 4.5+ estrelas
```python
results = redis.ft("idx:restaurants").search(
    Query("@cuisine:{pizza} @neighborhood:{Pinheiros}")
    .add_filter(NumericFilter("stars", 4.5, 5))
    .sort_by("views", asc=False)
    .paging(0, 10)
)
```

### Série temporal de views do restaurante 245 (últimos 10 min)
```python
series = redis.ts().range(
    "ts:resto:245:views",
    from_time="-",
    to_time="+",
    aggregation_type="sum",
    bucket_size_msec=60000  # agrega por minuto
)
```

---

## 🧪 Passo 8: Simulando Carga (Stress Test)

```bash
# Gera 1000 eventos aleatórios no MongoDB para testar o pipeline
python init/mongo_seed.py --stress --events 1000
```

Acompanhe o ranking sendo atualizado em tempo real no Terminal 2.

---

## 📋 Checklist de Validação

Ao terminar o lab, verifique:

- [ ] `docker-compose up -d` sobe sem erros (MongoDB + Redis)
- [ ] `mongo_seed.py` popula ≥ 500 restaurantes e ≥ 10K eventos
- [ ] `redis_indexes.py` cria `idx:restaurants` sem erro
- [ ] `mongodb_consumer.py` processa eventos sem travar
- [ ] `ZREVRANGE ranking:restaurants:views 0 9` retorna 10 resultados
- [ ] Busca `@cuisine:{pizza}` retorna resultados em <10ms
- [ ] TimeSeries retorna série por minuto para qualquer restaurante

---

## 💡 Decisões de Arquitetura (para discussão)

| Decisão | Escolha | Justificativa |
|---------|---------|---------------|
| Fonte de eventos | MongoDB Change Stream | Captura inserções sem polling |
| Rankings | Sorted Set | ZINCRBY atômico, ZREVRANGE O(log N) |
| Busca de pratos | RediSearch | Full-text + filtro simultâneo |
| Métricas temporais | RedisTimeSeries | Agregação nativa por janela |
| Consistência | Eventual | Aceitável para ranking/métricas |
| Frequência batch | A cada 1h | Recalcula top bairros e tendências |

---

## 📚 Conceitos Praticados

- MongoDB Change Stream — captura eventos do oplog sem polling
- Sorted Set (ZINCRBY) — ranking atômico e ordenado em O(log N)
- RediSearch — full-text search com filtros numéricos e geoespaciais
- RedisTimeSeries — série temporal com agregação nativa por janela
- Pipeline streaming vs batch — quando usar cada estratégia em produção
- Consistência eventual — trade-off aceitável para métricas de ranking

---

## 🔗 Referências

- [MongoDB Change Streams Docs](https://www.mongodb.com/docs/manual/changeStreams/)
- [Redis Sorted Sets](https://redis.io/docs/data-types/sorted-sets/)
- [RediSearch Query Syntax](https://redis.io/docs/interact/search-and-query/)
- [RedisTimeSeries](https://redis.io/docs/data-types/timeseries/)
- Repositório do lab Vector DB: `commithouse/lab-vector-db-redis`

---

## 🧹 Comandos para Limpar o Ambiente

```bash
# Encerrar containers e remover volumes do lab
docker-compose down -v

# (Opcional) remover imagens baixadas no lab
docker-compose down --rmi local
```
