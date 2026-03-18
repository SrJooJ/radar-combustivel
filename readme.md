# Radar Combustível — Pipeline MongoDB → Redis
## Plataforma de monitoramento de preços de combustíveis em tempo real

> **Trabalho Final — Bancos de Dados In-Memory | FIAP MBA em Tecnologia**

---

## 🎯 Objetivo

Construir um pipeline de streaming em tempo real que captura eventos da plataforma **Radar Combustível** do MongoDB e os propaga para o Redis, mantendo métricas atualizadas de:

- **Postos com menor preço** por combustível (ranking em tempo real)
- **Combustíveis mais buscados** (demanda por tipo)
- **Bairros com maior volume de buscas** (geo + agregação)
- **Evolução de preço ao longo do tempo** (séries temporais)
- **Postos mais bem avaliados** (nota média)
- **Postos mais buscados e com mais abastecimentos** (popularidade)

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────┐
│                    EVENTO DE ORIGEM                     │
│  Usuários/Postos → MongoDB (eventos brutos)             │
└──────────────────────────┬──────────────────────────────┘
                           │ Change Stream (oplog)
                           ▼
┌─────────────────────────────────────────────────────────┐
│              PIPELINE PYTHON (Consumer)                 │
│  mongodb_consumer.py                                    │
│  - Lê Change Stream do MongoDB                          │
│  - Transforma evento (event_transformer.py)             │
│  - Publica no Redis                                     │
└──────────────────────────┬──────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │ Sorted Sets │  │ RediSearch  │  │ TimeSeries  │
   │ (Rankings)  │  │ (Busca+Geo) │  │ (Preços)    │
   └─────────────┘  └─────────────┘  └─────────────┘
          │                │                │
          └────────────────┼────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────┐
│              CAMADA DE VISUALIZAÇÃO                     │
│  Streamlit Dashboard (data-view.py) — <10ms             │
└─────────────────────────────────────────────────────────┘
```

📘 Detalhamento do pipeline: [docs/streaming-mongo-redis.md](docs/streaming-mongo-redis.md)

---

## 📦 Estrutura do Repositório

```
radar-combustivel/
├── docker-compose.yml          # MongoDB + Redis + App
├── requirements.txt            # Dependências Python
├── .env                        # Variáveis de ambiente (local)
├── .env.example                # Variáveis de ambiente (template)
├── .gitignore
├── docs/
│   └── streaming-mongo-redis.md # Explicação do fluxo de streaming
├── init/
│   ├── mongo_seed.py           # Popula MongoDB com postos e eventos fake
│   └── redis_indexes.py        # Cria índices RediSearch + TimeSeries
├── pipeline/
│   ├── mongodb_consumer.py     # Lê Change Stream e publica no Redis
│   └── event_transformer.py    # Transforma eventos brutos
├── queries/
│   ├── data-view.py            # Dashboard Streamlit em tempo real
│   └── redis_reader.py         # Consultas CLI de demonstração
└── readme.md
```

---

## 🗃️ Modelo de Dados

### MongoDB — Coleção `eventos` (database: `radar_combustivel`)

```json
{
  "_id": "ObjectId",
  "type": "busca | atualizacao_preco | abastecimento | avaliacao",
  "ts": 1710010203000,
  "user_id": "usr_291",
  "posto_id": "posto_45",
  "posto_nome": "Posto Shell Av. Paulista",
  "bandeira": "Shell",
  "combustivel": "gasolina_comum",
  "preco": 5.89,
  "bairro": "Consolação",
  "cidade": "São Paulo",
  "lat": -23.5505,
  "lon": -46.6333,
  "nota": 4.5
}
```

### Redis — Estruturas de Destino

| Chave | Tipo | Descrição |
|-------|------|-----------|
| `ranking:postos:buscas` | Sorted Set | Score = total de buscas do posto |
| `ranking:postos:abastecimentos` | Sorted Set | Score = total de abastecimentos |
| `ranking:combustivel:buscas` | Sorted Set | Score = buscas por tipo de combustível |
| `ranking:bairro:buscas` | Sorted Set | Score = buscas por bairro |
| `ranking:preco:{combustivel}` | Sorted Set | Score = preço (menor = melhor) |
| `posto:{id}` | Hash | Metadados do posto + preços + nota |
| `idx:postos` | RediSearch Index | Busca por bandeira, bairro, nota + geo |
| `ts:posto:{id}:preco:{combustivel}` | TimeSeries | Evolução do preço ao longo do tempo |
| `ts:posto:{id}:buscas` | TimeSeries | Volume de buscas por minuto |
| `ts:posto:{id}:abastecimentos` | TimeSeries | Volume de abastecimentos por minuto |

### Justificativa das Estruturas Redis

| Estrutura | Uso | Justificativa |
|-----------|-----|---------------|
| **Sorted Set** | Rankings de preço, buscas, bairros | ZINCRBY atômico; ZRANGE/ZREVRANGE O(log N); ideal para top-N |
| **Hash** | Cadastro de postos | Acesso O(1) a campos individuais; agrupa metadados do posto |
| **RediSearch** | Busca por bandeira/bairro/nota | Full-text search + filtros numéricos + geo em <10ms |
| **TimeSeries** | Evolução de preço | Agregação nativa por janela temporal; retenção automática |
| **Geo** (via RediSearch) | Postos por proximidade | GeoField no índice permite queries por raio |

---

## 🔧 Configuração do Ambiente

### Passo 1: Pré-requisitos
- Docker + Docker Compose
- Python 3.10+

#### Ferramentas Opcionais
- [MongoDB Compass](https://www.mongodb.com/try/download/compass) — IDE para visualizar dados no MongoDB
- [RedisInsight](https://redis.io/insight/) — IDE para visualizar dados no Redis

### Passo 2: Variáveis de Ambiente

```bash
cp .env.example .env
```

```env
MONGO_URI=mongodb://localhost:27017/?directConnection=true
MONGO_DB=radar_combustivel
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
```

### Passo 3: Subir o ambiente
```bash
docker-compose up -d
```

### Passo 4: Instalar dependências
```bash
pip install -r requirements.txt
```

### Passo 5: Inicializar dados e índices

```bash
# 1. Popula MongoDB com 300 postos e ~24K eventos fake (10K gerais + 14.4K historico de precos)
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
[CONSUMER] Backfill concluído: 24400 eventos.
[CONSUMER] Conectado ao MongoDB Change Stream
[CONSUMER] Aguardando eventos...
[EVENT] busca | posto_45 | Posto Shell Pinheiros | Pinheiros
[REDIS] ZINCRBY ranking:postos:buscas 1 posto_45 → score: 142
[EVENT] atualizacao_preco | posto_12 | gasolina_comum | R$ 5.78
[REDIS] Preço atualizado posto_12 gasolina_comum → R$ 5.78
```

### Passo 7: Terminal 2 — Consultas em tempo real (CLI)
```bash
python queries/redis_reader.py
```

### Passo 8: Terminal 3 — Dashboard Streamlit
```bash
python -m streamlit run queries/data-view.py
```

Abra no navegador:
```text
http://localhost:8501
```

O dashboard exibe:
- Top 10 postos mais buscados
- Combustíveis mais buscados (gráfico de pizza)
- Bairros com mais buscas
- Ranking de menor preço por combustível
- Top 10 postos mais bem avaliados
- Busca dinâmica (RediSearch) por bandeira/bairro/nota
- Evolução de preço (TimeSeries) por posto e combustível

---

## 📊 Queries de Demonstração

### Top 10 postos mais buscados
```python
top = redis.zrevrange("ranking:postos:buscas", 0, 9, withscores=True)
```

### Ranking de menor preço de gasolina comum
```python
menores = redis.zrange("ranking:preco:gasolina_comum", 0, 9, withscores=True)
```

### Postos Shell em Pinheiros com nota >= 4.0
```python
results = redis.ft("idx:postos").search(
    Query("@bandeira:{Shell} @bairro:{Pinheiros}")
    .add_filter(NumericFilter("nota", 4.0, 5))
    .sort_by("buscas", asc=False)
    .paging(0, 10)
)
```

### Evolução de preço da gasolina no posto 1
```python
series = redis.execute_command(
    "TS.RANGE", "ts:posto:1:preco:gasolina_comum",
    "-", "+", "AGGREGATION", "last", "60000"
)
```

### Rodando direto no redis-cli
```bash
docker exec -it lab-redis redis-cli

# Top 10 postos mais buscados
ZREVRANGE ranking:postos:buscas 0 9 WITHSCORES

# Menor preço de gasolina comum
ZRANGE ranking:preco:gasolina_comum 0 9 WITHSCORES

# Combustíveis mais buscados
ZREVRANGE ranking:combustivel:buscas 0 5 WITHSCORES

# Bairros com mais buscas
ZREVRANGE ranking:bairro:buscas 0 9 WITHSCORES

# Busca por bandeira e bairro (RediSearch)
FT.SEARCH idx:postos "@bandeira:{Shell} @bairro:{Pinheiros} @nota:[4.0 5]" SORTBY buscas DESC LIMIT 0 10

# Evolução de preço (TimeSeries)
TS.RANGE ts:posto:1:preco:gasolina_comum - + AGGREGATION last 60000
```

---

## 🧪 Simulando Carga (Stress Test)

```bash
# Gera 1000 eventos aleatórios no MongoDB
python init/mongo_seed.py --stress --events 1000

# Aumentar volume
python init/mongo_seed.py --stress --events 2000
```

Validação:
1. **Terminal 1 (consumer)** mostra novos eventos sendo processados
2. **Terminal 2 (reader)** exibe aumento de scores nos rankings
3. **Dashboard Streamlit** reflete atualização nos gráficos

---

## 📋 Checklist de Validação

- [ ] `docker-compose up -d` sobe sem erros (MongoDB + Redis)
- [ ] `mongo_seed.py` popula 300 postos e ~24K eventos
- [ ] `redis_indexes.py` cria `idx:postos` sem erro
- [ ] `mongodb_consumer.py` processa eventos sem travar
- [ ] `ZREVRANGE ranking:postos:buscas 0 9` retorna resultados
- [ ] `ZRANGE ranking:preco:gasolina_comum 0 9` retorna postos com menor preço
- [ ] Busca `@bandeira:{Shell}` retorna resultados em <10ms
- [ ] TimeSeries retorna evolução de preço para qualquer posto
- [ ] Dashboard Streamlit exibe todos os painéis corretamente

---

## 💡 Decisões de Arquitetura

| Decisão | Escolha | Justificativa |
|---------|---------|---------------|
| Fonte de eventos | MongoDB Change Stream | Captura inserções sem polling, baixa latência |
| Rankings de preço | Sorted Set | Score = preço, ZRANGE retorna menores primeiro |
| Rankings de popularidade | Sorted Set | ZINCRBY atômico, ZREVRANGE para top-N |
| Busca de postos | RediSearch | Filtros por bandeira, bairro, nota + geo |
| Evolução de preço | RedisTimeSeries | Agregação por janela + retenção de 7 dias |
| Consistência | Eventual | Aceitável para rankings e métricas |

---

## 📚 Conceitos Praticados

- **MongoDB Change Stream** — captura eventos do oplog sem polling
- **Sorted Set (ZINCRBY/ZADD)** — rankings atômicos para preço e popularidade
- **RediSearch** — busca com filtros por bandeira, bairro, nota e geolocalização
- **RedisTimeSeries** — série temporal de preços com agregação nativa
- **Hashes** — cadastro resumido de postos com acesso O(1)
- **Pipeline streaming** — atualização em tempo quase real
- **Modelagem orientada a acesso** — estruturas Redis escolhidas para cada tipo de consulta

---

## 🧹 Comandos para Limpar o Ambiente

```bash
docker-compose down -v
```
