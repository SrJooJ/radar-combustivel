import re
import os
from typing import Dict

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis
from redis.commands.search.field import GeoField, NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_NAME = "radar_combustivel"
COLLECTION_NAME = "eventos"


def numeric_posto_id(value: str) -> str:
    match = re.search(r"(\d+)$", value or "")
    return match.group(1) if match else value


def load_posto_snapshot() -> Dict[str, dict]:
    mongo = MongoClient(MONGO_URI)
    col = mongo[DB_NAME][COLLECTION_NAME]
    pipeline = [
        {"$sort": {"ts": -1}},
        {
            "$group": {
                "_id": "$posto_id",
                "posto_nome": {"$first": "$posto_nome"},
                "bandeira": {"$first": "$bandeira"},
                "bairro": {"$first": "$bairro"},
                "cidade": {"$first": "$cidade"},
                "lat": {"$first": "$lat"},
                "lon": {"$first": "$lon"},
                "nota": {"$first": "$nota"},
            }
        },
    ]
    out = {}
    for row in col.aggregate(pipeline):
        pid = row["_id"]
        out[pid] = row
    return out


def load_precos_snapshot() -> Dict[str, Dict[str, float]]:
    """Carrega o último preço por posto/combustível."""
    mongo = MongoClient(MONGO_URI)
    col = mongo[DB_NAME][COLLECTION_NAME]
    pipeline = [
        {"$match": {"type": "atualizacao_preco"}},
        {"$sort": {"ts": -1}},
        {
            "$group": {
                "_id": {"posto_id": "$posto_id", "combustivel": "$combustivel"},
                "preco": {"$first": "$preco"},
            }
        },
    ]
    out: Dict[str, Dict[str, float]] = {}
    for row in col.aggregate(pipeline):
        pid = row["_id"]["posto_id"]
        comb = row["_id"]["combustivel"]
        if pid not in out:
            out[pid] = {}
        out[pid][comb] = float(row["preco"])
    return out


def main() -> None:
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    snapshot = load_posto_snapshot()
    precos = load_precos_snapshot()

    # Seed hash documents: posto:{id}
    for pid, item in snapshot.items():
        simple_id = numeric_posto_id(pid)
        key = f"posto:{simple_id}"
        mapping = {
            "posto_id": pid,
            "posto_nome": item.get("posto_nome", ""),
            "bandeira": item.get("bandeira", ""),
            "bairro": item.get("bairro", ""),
            "cidade": item.get("cidade", "São Paulo"),
            "nota": float(item.get("nota", 3.0)),
            "buscas": 0,
            "abastecimentos": 0,
            "location": f"{item.get('lon', 0)},{item.get('lat', 0)}",
        }

        # Adiciona preços disponíveis ao hash
        if pid in precos:
            for comb, preco in precos[pid].items():
                mapping[f"preco_{comb}"] = preco

        redis.hset(key, mapping=mapping)

        # TimeSeries: buscas, abastecimentos, e preço por combustível
        for metric in ("buscas", "abastecimentos"):
            ts_key = f"ts:posto:{simple_id}:{metric}"
            try:
                redis.execute_command(
                    "TS.CREATE",
                    ts_key,
                    "RETENTION",
                    604800000,
                    "LABELS",
                    "posto_id",
                    simple_id,
                    "metric",
                    metric,
                )
            except Exception:
                pass

        # TimeSeries para preço de cada combustível
        combustiveis = ["gasolina_comum", "gasolina_aditivada", "etanol", "diesel", "diesel_s10", "gnv"]
        for comb in combustiveis:
            ts_key = f"ts:posto:{simple_id}:preco:{comb}"
            try:
                redis.execute_command(
                    "TS.CREATE",
                    ts_key,
                    "RETENTION",
                    604800000,
                    "LABELS",
                    "posto_id",
                    simple_id,
                    "metric",
                    "preco",
                    "combustivel",
                    comb,
                )
            except Exception:
                pass

    # Recria índice RediSearch (idempotente para re-execuções)
    try:
        redis.execute_command("FT.DROPINDEX", "idx:postos", "DD")
    except Exception:
        pass

    redis.ft("idx:postos").create_index(
        fields=[
            TextField("posto_nome", weight=2.0),
            TagField("bandeira"),
            TagField("bairro"),
            TagField("cidade"),
            NumericField("nota", sortable=True),
            NumericField("buscas", sortable=True),
            NumericField("abastecimentos", sortable=True),
            NumericField("preco_gasolina_comum", sortable=True),
            NumericField("preco_etanol", sortable=True),
            NumericField("preco_diesel", sortable=True),
            GeoField("location"),
        ],
        definition=IndexDefinition(prefix=["posto:"], index_type=IndexType.HASH),
    )

    print(
        f"[REDIS] idx:postos criado com {len(snapshot)} documentos. "
        f"Preços carregados para {len(precos)} postos."
    )


if __name__ == "__main__":
    main()
