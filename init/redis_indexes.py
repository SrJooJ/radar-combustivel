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

DB_NAME = "marketplace"
COLLECTION_NAME = "events"


def numeric_restaurant_id(value: str) -> str:
    match = re.search(r"(\d+)$", value or "")
    return match.group(1) if match else value


def load_restaurant_snapshot() -> Dict[str, dict]:
    mongo = MongoClient(MONGO_URI)
    col = mongo[DB_NAME][COLLECTION_NAME]
    pipeline = [
        {"$sort": {"ts": -1}},
        {
            "$group": {
                "_id": "$restaurant_id",
                "restaurant_name": {"$first": "$restaurant_name"},
                "neighborhood": {"$first": "$neighborhood"},
                "cuisine": {"$first": "$cuisine"},
                "lat": {"$first": "$lat"},
                "lon": {"$first": "$lon"},
                "stars": {"$first": "$stars"},
            }
        },
    ]
    out = {}
    for row in col.aggregate(pipeline):
        rid = row["_id"]
        out[rid] = row
    return out


def main() -> None:
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    snapshot = load_restaurant_snapshot()

    # Seed hash documents: resto:{id}
    for rid, item in snapshot.items():
        simple_id = numeric_restaurant_id(rid)
        key = f"resto:{simple_id}"
        redis.hset(
            key,
            mapping={
                "restaurant_id": rid,
                "restaurant_name": item.get("restaurant_name", ""),
                "neighborhood": item.get("neighborhood", ""),
                "cuisine": item.get("cuisine", ""),
                "stars": float(item.get("stars", 4.0)),
                "views": 0,
                "location": f"{item.get('lon', 0)},{item.get('lat', 0)}",
            },
        )

        # TimeSeries keys from spec: ts:resto:{id}:views|orders
        for metric in ("views", "orders"):
            ts_key = f"ts:resto:{simple_id}:{metric}"
            try:
                redis.execute_command(
                    "TS.CREATE",
                    ts_key,
                    "RETENTION",
                    604800000,
                    "LABELS",
                    "restaurant_id",
                    simple_id,
                    "metric",
                    metric,
                )
            except Exception:
                # already exists
                pass

    # Recreate RediSearch index (idempotent for lab reruns)
    try:
        redis.execute_command("FT.DROPINDEX", "idx:restaurants", "DD")
    except Exception:
        pass

    redis.ft("idx:restaurants").create_index(
        fields=[
            TextField("restaurant_name", weight=2.0),
            TagField("neighborhood"),
            TagField("cuisine"),
            NumericField("stars", sortable=True),
            NumericField("views", sortable=True),
            GeoField("location"),
        ],
        definition=IndexDefinition(prefix=["resto:"], index_type=IndexType.HASH),
    )

    print(f"[REDIS] idx:restaurants criado com {len(snapshot)} documentos.")


if __name__ == "__main__":
    main()
