import argparse
import os
import time
from typing import Any, Dict

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis

from event_transformer import hash_key, normalize_event, ranking_key, ts_key

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_NAME = "marketplace"
COLLECTION_NAME = "events"


def ensure_ts_add(redis: Redis, key: str, ts: int, value: int, labels: Dict[str, str]) -> None:
    try:
        redis.execute_command("TS.ADD", key, ts, value)
    except Exception:
        redis.execute_command(
            "TS.CREATE",
            key,
            "RETENTION",
            604800000,
            "LABELS",
            *sum(([k, v] for k, v in labels.items()), []),
        )
        redis.execute_command("TS.ADD", key, ts, value)


def apply_to_redis(redis: Redis, event: Dict[str, Any]) -> None:
    r_hash = hash_key(event)
    redis.hset(
        r_hash,
        mapping={
            "restaurant_id": event["restaurant_id"],
            "restaurant_name": event["restaurant_name"],
            "neighborhood": event["neighborhood"],
            "cuisine": event["cuisine"],
            "location": f"{event['lon']},{event['lat']}",
        },
    )

    if event["type"] == "view":
        score = redis.zincrby("ranking:restaurants:views", 1, event["restaurant_id"])
        redis.hincrby(r_hash, "views", 1)
        ensure_ts_add(
            redis,
            ts_key(event, "views"),
            event["ts"],
            1,
            {"restaurant_id": event["restaurant_num"], "metric": "views"},
        )
        print(
            f"[REDIS] ZINCRBY ranking:restaurants:views 1 {event['restaurant_id']} → score: {int(float(score))}"
        )

    elif event["type"] == "order":
        score = redis.zincrby("ranking:restaurants:orders", 1, event["restaurant_id"])
        redis.hincrby(r_hash, "orders", 1)
        ensure_ts_add(
            redis,
            ts_key(event, "orders"),
            event["ts"],
            1,
            {"restaurant_id": event["restaurant_num"], "metric": "orders"},
        )
        print(
            f"[REDIS] ZINCRBY ranking:restaurants:orders 1 {event['restaurant_id']} → score: {int(float(score))}"
        )

    elif event["type"] == "search":
        score = redis.zincrby("ranking:dishes:searches", 1, event["dish_id"])
        redis.hincrby(r_hash, "searches", 1)
        print(
            f"[REDIS] ZINCRBY ranking:dishes:searches 1 {event['dish_id']} → score: {int(float(score))}"
        )

    elif event["type"] == "rating":
        redis.hincrbyfloat(r_hash, "rating_sum", event["stars"])
        redis.hincrby(r_hash, "rating_count", 1)
        rating_sum = float(redis.hget(r_hash, "rating_sum") or 0.0)
        rating_count = int(redis.hget(r_hash, "rating_count") or 1)
        avg = round(rating_sum / max(rating_count, 1), 2)
        redis.hset(r_hash, "stars", avg)
        print(f"[REDIS] HSET {r_hash} stars {avg}")


def handle_event(redis: Redis, raw_event: Dict[str, Any]) -> None:
    event = normalize_event(raw_event)
    if event["type"] not in {"view", "search", "order", "rating"}:
        return

    if event["type"] == "search":
        print(f"[EVENT] search | dish: {event['dish_name'].lower()}")
    else:
        print(
            f"[EVENT] {event['type']} | {event['restaurant_id']} | {event['restaurant_name']} | {event['neighborhood']}"
        )
    apply_to_redis(redis, event)


def backfill_existing(col, redis: Redis, limit: int = 50000) -> None:
    processed = 0
    for doc in col.find({}).sort("ts", 1).limit(limit):
        handle_event(redis, doc)
        processed += 1
    print(f"[CONSUMER] Backfill concluído: {processed} eventos.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Consome eventos do MongoDB Change Stream e publica no Redis.")
    parser.add_argument("--skip-backfill", action="store_true", help="Não processa eventos já existentes.")
    args = parser.parse_args()

    mongo = MongoClient(MONGO_URI)
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    col = mongo[DB_NAME][COLLECTION_NAME]

    if not args.skip_backfill:
        backfill_existing(col, redis)

    print("[CONSUMER] Conectado ao MongoDB Change Stream")
    print("[CONSUMER] Aguardando eventos...")

    while True:
        try:
            with col.watch([{"$match": {"operationType": "insert"}}], full_document="updateLookup") as stream:
                for change in stream:
                    handle_event(redis, change["fullDocument"])
        except Exception as exc:
            print(f"[CONSUMER] Reconectando após erro: {exc}")
            time.sleep(2)


if __name__ == "__main__":
    main()
