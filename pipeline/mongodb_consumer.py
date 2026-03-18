import argparse
import os
import time
from typing import Any, Dict

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis
from redis.exceptions import ResponseError

from event_transformer import hash_key, normalize_event, preco_ts_key, ts_key

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_NAME = "radar_combustivel"
COLLECTION_NAME = "eventos"


def ensure_ts_add(redis: Redis, key: str, ts: int, value: float, labels: Dict[str, str]) -> None:
    try:
        redis.execute_command("TS.ADD", key, ts, value, "ON_DUPLICATE", "LAST")
    except ResponseError as exc:
        msg = str(exc)
        if "key does not exist" not in msg and "TSDB: the key does not exist" not in msg:
            raise
        redis.execute_command(
            "TS.CREATE",
            key,
            "RETENTION",
            604800000,
            "DUPLICATE_POLICY",
            "LAST",
            "LABELS",
            *sum(([k, v] for k, v in labels.items()), []),
        )
        redis.execute_command("TS.ADD", key, ts, value, "ON_DUPLICATE", "LAST")


def apply_to_redis(redis: Redis, event: Dict[str, Any]) -> None:
    p_hash = hash_key(event)

    # Atualiza hash do posto com dados mais recentes
    redis.hset(
        p_hash,
        mapping={
            "posto_id": event["posto_id"],
            "posto_nome": event["posto_nome"],
            "bandeira": event["bandeira"],
            "bairro": event["bairro"],
            "cidade": event["cidade"],
            "location": f"{event['lon']},{event['lat']}",
        },
    )

    if event["type"] == "busca":
        # Incrementa ranking de postos mais buscados
        score = redis.zincrby("ranking:postos:buscas", 1, event["posto_id"])
        redis.hincrby(p_hash, "buscas", 1)

        # Ranking de combustíveis mais buscados
        redis.zincrby("ranking:combustivel:buscas", 1, event["combustivel"])

        # Ranking de bairros com mais buscas
        redis.zincrby("ranking:bairro:buscas", 1, event["bairro"])

        # TimeSeries de buscas do posto
        ensure_ts_add(
            redis,
            ts_key(event, "buscas"),
            event["ts"],
            1,
            {"posto_id": event["posto_num"], "metric": "buscas"},
        )
        print(
            f"[REDIS] ZINCRBY ranking:postos:buscas 1 {event['posto_id']} -> score: {int(float(score))}"
        )

    elif event["type"] == "atualizacao_preco":
        # Calcula variação de preço antes de sobrescrever
        preco_anterior = redis.hget(p_hash, f"preco_{event['combustivel']}")
        if preco_anterior is not None:
            variacao = round(event["preco"] - float(preco_anterior), 2)
            variacao_abs = round(abs(variacao), 2)
            # Armazena variação absoluta no ranking (maior variação = score maior)
            redis.zadd(f"ranking:variacao:{event['combustivel']}", {event["posto_id"]: variacao_abs})
            # Armazena variação com sinal no hash do posto
            redis.hset(p_hash, f"variacao_{event['combustivel']}", variacao)

        # Atualiza preço no hash do posto
        redis.hset(p_hash, f"preco_{event['combustivel']}", event["preco"])

        # Ranking de menor preço por combustível (score = preço, menor = melhor)
        ranking_key = f"ranking:preco:{event['combustivel']}"
        redis.zadd(ranking_key, {event["posto_id"]: event["preco"]})

        # TimeSeries de evolução de preço
        ensure_ts_add(
            redis,
            preco_ts_key(event),
            event["ts"],
            event["preco"],
            {
                "posto_id": event["posto_num"],
                "metric": "preco",
                "combustivel": event["combustivel"],
            },
        )
        print(
            f"[REDIS] Preco atualizado {event['posto_id']} {event['combustivel']} -> R$ {event['preco']:.2f}"
        )

    elif event["type"] == "abastecimento":
        # Incrementa ranking de postos com mais abastecimentos
        score = redis.zincrby("ranking:postos:abastecimentos", 1, event["posto_id"])
        redis.hincrby(p_hash, "abastecimentos", 1)

        # TimeSeries de abastecimentos
        ensure_ts_add(
            redis,
            ts_key(event, "abastecimentos"),
            event["ts"],
            1,
            {"posto_id": event["posto_num"], "metric": "abastecimentos"},
        )
        print(
            f"[REDIS] ZINCRBY ranking:postos:abastecimentos 1 {event['posto_id']} -> score: {int(float(score))}"
        )

    elif event["type"] == "avaliacao":
        # Calcula média de nota do posto
        redis.hincrbyfloat(p_hash, "nota_sum", event["nota"])
        redis.hincrby(p_hash, "nota_count", 1)
        nota_sum = float(redis.hget(p_hash, "nota_sum") or 0.0)
        nota_count = int(redis.hget(p_hash, "nota_count") or 1)
        avg = round(nota_sum / max(nota_count, 1), 2)
        redis.hset(p_hash, "nota", avg)
        print(f"[REDIS] HSET {p_hash} nota {avg}")


def handle_event(redis: Redis, raw_event: Dict[str, Any]) -> None:
    event = normalize_event(raw_event)
    if event["type"] not in {"busca", "atualizacao_preco", "abastecimento", "avaliacao"}:
        return

    if event["type"] == "atualizacao_preco":
        print(
            f"[EVENT] atualizacao_preco | {event['posto_id']} | {event['combustivel']} | R$ {event['preco']:.2f}"
        )
    else:
        print(
            f"[EVENT] {event['type']} | {event['posto_id']} | {event['posto_nome']} | {event['bairro']}"
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
