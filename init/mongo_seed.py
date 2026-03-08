import argparse
import os
import random
import time
from dataclasses import dataclass
from typing import Dict, List
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from faker import Faker
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

load_dotenv()


fake = Faker("pt_BR")
RANDOM = random.Random(42)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
LOCALHOST_DIRECT_URI = "mongodb://localhost:27017/?directConnection=true"
DB_NAME = "marketplace"
COLLECTION_NAME = "events"

NEIGHBORHOODS = [
    "Pinheiros",
    "Vila Madalena",
    "Itaim Bibi",
    "Moema",
    "Perdizes",
    "Tatuapé",
    "Santana",
    "Aclimação",
    "Consolação",
    "Brooklin",
]

CUISINES = [
    "pizza",
    "japonesa",
    "brasileira",
    "hamburguer",
    "arabe",
    "vegana",
    "italiana",
    "chinesa",
]

DISHES_BY_CUISINE = {
    "pizza": ["Pizza Margherita", "Pizza Calabresa", "Pizza Quatro Queijos", "Pizza Pepperoni"],
    "japonesa": ["Temaki Salmão", "Uramaki Filadélfia", "Sashimi de Atum", "Yakissoba"],
    "brasileira": ["Feijoada", "Parmegiana", "Picadinho", "Escondidinho"],
    "hamburguer": ["Cheeseburger", "Burger Bacon", "Burger Veggie", "Smash Duplo"],
    "arabe": ["Kibe", "Esfiha de Carne", "Kafta", "Homus com Pão Sírio"],
    "vegana": ["Bowl de Grãos", "Wrap Vegano", "Lasanha de Berinjela", "Burger de Grão-de-Bico"],
    "italiana": ["Nhoque ao Sugo", "Risoto de Funghi", "Penne Alfredo", "Lasanha Bolonhesa"],
    "chinesa": ["Frango Xadrez", "Rolinho Primavera", "Lámen", "Arroz Chop Suey"],
}


@dataclass
class Restaurant:
    restaurant_id: str
    restaurant_name: str
    neighborhood: str
    cuisine: str
    lat: float
    lon: float
    base_stars: float


def get_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def with_direct_connection(uri: str) -> str:
    parts = urlsplit(uri)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["directConnection"] = "true"
    new_query = urlencode(query)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def without_replicaset(uri: str) -> str:
    parts = urlsplit(uri)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.pop("replicaSet", None)
    new_query = urlencode(query)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def candidate_uris() -> List[str]:
    candidates: List[str] = []
    primary = with_direct_connection(MONGO_URI)
    candidates.append(primary)

    # Running on host OS: container DNS name "mongo" is usually unresolved.
    if "mongo:27017" in primary:
        host_safe = primary.replace("mongo:27017", "localhost:27017")
        candidates.append(without_replicaset(host_safe))

    candidates.append(without_replicaset(LOCALHOST_DIRECT_URI))

    unique: List[str] = []
    for uri in candidates:
        if uri not in unique:
            unique.append(uri)
    return unique


def get_client_with_fallback() -> MongoClient:
    """
    Try configured URI and host-safe fallbacks.
    """
    last_exc: Exception | None = None
    for uri in candidate_uris():
        client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
        try:
            client.admin.command("ping")
            return client
        except PyMongoError as exc:
            client.close()
            last_exc = exc
    tried = " | ".join(candidate_uris())
    raise RuntimeError(f"Nao foi possivel conectar ao MongoDB. URIs testadas: {tried}") from last_exc


def ensure_replicaset(client: MongoClient) -> None:
    admin = client.admin
    try:
        admin.command("replSetGetStatus")
    except OperationFailure:
        try:
            admin.command("replSetInitiate", {"_id": "rs0", "members": [{"_id": 0, "host": "localhost:27017"}]})
            time.sleep(2)
        except OperationFailure:
            # Replica set may already be initiating/running
            pass


def random_sp_location() -> tuple[float, float]:
    lat = -23.70 + RANDOM.random() * 0.30
    lon = -46.80 + RANDOM.random() * 0.30
    return round(lat, 6), round(lon, 6)


def build_restaurants(count: int) -> List[Restaurant]:
    restaurants: List[Restaurant] = []
    for i in range(1, count + 1):
        cuisine = RANDOM.choice(CUISINES)
        neighborhood = RANDOM.choice(NEIGHBORHOODS)
        lat, lon = random_sp_location()
        restaurants.append(
            Restaurant(
                restaurant_id=f"resto_{i}",
                restaurant_name=f"{fake.first_name()} {fake.word().capitalize()}",
                neighborhood=neighborhood,
                cuisine=cuisine,
                lat=lat,
                lon=lon,
                base_stars=round(RANDOM.uniform(3.4, 5.0), 1),
            )
        )
    return restaurants


def build_dish_catalog(restaurants: List[Restaurant]) -> Dict[str, List[dict]]:
    dish_idx = 1
    catalog: Dict[str, List[dict]] = {}
    for r in restaurants:
        dishes = []
        for dish_name in DISHES_BY_CUISINE[r.cuisine]:
            dishes.append({"dish_id": f"dish_{dish_idx}", "dish_name": dish_name})
            dish_idx += 1
        catalog[r.restaurant_id] = dishes
    return catalog


def make_event(restaurants: List[Restaurant], dish_catalog: Dict[str, List[dict]], base_ts: int) -> dict:
    r = RANDOM.choice(restaurants)
    event_type = RANDOM.choices(["view", "search", "order", "rating"], weights=[45, 25, 20, 10], k=1)[0]
    ts = base_ts + RANDOM.randint(0, 3_600_000)
    dish = RANDOM.choice(dish_catalog[r.restaurant_id])

    event = {
        "type": event_type,
        "ts": ts,
        "user_id": f"usr_{RANDOM.randint(1, 15000)}",
        "restaurant_id": r.restaurant_id,
        "restaurant_name": r.restaurant_name,
        "dish_name": dish["dish_name"],
        "dish_id": dish["dish_id"],
        "neighborhood": r.neighborhood,
        "lat": r.lat,
        "lon": r.lon,
        "stars": r.base_stars,
        "cuisine": r.cuisine,
    }

    if event_type == "rating":
        event["stars"] = round(RANDOM.uniform(1.0, 5.0), 1)

    return event


def seed_initial(restaurants_count: int = 500, events_count: int = 10_000) -> None:
    client = get_client_with_fallback()
    ensure_replicaset(client)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]

    col.delete_many({})
    col.create_index("ts")
    col.create_index("type")
    col.create_index("restaurant_id")

    restaurants = build_restaurants(restaurants_count)
    dish_catalog = build_dish_catalog(restaurants)

    base_ts = int(time.time() * 1000) - 86_400_000
    events = [make_event(restaurants, dish_catalog, base_ts) for _ in range(events_count)]
    col.insert_many(events, ordered=False)

    print(f"[SEED] MongoDB populado com {restaurants_count} restaurantes e {events_count} eventos fake.")


def stress_insert(events_count: int = 1000) -> None:
    client = get_client_with_fallback()
    ensure_replicaset(client)
    col = client[DB_NAME][COLLECTION_NAME]

    # Build a lightweight in-memory catalog from existing IDs
    distinct_ids = col.distinct("restaurant_id")
    if not distinct_ids:
        seed_initial()
        distinct_ids = col.distinct("restaurant_id")

    restaurants: List[Restaurant] = []
    for rid in distinct_ids[:1000]:
        sample = col.find_one({"restaurant_id": rid})
        restaurants.append(
            Restaurant(
                restaurant_id=sample["restaurant_id"],
                restaurant_name=sample.get("restaurant_name", f"Resto {rid}"),
                neighborhood=sample.get("neighborhood", RANDOM.choice(NEIGHBORHOODS)),
                cuisine=sample.get("cuisine", RANDOM.choice(CUISINES)),
                lat=float(sample.get("lat", random_sp_location()[0])),
                lon=float(sample.get("lon", random_sp_location()[1])),
                base_stars=float(sample.get("stars", 4.0)),
            )
        )
    dish_catalog = build_dish_catalog(restaurants)

    now = int(time.time() * 1000)
    events = [make_event(restaurants, dish_catalog, now) for _ in range(events_count)]
    col.insert_many(events, ordered=False)
    print(f"[STRESS] Inseridos {events_count} eventos no MongoDB.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Popula MongoDB com dados fake para o Lab 7.")
    parser.add_argument("--stress", action="store_true", help="Insere apenas carga incremental de eventos.")
    parser.add_argument("--events", type=int, default=1000, help="Quantidade de eventos para modo stress.")
    args = parser.parse_args()

    if args.stress:
        stress_insert(events_count=args.events)
    else:
        seed_initial(restaurants_count=500, events_count=10_000)


if __name__ == "__main__":
    main()
