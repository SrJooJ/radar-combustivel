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
DB_NAME = "radar_combustivel"
COLLECTION_NAME = "eventos"

BAIRROS = [
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
    "Lapa",
    "Bela Vista",
    "Vila Mariana",
    "Liberdade",
    "Butantã",
]

BANDEIRAS = [
    "Shell",
    "BR",
    "Ipiranga",
    "Petrobrás",
    "Ale",
    "Raízen",
    "Total",
    "Texaco",
]

COMBUSTIVEIS = [
    "gasolina_comum",
    "gasolina_aditivada",
    "etanol",
    "diesel",
    "diesel_s10",
    "gnv",
]

# Faixas de preço realistas (R$/litro ou R$/m³ para GNV)
FAIXA_PRECO = {
    "gasolina_comum": (5.49, 6.59),
    "gasolina_aditivada": (5.89, 6.99),
    "etanol": (3.49, 4.59),
    "diesel": (5.79, 6.89),
    "diesel_s10": (5.99, 7.09),
    "gnv": (3.79, 4.89),
}


@dataclass
class Posto:
    posto_id: str
    posto_nome: str
    bandeira: str
    bairro: str
    cidade: str
    lat: float
    lon: float
    nota_base: float


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
            pass


def random_sp_location() -> tuple[float, float]:
    lat = -23.70 + RANDOM.random() * 0.30
    lon = -46.80 + RANDOM.random() * 0.30
    return round(lat, 6), round(lon, 6)


def build_postos(count: int) -> List[Posto]:
    postos: List[Posto] = []
    for i in range(1, count + 1):
        bandeira = RANDOM.choice(BANDEIRAS)
        bairro = RANDOM.choice(BAIRROS)
        lat, lon = random_sp_location()
        postos.append(
            Posto(
                posto_id=f"posto_{i}",
                posto_nome=f"Posto {bandeira} {fake.street_name()[:20]}",
                bandeira=bandeira,
                bairro=bairro,
                cidade="São Paulo",
                lat=lat,
                lon=lon,
                nota_base=round(RANDOM.uniform(2.5, 5.0), 1),
            )
        )
    return postos


def build_precos_postos(postos: List[Posto]) -> Dict[str, Dict[str, float]]:
    """Gera preços base por combustível para cada posto."""
    precos: Dict[str, Dict[str, float]] = {}
    for p in postos:
        precos_posto: Dict[str, float] = {}
        for comb in COMBUSTIVEIS:
            minimo, maximo = FAIXA_PRECO[comb]
            precos_posto[comb] = round(RANDOM.uniform(minimo, maximo), 2)
        precos[p.posto_id] = precos_posto
    return precos


def make_event(postos: List[Posto], precos: Dict[str, Dict[str, float]], base_ts: int) -> dict:
    p = RANDOM.choice(postos)
    event_type = RANDOM.choices(
        ["busca", "atualizacao_preco", "abastecimento", "avaliacao"],
        weights=[40, 20, 25, 15],
        k=1,
    )[0]
    ts = base_ts + RANDOM.randint(0, 3_600_000)
    combustivel = RANDOM.choice(COMBUSTIVEIS)
    preco_base = precos[p.posto_id][combustivel]

    event = {
        "type": event_type,
        "ts": ts,
        "user_id": f"usr_{RANDOM.randint(1, 15000)}",
        "posto_id": p.posto_id,
        "posto_nome": p.posto_nome,
        "bandeira": p.bandeira,
        "combustivel": combustivel,
        "preco": preco_base,
        "bairro": p.bairro,
        "cidade": p.cidade,
        "lat": p.lat,
        "lon": p.lon,
        "nota": p.nota_base,
    }

    if event_type == "atualizacao_preco":
        # Variação de preço de -5% a +5%
        variacao = RANDOM.uniform(-0.05, 0.05)
        event["preco"] = round(preco_base * (1 + variacao), 2)

    if event_type == "avaliacao":
        event["nota"] = round(RANDOM.uniform(1.0, 5.0), 1)

    return event


def seed_initial(postos_count: int = 300, events_count: int = 10_000) -> None:
    client = get_client_with_fallback()
    ensure_replicaset(client)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]

    col.delete_many({})
    col.create_index("ts")
    col.create_index("type")
    col.create_index("posto_id")
    col.create_index("combustivel")
    col.create_index("bairro")

    postos = build_postos(postos_count)
    precos = build_precos_postos(postos)

    base_ts = int(time.time() * 1000) - 86_400_000
    events = [make_event(postos, precos, base_ts) for _ in range(events_count)]
    col.insert_many(events, ordered=False)

    print(f"[SEED] MongoDB populado com {postos_count} postos e {events_count} eventos fake.")
    print(f"[SEED] Database: {DB_NAME} | Collection: {COLLECTION_NAME}")


def stress_insert(events_count: int = 1000) -> None:
    client = get_client_with_fallback()
    ensure_replicaset(client)
    col = client[DB_NAME][COLLECTION_NAME]

    distinct_ids = col.distinct("posto_id")
    if not distinct_ids:
        seed_initial()
        distinct_ids = col.distinct("posto_id")

    postos: List[Posto] = []
    for pid in distinct_ids[:1000]:
        sample = col.find_one({"posto_id": pid})
        postos.append(
            Posto(
                posto_id=sample["posto_id"],
                posto_nome=sample.get("posto_nome", f"Posto {pid}"),
                bandeira=sample.get("bandeira", RANDOM.choice(BANDEIRAS)),
                bairro=sample.get("bairro", RANDOM.choice(BAIRROS)),
                cidade=sample.get("cidade", "São Paulo"),
                lat=float(sample.get("lat", random_sp_location()[0])),
                lon=float(sample.get("lon", random_sp_location()[1])),
                nota_base=float(sample.get("nota", 4.0)),
            )
        )
    precos = build_precos_postos(postos)

    now = int(time.time() * 1000)
    events = [make_event(postos, precos, now) for _ in range(events_count)]
    col.insert_many(events, ordered=False)
    print(f"[STRESS] Inseridos {events_count} eventos no MongoDB.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Popula MongoDB com dados fake do Radar Combustível.")
    parser.add_argument("--stress", action="store_true", help="Insere apenas carga incremental de eventos.")
    parser.add_argument("--events", type=int, default=1000, help="Quantidade de eventos para modo stress.")
    args = parser.parse_args()

    if args.stress:
        stress_insert(events_count=args.events)
    else:
        seed_initial(postos_count=300, events_count=10_000)


if __name__ == "__main__":
    main()
