import time
from typing import List, Tuple

from redis import Redis
from redis.commands.search.query import NumericFilter, Query


REDIS_HOST = "localhost"
REDIS_PORT = 6379


def top_postos_buscas(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:postos:buscas", 0, n - 1, withscores=True)


def top_postos_abastecimentos(redis: Redis, n: int = 5) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:postos:abastecimentos", 0, n - 1, withscores=True)


def top_combustiveis_buscados(redis: Redis, n: int = 6) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:combustivel:buscas", 0, n - 1, withscores=True)


def top_bairros_buscas(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:bairro:buscas", 0, n - 1, withscores=True)


def menor_preco_gasolina(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    """Retorna postos com menor preço de gasolina comum (Sorted Set ordenado por preço)."""
    return redis.zrange("ranking:preco:gasolina_comum", 0, n - 1, withscores=True)


def posto_nome(redis: Redis, posto_id: str) -> str:
    import re
    match = re.search(r"(\d+)$", posto_id or "")
    num = match.group(1) if match else posto_id
    name = redis.hget(f"posto:{num}", "posto_nome")
    return name or posto_id


def shell_pinheiros(redis: Redis):
    """Busca postos Shell em Pinheiros com nota >= 4.0."""
    query = (
        Query("@bandeira:{Shell} @bairro:{Pinheiros}")
        .add_filter(NumericFilter("nota", 4.0, 5))
        .sort_by("buscas", asc=False)
        .paging(0, 10)
    )
    return redis.ft("idx:postos").search(query)


def preco_series(redis: Redis, posto_numeric_id: str = "1", combustivel: str = "gasolina_comum"):
    key = f"ts:posto:{posto_numeric_id}:preco:{combustivel}"
    return redis.execute_command("TS.RANGE", key, "-", "+", "AGGREGATION", "last", "60000")


def print_block(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> None:
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    print("[READER] Consultas em tempo real do Radar Combustível iniciadas.")

    while True:
        print_block("Top 10 postos mais buscados")
        for idx, (member, score) in enumerate(top_postos_buscas(redis), start=1):
            nome = posto_nome(redis, member)
            print(f"{idx:02d}. {nome} ({member}) -> {int(score)} buscas")

        print_block("Top 5 postos com mais abastecimentos")
        for idx, (member, score) in enumerate(top_postos_abastecimentos(redis), start=1):
            nome = posto_nome(redis, member)
            print(f"{idx:02d}. {nome} ({member}) -> {int(score)} abastecimentos")

        print_block("Combustíveis mais buscados")
        for idx, (member, score) in enumerate(top_combustiveis_buscados(redis), start=1):
            print(f"{idx:02d}. {member} -> {int(score)} buscas")

        print_block("Top 10 bairros com mais buscas")
        for idx, (member, score) in enumerate(top_bairros_buscas(redis), start=1):
            print(f"{idx:02d}. {member} -> {int(score)} buscas")

        print_block("Top 10 menor preço gasolina comum")
        for idx, (member, score) in enumerate(menor_preco_gasolina(redis), start=1):
            nome = posto_nome(redis, member)
            print(f"{idx:02d}. {nome} ({member}) -> R$ {score:.2f}")

        print_block("Postos Shell em Pinheiros com nota >= 4.0 (RediSearch)")
        try:
            result = shell_pinheiros(redis)
            if result.total == 0:
                print("Nenhum resultado para @bandeira:{Shell} @bairro:{Pinheiros}.")
            else:
                for doc in result.docs[:10]:
                    print(
                        f"{doc.id} | {getattr(doc, 'posto_nome', '-')}"
                        f" | nota={getattr(doc, 'nota', '-')}"
                        f" | buscas={getattr(doc, 'buscas', '-')}"
                    )
        except Exception as exc:
            print(f"Falha na busca RediSearch: {exc}")

        print_block("Série temporal de preço gasolina comum - posto 1 (por minuto)")
        try:
            series = preco_series(redis, "1", "gasolina_comum")
            if not series:
                print("Sem dados de série temporal para ts:posto:1:preco:gasolina_comum.")
            else:
                for point in series[-10:]:
                    ts, value = point
                    print(f"{ts} -> R$ {float(value):.2f}")
        except Exception as exc:
            print(f"Falha na TimeSeries: {exc}")

        time.sleep(5)


if __name__ == "__main__":
    main()
