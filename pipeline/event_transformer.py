import re
from typing import Any, Dict


def _extract_numeric_id(value: str) -> str:
    match = re.search(r"(\d+)$", value or "")
    return match.group(1) if match else value


def normalize_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    event_type = str(raw.get("type", "")).strip().lower()
    posto_id = str(raw.get("posto_id", "")).strip()
    posto_num = _extract_numeric_id(posto_id)

    ts = int(raw.get("ts") or 0)
    if ts <= 0:
        raise ValueError("Evento sem timestamp válido (ms).")

    event = {
        "type": event_type,
        "ts": ts,
        "user_id": str(raw.get("user_id", "")),
        "posto_id": posto_id,
        "posto_num": posto_num,
        "posto_nome": str(raw.get("posto_nome", "")),
        "bandeira": str(raw.get("bandeira", "")),
        "combustivel": str(raw.get("combustivel", "")),
        "preco": float(raw.get("preco", 0.0)),
        "bairro": str(raw.get("bairro", "")),
        "cidade": str(raw.get("cidade", "")),
        "lat": float(raw.get("lat", 0.0)),
        "lon": float(raw.get("lon", 0.0)),
        "nota": float(raw.get("nota", 0.0)),
    }
    return event


def hash_key(event: Dict[str, Any]) -> str:
    return f"posto:{event['posto_num']}"


def ts_key(event: Dict[str, Any], metric: str) -> str:
    return f"ts:posto:{event['posto_num']}:{metric}"


def preco_ts_key(event: Dict[str, Any]) -> str:
    return f"ts:posto:{event['posto_num']}:preco:{event['combustivel']}"


def ranking_key(event: Dict[str, Any]) -> str:
    if event["type"] == "busca":
        return "ranking:postos:buscas"
    if event["type"] == "abastecimento":
        return "ranking:postos:abastecimentos"
    return ""
