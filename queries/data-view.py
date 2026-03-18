import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from redis import Redis
from redis.commands.search.query import NumericFilter, Query

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

COMBUSTIVEIS_LABEL = {
    "gasolina_comum": "Gasolina Comum",
    "gasolina_aditivada": "Gasolina Aditivada",
    "etanol": "Etanol",
    "diesel": "Diesel",
    "diesel_s10": "Diesel S10",
    "gnv": "GNV",
}


def get_redis() -> Redis:
    return Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def extract_numeric_id(value: str) -> str:
    match = re.search(r"(\d+)$", value or "")
    return match.group(1) if match else value


def resolve_posto_names(redis: Redis, posto_ids: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for pid in posto_ids:
        numeric = extract_numeric_id(pid)
        name = redis.hget(f"posto:{numeric}", "posto_nome")
        out[pid] = name or pid
    return out


# ── Consultas Redis ──────────────────────────────────────────────────────────

def top_postos_buscas(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:postos:buscas", 0, n - 1, withscores=True)


def top_postos_abastecimentos(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:postos:abastecimentos", 0, n - 1, withscores=True)


def top_combustiveis(redis: Redis, n: int = 6) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:combustivel:buscas", 0, n - 1, withscores=True)


def top_bairros(redis: Redis, n: int = 15) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:bairro:buscas", 0, n - 1, withscores=True)


def menor_preco(redis: Redis, combustivel: str, n: int = 10) -> List[Tuple[str, float]]:
    return redis.zrange(f"ranking:preco:{combustivel}", 0, n - 1, withscores=True)


def top_rated_postos(redis: Redis, n: int = 10) -> Any:
    query = Query("*").sort_by("nota", asc=False).paging(0, n)
    return redis.ft("idx:postos").search(query)


def search_postos(
    redis: Redis,
    bandeira: str,
    bairro: str,
    min_nota: float,
    limit: int,
) -> Any:
    bandeira = bandeira.strip()
    bairro = bairro.strip()
    query_parts = []
    if bandeira:
        query_parts.append(f"@bandeira:{{{bandeira}}}")
    if bairro:
        query_parts.append(f"@bairro:{{{bairro}}}")
    query_text = " ".join(query_parts) if query_parts else "*"

    query = (
        Query(query_text)
        .add_filter(NumericFilter("nota", min_nota, 5))
        .sort_by("buscas", asc=False)
        .paging(0, limit)
    )
    return redis.ft("idx:postos").search(query)


def preco_series(redis: Redis, posto_numeric_id: str, combustivel: str) -> List[Tuple[int, float]]:
    key = f"ts:posto:{posto_numeric_id}:preco:{combustivel}"
    return redis.execute_command("TS.RANGE", key, "-", "+", "AGGREGATION", "last", "60000")


# ── Dashboard Streamlit ──────────────────────────────────────────────────────

st.set_page_config(page_title="Radar Combustível — Dashboard", layout="wide")
st.title("⛽ Radar Combustível — Dashboard Redis")
st.caption("Visualização em tempo real dos dados servidos pelo pipeline MongoDB → Redis.")

auto_refresh = st.sidebar.toggle("Auto-refresh", value=True)
refresh_seconds = st.sidebar.number_input("Intervalo (segundos)", min_value=1, max_value=60, value=5, step=1)

redis = get_redis()

# ── Linha 1: Postos mais buscados + Combustíveis mais buscados ───────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("🔍 Top 10 postos mais buscados")
    postos_buscas = top_postos_buscas(redis, 10)
    df_buscas = pd.DataFrame(postos_buscas, columns=["posto_id", "buscas"])
    if df_buscas.empty:
        st.info("Sem dados em `ranking:postos:buscas`.")
    else:
        names = resolve_posto_names(redis, df_buscas["posto_id"].tolist())
        df_buscas["posto_nome"] = df_buscas["posto_id"].map(names)
        df_buscas["buscas"] = df_buscas["buscas"].astype(int)
        fig = px.bar(
            df_buscas.sort_values("buscas", ascending=True),
            x="buscas",
            y="posto_nome",
            orientation="h",
            title="Buscas por posto",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_buscas[["posto_nome", "buscas"]], use_container_width=True, hide_index=True)

with col2:
    st.subheader("🛢️ Combustíveis mais buscados")
    combs = top_combustiveis(redis, 6)
    df_combs = pd.DataFrame(combs, columns=["combustivel", "buscas"])
    if df_combs.empty:
        st.info("Sem dados em `ranking:combustivel:buscas`.")
    else:
        df_combs["label"] = df_combs["combustivel"].map(
            lambda x: COMBUSTIVEIS_LABEL.get(x, x)
        )
        df_combs["buscas"] = df_combs["buscas"].astype(int)
        fig = px.pie(df_combs, names="label", values="buscas", title="Participação de buscas por combustível")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_combs[["label", "buscas"]], use_container_width=True, hide_index=True)

# ── Linha 2: Bairros + Ranking de menor preço ────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("📍 Bairros com mais buscas")
    bairros = top_bairros(redis, 15)
    df_bairros = pd.DataFrame(bairros, columns=["bairro", "buscas"])
    if df_bairros.empty:
        st.info("Sem dados em `ranking:bairro:buscas`.")
    else:
        df_bairros["buscas"] = df_bairros["buscas"].astype(int)
        fig = px.bar(
            df_bairros.sort_values("buscas", ascending=True),
            x="buscas",
            y="bairro",
            orientation="h",
            title="Buscas por bairro",
        )
        st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("💰 Ranking de menor preço")
    comb_selecionado = st.selectbox(
        "Combustível",
        list(COMBUSTIVEIS_LABEL.keys()),
        format_func=lambda x: COMBUSTIVEIS_LABEL[x],
    )
    menores = menor_preco(redis, comb_selecionado, 10)
    df_menores = pd.DataFrame(menores, columns=["posto_id", "preco"])
    if df_menores.empty:
        st.info(f"Sem dados em `ranking:preco:{comb_selecionado}`.")
    else:
        names = resolve_posto_names(redis, df_menores["posto_id"].tolist())
        df_menores["posto_nome"] = df_menores["posto_id"].map(names)
        df_menores["preco"] = df_menores["preco"].apply(lambda x: round(float(x), 2))
        fig = px.bar(
            df_menores,
            x="preco",
            y="posto_nome",
            orientation="h",
            title=f"Menor preço — {COMBUSTIVEIS_LABEL[comb_selecionado]}",
            text="preco",
        )
        fig.update_traces(texttemplate="R$ %{text:.2f}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            df_menores[["posto_nome", "preco"]].rename(columns={"preco": "R$/litro"}),
            use_container_width=True,
            hide_index=True,
        )

# ── Maior variação de preço ───────────────────────────────────────────────────
st.subheader("📊 Postos com maior variação recente de preço")
var_col1, var_col2 = st.columns(2)
with var_col1:
    comb_variacao = st.selectbox(
        "Combustível (variação)",
        list(COMBUSTIVEIS_LABEL.keys()),
        format_func=lambda x: COMBUSTIVEIS_LABEL[x],
        key="var_comb",
    )
with var_col2:
    n_variacao = st.number_input("Quantidade", min_value=1, max_value=30, value=10, step=1, key="var_n")

variacao_data = redis.zrevrange(f"ranking:variacao:{comb_variacao}", 0, int(n_variacao) - 1, withscores=True)
df_var = pd.DataFrame(variacao_data, columns=["posto_id", "variacao_abs"])
if df_var.empty:
    st.info(f"Sem dados de variação para `ranking:variacao:{comb_variacao}`.")
else:
    var_names = resolve_posto_names(redis, df_var["posto_id"].tolist())
    df_var["posto_nome"] = df_var["posto_id"].map(var_names)
    df_var["variacao_abs"] = df_var["variacao_abs"].apply(lambda x: round(float(x), 2))
    # Busca variação com sinal do hash
    var_sinais = []
    for pid in df_var["posto_id"]:
        num = extract_numeric_id(pid)
        v = redis.hget(f"posto:{num}", f"variacao_{comb_variacao}")
        var_sinais.append(float(v) if v else 0.0)
    df_var["variacao_R$"] = [round(v, 2) for v in var_sinais]
    df_var["direcao"] = df_var["variacao_R$"].apply(lambda x: "Subiu" if x > 0 else ("Desceu" if x < 0 else "Estável"))

    fig = px.bar(
        df_var.sort_values("variacao_abs", ascending=True),
        x="variacao_abs",
        y="posto_nome",
        orientation="h",
        color="direcao",
        color_discrete_map={"Subiu": "#ef4444", "Desceu": "#22c55e", "Estável": "#94a3b8"},
        title=f"Maior variação — {COMBUSTIVEIS_LABEL[comb_variacao]}",
        text="variacao_R$",
    )
    fig.update_traces(texttemplate="R$ %{text:.2f}", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(
        df_var[["posto_nome", "variacao_R$", "direcao"]],
        use_container_width=True,
        hide_index=True,
    )

# ── Top avaliados ────────────────────────────────────────────────────────────
st.subheader("⭐ Top 10 postos mais bem avaliados")
try:
    rated = top_rated_postos(redis, 10)
    rated_rows: List[Dict[str, Any]] = []
    for doc in rated.docs:
        rated_rows.append(
            {
                "id": doc.id,
                "posto_nome": getattr(doc, "posto_nome", "-"),
                "nota": float(getattr(doc, "nota", 0)),
                "bandeira": getattr(doc, "bandeira", "-"),
                "bairro": getattr(doc, "bairro", "-"),
                "buscas": int(float(getattr(doc, "buscas", 0))),
            }
        )
    df_rated = pd.DataFrame(rated_rows)
    if df_rated.empty:
        st.info("Sem dados no índice `idx:postos` para ranking por nota.")
    else:
        fig = px.bar(
            df_rated.sort_values("nota", ascending=True),
            x="nota",
            y="posto_nome",
            orientation="h",
            color="bandeira",
            title="Top 10 por nota",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_rated, use_container_width=True, hide_index=True)
except Exception as exc:
    st.error(f"Falha ao montar top avaliados: {exc}")

# ── Busca dinâmica (RediSearch) ──────────────────────────────────────────────
st.subheader("🔎 Busca dinâmica de postos (RediSearch)")
f1, f2, f3, f4 = st.columns(4)
with f1:
    bandeira_filter = st.text_input("Bandeira", value="Shell")
with f2:
    bairro_filter = st.text_input("Bairro", value="Pinheiros")
with f3:
    min_nota_filter = st.slider("Nota mínima", min_value=0.0, max_value=5.0, value=3.0, step=0.1)
with f4:
    limit_filter = st.number_input("Limite", min_value=1, max_value=50, value=10, step=1)

try:
    result = search_postos(
        redis,
        bandeira_filter,
        bairro_filter,
        float(min_nota_filter),
        int(limit_filter),
    )
    rows: List[Dict[str, Any]] = []
    for doc in result.docs:
        rows.append(
            {
                "id": doc.id,
                "posto_nome": getattr(doc, "posto_nome", "-"),
                "nota": float(getattr(doc, "nota", 0)),
                "bandeira": getattr(doc, "bandeira", "-"),
                "bairro": getattr(doc, "bairro", "-"),
                "buscas": int(float(getattr(doc, "buscas", 0))),
            }
        )
    df_search = pd.DataFrame(rows)
    if df_search.empty:
        st.info(
            "Nenhum resultado para os filtros atuais "
            f"(bandeira={bandeira_filter or '*'}, bairro={bairro_filter or '*'}, "
            f"nota>={min_nota_filter})."
        )
    else:
        st.caption(f"{result.total} resultado(s) encontrado(s).")
        st.dataframe(df_search, use_container_width=True, hide_index=True)
except Exception as exc:
    st.error(f"Falha na busca RediSearch: {exc}")

# ── Série temporal de preço ──────────────────────────────────────────────────
st.subheader("📈 Evolução de preço (TimeSeries)")
ts_col1, ts_col2 = st.columns(2)
with ts_col1:
    posto_num = st.text_input("ID numérico do posto", value="1")
with ts_col2:
    comb_ts = st.selectbox(
        "Combustível (TimeSeries)",
        list(COMBUSTIVEIS_LABEL.keys()),
        format_func=lambda x: COMBUSTIVEIS_LABEL[x],
        key="ts_comb",
    )

try:
    series = preco_series(redis, posto_num, comb_ts)
    if not series:
        st.info(f"Sem dados de TimeSeries para `ts:posto:{posto_num}:preco:{comb_ts}`.")
    else:
        df_series = pd.DataFrame(series, columns=["ts", "preco"])
        df_series["preco"] = df_series["preco"].astype(float)
        df_series["datetime"] = df_series["ts"].apply(lambda v: datetime.fromtimestamp(v / 1000.0))
        fig = px.line(
            df_series,
            x="datetime",
            y="preco",
            markers=True,
            title=f"Posto {posto_num} — {COMBUSTIVEIS_LABEL[comb_ts]}",
            labels={"preco": "R$/litro", "datetime": "Data/Hora"},
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            df_series[["datetime", "preco"]].tail(15).rename(columns={"preco": "R$/litro"}),
            use_container_width=True,
            hide_index=True,
        )
except Exception as exc:
    st.error(f"Falha na TimeSeries: {exc}")

if auto_refresh:
    time.sleep(int(refresh_seconds))
    st.rerun()
