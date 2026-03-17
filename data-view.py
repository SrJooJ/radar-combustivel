import os
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


def get_redis() -> Redis:
    return Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def top_restaurants(redis: Redis, n: int = 10) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:restaurants:views", 0, n - 1, withscores=True)


def top_dishes(redis: Redis, n: int = 5) -> List[Tuple[str, float]]:
    return redis.zrevrange("ranking:dishes:searches", 0, n - 1, withscores=True)


def search_restaurants(
    redis: Redis,
    cuisine: str,
    neighborhood: str,
    min_stars: float,
    limit: int,
) -> Any:
    cuisine = cuisine.strip()
    neighborhood = neighborhood.strip()
    query_parts = []
    if cuisine:
        query_parts.append(f"@cuisine:{{{cuisine}}}")
    if neighborhood:
        query_parts.append(f"@neighborhood:{{{neighborhood}}}")
    query_text = " ".join(query_parts) if query_parts else "*"

    query = (
        Query(query_text)
        .add_filter(NumericFilter("stars", min_stars, 5))
        .sort_by("views", asc=False)
        .paging(0, limit)
    )
    return redis.ft("idx:restaurants").search(query)


def views_series(redis: Redis, restaurant_numeric_id: str) -> List[Tuple[int, int]]:
    key = f"ts:resto:{restaurant_numeric_id}:views"
    return redis.execute_command("TS.RANGE", key, "-", "+", "AGGREGATION", "sum", "60000")


st.set_page_config(page_title="Marketplace Redis Dashboard", layout="wide")
st.title("🍽️ Marketplace de Restaurantes — Redis Dashboard")
st.caption("Visualização em tempo real das estruturas alimentadas pelo consumidor MongoDB -> Redis.")

redis = get_redis()

col1, col2 = st.columns(2)

with col1:
    st.subheader("🏆 Top 10 restaurantes mais visitados")
    restaurants = top_restaurants(redis, 10)
    df_restaurants = pd.DataFrame(restaurants, columns=["restaurant_id", "views"])
    if df_restaurants.empty:
        st.info("Sem dados em `ranking:restaurants:views`.")
    else:
        df_restaurants["views"] = df_restaurants["views"].astype(int)
        fig = px.bar(
            df_restaurants.sort_values("views", ascending=True),
            x="views",
            y="restaurant_id",
            orientation="h",
            title="Views por restaurante",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_restaurants, use_container_width=True, hide_index=True)

with col2:
    st.subheader("🔎 Top 5 pratos mais buscados")
    dishes = top_dishes(redis, 5)
    df_dishes = pd.DataFrame(dishes, columns=["dish_id", "searches"])
    if df_dishes.empty:
        st.info("Sem dados em `ranking:dishes:searches`.")
    else:
        df_dishes["searches"] = df_dishes["searches"].astype(int)
        fig = px.pie(df_dishes, names="dish_id", values="searches", title="Participação das buscas")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_dishes, use_container_width=True, hide_index=True)

st.subheader("🔍 Busca dinâmica de restaurantes (RediSearch)")
f1, f2, f3, f4 = st.columns(4)
with f1:
    cuisine_filter = st.text_input("Cuisine", value="pizza")
with f2:
    neighborhood_filter = st.text_input("Bairro", value="Pinheiros")
with f3:
    min_stars_filter = st.slider("Nota mínima", min_value=0.0, max_value=5.0, value=4.5, step=0.1)
with f4:
    limit_filter = st.number_input("Limite", min_value=1, max_value=50, value=10, step=1)

try:
    result = search_restaurants(
        redis,
        cuisine_filter,
        neighborhood_filter,
        float(min_stars_filter),
        int(limit_filter),
    )
    rows: List[Dict[str, Any]] = []
    for doc in result.docs:
        rows.append(
            {
                "id": doc.id,
                "restaurant_name": getattr(doc, "restaurant_name", "-"),
                "stars": float(getattr(doc, "stars", 0)),
                "views": int(float(getattr(doc, "views", 0))),
                "cuisine": getattr(doc, "cuisine", "-"),
                "neighborhood": getattr(doc, "neighborhood", "-"),
            }
        )
    df_search = pd.DataFrame(rows)
    if df_search.empty:
        st.info(
            "Nenhum resultado para os filtros atuais "
            f"(cuisine={cuisine_filter or '*'}, bairro={neighborhood_filter or '*'}, "
            f"stars>={min_stars_filter})."
        )
    else:
        st.caption(f"{result.total} resultado(s) encontrado(s).")
        st.dataframe(df_search, use_container_width=True, hide_index=True)
except Exception as exc:
    st.error(f"Falha na busca RediSearch: {exc}")

st.subheader("📈 Série temporal de views por minuto")
restaurant_num = st.text_input("ID numérico do restaurante", value="245")
try:
    series = views_series(redis, restaurant_num)
    if not series:
        st.info(f"Sem dados de TimeSeries para `ts:resto:{restaurant_num}:views`.")
    else:
        df_series = pd.DataFrame(series, columns=["ts", "views"])
        df_series["views"] = df_series["views"].astype(int)
        df_series["datetime"] = df_series["ts"].apply(lambda v: datetime.fromtimestamp(v / 1000.0))
        fig = px.line(df_series, x="datetime", y="views", markers=True, title=f"Restaurante {restaurant_num}")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_series[["ts", "views"]].tail(15), use_container_width=True, hide_index=True)
except Exception as exc:
    st.error(f"Falha na TimeSeries: {exc}")
