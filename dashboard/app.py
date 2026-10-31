import os

import altair as alt
import duckdb
import streamlit as st

GOLD_PATH = "s3://datalake/gold"


def connect():
    """Connecte DuckDB au stockage S3 (RustFS)."""
    con = duckdb.connect()
    endpoint = os.environ["S3_ENDPOINT"].replace("http://", "")
    con.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{os.environ["S3_ACCESS_KEY"]}',
            SECRET '{os.environ["S3_SECRET_KEY"]}',
            ENDPOINT '{endpoint}',
            URL_STYLE 'path',
            USE_SSL false
        )
    """)
    return con


def read_table(con, name):
    return con.sql(f"SELECT * FROM read_parquet('{GOLD_PATH}/{name}/*.parquet')").df()


def format_number(value):
    return f"{value:,.0f}".replace(",", " ")


st.set_page_config(page_title="Taxis de Chicago", layout="wide")
st.title("Taxis de Chicago : T1 2023")

con = connect()
try:
    revenue = read_table(con, "revenue_per_day")
    duration = read_table(con, "avg_trip_duration_per_day")
    zones = read_table(con, "top_pickup_zones")
except duckdb.Error:
    st.warning("Pas encore de données : lance le DAG taxi_pipeline dans Airflow, puis recharge cette page.")
    st.stop()

trips_total = revenue["trip_count"].sum()
revenue_total = revenue["revenue"].sum()

col1, col2, col3 = st.columns(3)
col1.metric("Trajets", format_number(trips_total))
col2.metric("Revenu total", f"{format_number(revenue_total)} $")
col3.metric("Revenu moyen par trajet", f"{revenue_total / trips_total:.2f} $")

st.subheader("Revenu par jour ($)")
st.line_chart(revenue, x="trip_date", y="revenue", x_label="Date", y_label="Revenu ($)")

st.subheader("Durée moyenne des trajets par jour (minutes)")
st.line_chart(duration, x="trip_date", y="avg_duration_minutes", x_label="Date", y_label="Durée (min)")

st.subheader("Top 10 des zones de prise en charge")
chart = (
    alt.Chart(zones)
    .mark_bar()
    .encode(
        x=alt.X("trip_count", title="Nombre de trajets"),
        y=alt.Y("community_name", title=None, sort="-x"),
        tooltip=["community_name", "trip_count", "revenue"],
    )
)
st.altair_chart(chart, use_container_width=True)
