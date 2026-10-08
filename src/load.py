"""Carga al Data Warehouse (music_dw).

Estrategia: una sola transaccion. Las tablas preparadas se suben a tablas de
paso (stg_*) y se insertan en el modelo con INSERT ... ON CONFLICT sobre las
llaves de negocio. Repetir la carga con el mismo lote no duplica filas, y si algo
falla todo se revierte, incluidas las tablas de paso.
"""
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from settings import WORK_DIR

PREPARED_DIR = WORK_DIR / "prepared"
CHUNK = 5000

# --- Pasos de carga: (tabla destino, SQL). Orden = dependencias de claves ---
DIM_STEPS = [
    ("dim_year", """
        INSERT INTO dim_year (year_key, decade)
        SELECT year_key, decade FROM stg_dim_year
        ON CONFLICT (year_key) DO NOTHING"""),
    ("dim_category", """
        INSERT INTO dim_category (category_name)
        SELECT category_name FROM stg_dim_category
        ON CONFLICT (category_name) DO NOTHING"""),
    ("dim_genre", """
        INSERT INTO dim_genre (genre_name)
        SELECT DISTINCT track_genre FROM stg_bridge_track_genre
        ON CONFLICT (genre_name) DO NOTHING"""),
    ("dim_artist", """
        INSERT INTO dim_artist AS d
            (artist_norm, artist_name, in_spotify, in_grammy, is_placeholder)
        SELECT artist_norm, artist_name, in_spotify, in_grammy, is_placeholder
        FROM stg_dim_artist
        ON CONFLICT (artist_norm) DO UPDATE SET
            in_spotify = d.in_spotify OR EXCLUDED.in_spotify,
            in_grammy  = d.in_grammy  OR EXCLUDED.in_grammy
        WHERE d.in_spotify IS DISTINCT FROM (d.in_spotify OR EXCLUDED.in_spotify)
           OR d.in_grammy  IS DISTINCT FROM (d.in_grammy  OR EXCLUDED.in_grammy)"""),
    ("dim_track", """
        INSERT INTO dim_track AS d (track_id, explicit)
        SELECT track_id, explicit FROM stg_dim_track
        ON CONFLICT (track_id) DO UPDATE SET explicit = EXCLUDED.explicit
        WHERE d.explicit IS DISTINCT FROM EXCLUDED.explicit"""),
]

# Filas de paso que no encontrarian su clave en la dimension (deben ser 0)
ORPHAN_CHECKS = [
    ("fact_track_audio sin cancion", """
        SELECT count(*) FROM stg_fact_track_audio s
        LEFT JOIN dim_track t ON t.track_id = s.track_id
        WHERE t.track_key IS NULL"""),
    ("bridge_track_genre sin cancion o genero", """
        SELECT count(*) FROM stg_bridge_track_genre s
        LEFT JOIN dim_track t ON t.track_id = s.track_id
        LEFT JOIN dim_genre g ON g.genre_name = s.track_genre
        WHERE t.track_key IS NULL OR g.genre_key IS NULL"""),
    ("bridge_track_artist sin cancion o artista", """
        SELECT count(*) FROM stg_bridge_track_artist s
        LEFT JOIN dim_track t ON t.track_id = s.track_id
        LEFT JOIN dim_artist a ON a.artist_norm = s.artist_norm
        WHERE t.track_key IS NULL OR a.artist_key IS NULL"""),
    ("fact_nomination sin artista o categoria", """
        SELECT count(*) FROM stg_fact_nomination s
        LEFT JOIN dim_artist a ON a.artist_norm = s.artist_norm
        LEFT JOIN dim_category c ON c.category_name = s.category
        WHERE a.artist_key IS NULL OR c.category_key IS NULL"""),
]

FACT_STEPS = [
    ("bridge_track_genre", """
        INSERT INTO bridge_track_genre (track_key, genre_key)
        SELECT t.track_key, g.genre_key
        FROM stg_bridge_track_genre s
        JOIN dim_track t ON t.track_id = s.track_id
        JOIN dim_genre g ON g.genre_name = s.track_genre
        ON CONFLICT DO NOTHING"""),
    ("bridge_track_artist", """
        INSERT INTO bridge_track_artist (track_key, artist_key)
        SELECT t.track_key, a.artist_key
        FROM stg_bridge_track_artist s
        JOIN dim_track t ON t.track_id = s.track_id
        JOIN dim_artist a ON a.artist_norm = s.artist_norm
        ON CONFLICT DO NOTHING"""),
    ("fact_track_audio", """
        INSERT INTO fact_track_audio AS f
            (track_key, popularity, danceability, energy, valence, acousticness)
        SELECT t.track_key, s.popularity, s.danceability, s.energy,
               s.valence, s.acousticness
        FROM stg_fact_track_audio s
        JOIN dim_track t ON t.track_id = s.track_id
        ON CONFLICT (track_key) DO UPDATE SET
            popularity   = EXCLUDED.popularity,
            danceability = EXCLUDED.danceability,
            energy       = EXCLUDED.energy,
            valence      = EXCLUDED.valence,
            acousticness = EXCLUDED.acousticness
        WHERE (f.popularity, f.danceability, f.energy, f.valence, f.acousticness)
              IS DISTINCT FROM
              (EXCLUDED.popularity, EXCLUDED.danceability, EXCLUDED.energy,
               EXCLUDED.valence, EXCLUDED.acousticness)"""),
    ("fact_nomination", """
        INSERT INTO fact_nomination
            (nomination_id, artist_key, category_key, year_key,
             nominee, winner, artist_source)
        SELECT s.nomination_id, a.artist_key, c.category_key, s.year,
               s.nominee, s.winner, s.artist_source
        FROM stg_fact_nomination s
        JOIN dim_artist a ON a.artist_norm = s.artist_norm
        JOIN dim_category c ON c.category_name = s.category
        ON CONFLICT (nomination_id, artist_key) DO NOTHING"""),
]


def _dw_engine():
    url = URL.create(
        "postgresql+psycopg2",
        username=os.environ["DATA_DB_USER"],
        password=os.environ["DATA_DB_PASSWORD"],
        host=os.getenv("DATA_DB_HOST", "data-db"),
        port=int(os.getenv("DATA_DB_PORT", "5432")),
        database=os.environ["DW_DB_NAME"],
    )
    return create_engine(url)


def _read_prepared(folder):
    """Lee los CSV preparados. nomination_id va como texto (hash de 64 bits).
    Solo las celdas vacias cuentan como NaN: textos como 'N/A' o 'None' se conservan."""
    names = ["dim_year", "dim_category", "dim_artist", "dim_track",
             "fact_track_audio", "bridge_track_genre", "bridge_track_artist"]
    read = dict(keep_default_na=False, na_values=[""])
    data = {n: pd.read_csv(folder / f"{n}.csv", **read) for n in names}
    data["fact_nomination"] = pd.read_csv(
        folder / "fact_nomination.csv", dtype={"nomination_id": str}, **read)
    return data


def _run_steps(conn, steps, dag_run_id, summary):
    for table, sql in steps:
        changed = conn.execute(text(sql)).rowcount
        total = conn.execute(text(f"SELECT count(*) FROM {table}")).scalar()
        conn.execute(
            text("INSERT INTO etl_load_audit (dag_run_id, table_name, rows_loaded) "
                 "VALUES (:run, :tbl, :n)"),
            {"run": dag_run_id, "tbl": table, "n": changed})
        summary[table] = {"nuevas_o_cambiadas": changed, "total_en_tabla": total}
        print(f"[load] {table}: nuevas/cambiadas={changed} total_en_tabla={total}")


def load_dw(prepared_dir=None, dag_run_id="manual"):
    folder = Path(prepared_dir) if prepared_dir else PREPARED_DIR
    data = _read_prepared(folder)
    summary = {}

    # engine.begin(): todo en una transaccion; si algo falla se revierte completo
    with _dw_engine().begin() as conn:
        for name, df in data.items():
            df.to_sql(f"stg_{name}", conn, if_exists="replace", index=False,
                      chunksize=CHUNK, method="multi")
            print(f"[load] tabla de paso stg_{name}: {len(df)} filas")

        _run_steps(conn, DIM_STEPS, dag_run_id, summary)

        for label, sql in ORPHAN_CHECKS:
            orphans = conn.execute(text(sql)).scalar()
            if orphans:
                raise ValueError(f"[load] {orphans} filas de {label}. Se revierte la carga.")

        _run_steps(conn, FACT_STEPS, dag_run_id, summary)

        for name in data:
            conn.execute(text(f"DROP TABLE IF EXISTS stg_{name}"))

    print(f"[load] carga completada dag_run_id={dag_run_id}")
    return summary