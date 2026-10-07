
"""Extraccion de las dos fuentes. No limpia ni corrige datos: solo selecciona
las columnas necesarias y deja un archivo de trabajo para la validacion cruda."""
import os

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from settings import (
    GRAMMY_COLUMNS, GRAMMY_RAW, SPOTIFY_COLUMNS, SPOTIFY_RAW,
    SPOTIFY_SOURCE, WORK_DIR,
)


def _keep_columns(df, wanted, name):
    """Conserva las columnas pedidas que existan. Si falta alguna NO falla aqui:
    lo reporta en el log y la regla de validacion cruda decide (Critical)."""
    present = [c for c in wanted if c in df.columns]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        print(f"[{name}] WARNING: faltan columnas requeridas en la fuente: {missing}")
    return df[present].copy()


def extract_spotify():
    """Lee el CSV de Spotify y escribe el archivo de trabajo crudo."""
    if not SPOTIFY_SOURCE.exists():
        raise FileNotFoundError(f"No existe la fuente de Spotify: {SPOTIFY_SOURCE}")
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(SPOTIFY_SOURCE)
    print(f"[spotify] fuente={SPOTIFY_SOURCE} filas={len(df)} columnas={df.shape[1]}")

    raw = _keep_columns(df, SPOTIFY_COLUMNS, "spotify")
    raw.to_csv(SPOTIFY_RAW, index=False)
    print(f"[spotify] archivo crudo={SPOTIFY_RAW} filas={len(raw)} columnas={raw.shape[1]}")
    return str(SPOTIFY_RAW)


def _grammy_engine():
    url = URL.create(
        "postgresql+psycopg2",
        username=os.environ["DATA_DB_USER"],
        password=os.environ["DATA_DB_PASSWORD"],
        host=os.getenv("DATA_DB_HOST", "data-db"),
        port=int(os.getenv("DATA_DB_PORT", "5432")),
        database=os.environ["GRAMMY_DB_NAME"],
    )
    return create_engine(url)


def extract_grammys():
    """Lee Grammy desde la base de datos fuente (NO desde el CSV)."""
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_sql("SELECT * FROM grammy_awards", _grammy_engine())
    print(f"[grammy] fuente=PostgreSQL {os.environ['GRAMMY_DB_NAME']}.grammy_awards "
          f"filas={len(df)} columnas={df.shape[1]}")

    raw = _keep_columns(df, GRAMMY_COLUMNS, "grammy")
    raw.to_csv(GRAMMY_RAW, index=False)
    print(f"[grammy] archivo crudo={GRAMMY_RAW} filas={len(raw)} columnas={raw.shape[1]}")
    return str(GRAMMY_RAW)
