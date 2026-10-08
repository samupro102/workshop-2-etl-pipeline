"""Transformación e integración: construye las tablas del modelo estrella.

Lee los archivos crudos de data/work/ y escribe tablas preparadas en
data/work/prepared/. Pasa rutas, no datos, entre tareas de Airflow.
"""
import logging
import re
import unicodedata
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

WORK_DIR = Path("/opt/airflow/data/work")
PREPARED_DIR = WORK_DIR / "prepared"

AUDIO_COLS = ["danceability", "energy", "valence", "acousticness"]


def normalize_name(name) -> str:
    """Minúsculas, sin tildes, sin signos; espacios colapsados."""
    if not isinstance(name, str):
        return ""
    text = unicodedata.normalize("NFKD", name)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-z0-9 ]", " ", text.lower())
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"^the ", "", text)


def build_spotify_tables() -> dict:
    """Construye dim_track, fact_track_audio y las puentes de Spotify."""
    PREPARED_DIR.mkdir(parents=True, exist_ok=True)
    raw = pd.read_csv(WORK_DIR / "spotify_raw.csv")
    log.info("Spotify crudo: %d filas, %d track_id distintos",
             len(raw), raw["track_id"].nunique())

    # --- Grano: una fila por track_id ---
    grouped = raw.groupby("track_id")
    fact = grouped.agg(
        popularity=("popularity", "max"),          # decisión aprobada: máximo
        explicit=("explicit", "first"),
        **{c: (c, "first") for c in AUDIO_COLS},
    ).reset_index()

    # Control: ¿el audio es el mismo en todas las filas de una canción?
    audio_diff = (grouped[AUDIO_COLS].nunique() > 1).any(axis=1).sum()
    log.info("track_id con audio distinto entre filas: %d", audio_diff)

    dim_track = fact[["track_id", "explicit"]].copy()
    fact_audio = fact.drop(columns=["explicit"])

    # --- Puente canción-género (los duplicados exactos se absorben) ---
    bridge_genre = (raw[["track_id", "track_genre"]]
                    .dropna().drop_duplicates())

    # --- Puente canción-artista (artists viene separado por ';') ---
    pairs = raw[["track_id", "artists"]].dropna().copy()
    pairs["artist_name"] = pairs["artists"].str.split(";")
    pairs = pairs.explode("artist_name")
    pairs["artist_name"] = pairs["artist_name"].str.strip()
    pairs["artist_norm"] = pairs["artist_name"].map(normalize_name)
    pairs = pairs[pairs["artist_norm"] != ""]
    bridge_artist = pairs[["track_id", "artist_norm"]].drop_duplicates()

    spotify_artists = (pairs.drop_duplicates("artist_norm")
                       [["artist_norm", "artist_name"]])

    # --- Guardar y reportar ---
    outputs = {
        "dim_track": dim_track,
        "fact_track_audio": fact_audio,
        "bridge_track_genre": bridge_genre,
        "bridge_track_artist": bridge_artist,
        "spotify_artists": spotify_artists,
    }
    counts = {}
    for name, df in outputs.items():
        df.to_csv(PREPARED_DIR / f"{name}.csv", index=False)
        counts[name] = len(df)
        log.info("%s: %d filas", name, len(df))
    return counts
def _recover_artist(row):
    """Devuelve (artista_crudo, fuente) según la regla aprobada."""
    artist = row["artist"]
    if isinstance(artist, str) and artist.strip():
        return artist, "artist"
    workers = row["workers"]
    if isinstance(workers, str):
        m = re.search(r"\(([^)]+)\)", workers)       # texto entre paréntesis
        if m:
            return m.group(1), "workers"
    if row["category"] == "Best New Artist" and isinstance(row["nominee"], str):
        return row["nominee"], "nominee"
    return None, "unknown"


# Separadores de colaboración en `artist`. Se evita partir por coma sola
# ni por "and" para no romper nombres de bandas.
_SPLIT = re.compile(r"\s*;\s*|\s+(?:featuring|feat\.?|&)\s+", flags=re.IGNORECASE)
_ROLE = re.compile(
    r",\s*(?:conductor|soloist|artist|producer|composer|arranger|engineer|songwriter)\b.*$",
    flags=re.IGNORECASE)


def build_grammy_tables() -> dict:
    """Construye dim_year, dim_category y fact_nomination."""
    PREPARED_DIR.mkdir(parents=True, exist_ok=True)
    raw = pd.read_csv(WORK_DIR / "grammy_raw.csv")
    log.info("Grammy crudo: %d filas", len(raw))

    recovered = raw.apply(_recover_artist, axis=1, result_type="expand")
    raw["artist_raw"], raw["artist_source"] = recovered[0], recovered[1]
    log.info("Fuente del artista: %s",
             raw["artist_source"].value_counts().to_dict())

    # nomination_id: hash determinista de la nominación original
    key = (raw["year"].astype(str) + "|" + raw["category"].astype(str) + "|"
           + raw["nominee"].astype(str) + "|" + raw["artist_raw"].astype(str))
    raw["nomination_id"] = pd.util.hash_pandas_object(key, index=False).astype(str)

    # Separar colaboraciones: una fila por artista
    raw["artist_name"] = raw["artist_raw"].apply(
        lambda s: _SPLIT.split(s) if isinstance(s, str) else [None])
    fact = raw.explode("artist_name")
    fact["artist_name"] = (fact["artist_name"].str.strip()
                           .str.replace(_ROLE, "", regex=True))
    fact["artist_norm"] = fact["artist_name"].map(normalize_name)
    fact.loc[fact["artist_norm"].isin(["", "various artists"]),
             "artist_norm"] = "__unknown__"
    fact.loc[fact["artist_norm"] == "__unknown__", "artist_source"] = "unknown"

    grammy_artists = (fact[fact["artist_norm"] != "__unknown__"]
                      .drop_duplicates("artist_norm")[["artist_norm", "artist_name"]])
    fact = fact[["nomination_id", "artist_norm", "category", "year",
                 "nominee", "winner", "artist_source"]].drop_duplicates(
                     ["nomination_id", "artist_norm"])

    dim_year = (fact[["year"]].drop_duplicates().rename(columns={"year": "year_key"}))
    dim_year["decade"] = (dim_year["year_key"] // 10) * 10
    dim_category = fact[["category"]].drop_duplicates().rename(
        columns={"category": "category_name"})

    outputs = {"dim_year": dim_year, "dim_category": dim_category,
               "fact_nomination": fact,
               "grammy_artists": grammy_artists}
    counts = {}
    for name, df in outputs.items():
        df.to_csv(PREPARED_DIR / f"{name}.csv", index=False)
        counts[name] = len(df)
        log.info("%s: %d filas", name, len(df))
    counts["nominaciones_distintas"] = fact["nomination_id"].nunique()
    log.info("nominaciones distintas: %d", counts["nominaciones_distintas"])
    return counts
def build_dim_artist() -> dict:
    """Une los artistas de ambas fuentes y mide el cruce Grammy -> Spotify."""
    sp = pd.read_csv(PREPARED_DIR / "spotify_artists.csv", keep_default_na=False, na_values=[""])
    gr = pd.read_csv(PREPARED_DIR / "grammy_artists.csv", keep_default_na=False, na_values=[""])

    dim = sp.merge(gr, on="artist_norm", how="outer",
                   suffixes=("_sp", "_gr"), indicator=True)
    dim["artist_name"] = dim["artist_name_sp"].fillna(dim["artist_name_gr"])
    dim["in_spotify"] = dim["_merge"].isin(["both", "left_only"])
    dim["in_grammy"] = dim["_merge"].isin(["both", "right_only"])
    dim["is_placeholder"] = False
    dim = dim[["artist_norm", "artist_name", "in_spotify",
               "in_grammy", "is_placeholder"]]

    grammy_total = int(dim["in_grammy"].sum())
    matched = int((dim["in_spotify"] & dim["in_grammy"]).sum())
    match_rate = matched / grammy_total if grammy_total else 0.0
    log.info("Artistas Grammy: %d | con cruce en Spotify: %d (%.2f%%) | sin cruce: %d",
             grammy_total, matched, match_rate * 100, grammy_total - matched)
    
    dim["artist_name"] = dim["artist_name"].fillna(dim["artist_norm"])
    dim.to_csv(PREPARED_DIR / "dim_artist.csv", index=False)
    log.info("dim_artist: %d filas", len(dim))
    return {"dim_artist": len(dim), "grammy_artists": grammy_total,
            "matched_artists": matched, "match_rate": round(match_rate, 4)}


def transform_and_integrate() -> dict:
    """Punto de entrada de la tarea de Airflow: devuelve solo metadatos."""
    counts = {}
    counts.update(build_spotify_tables())
    counts.update(build_grammy_tables())
    counts.update(build_dim_artist())
    return counts