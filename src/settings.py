"""Rutas y columnas del pipeline. Todo corre dentro del contenedor de Airflow."""
import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
WORK_DIR = DATA_DIR / "work"
OUTPUT_DIR = DATA_DIR / "output"

SPOTIFY_SOURCE = DATA_DIR / "raw" / "spotify_dataset.csv"
SPOTIFY_RAW = WORK_DIR / "spotify_raw.csv"
GRAMMY_RAW = WORK_DIR / "grammy_raw.csv"

# Columnas que usan los requerimientos R1-R3
SPOTIFY_COLUMNS = [
    "track_id", "artists", "track_genre", "popularity", "explicit",
    "danceability", "energy", "valence", "acousticness",
]
GRAMMY_COLUMNS = ["year", "category", "nominee", "artist", "workers", "winner"]
