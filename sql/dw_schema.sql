-- Esquema estrella del Data Warehouse (base music_dw)

-- ===== DIMENSIONES =====

CREATE TABLE IF NOT EXISTS dim_year (
    year_key  INTEGER PRIMARY KEY CHECK (year_key BETWEEN 1900 AND 2100),
    decade    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_key  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_key  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    genre_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_artist (
    artist_key     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    artist_norm    TEXT    NOT NULL UNIQUE,   -- llave de negocio (nombre normalizado)
    artist_name    TEXT    NOT NULL,
    in_spotify     BOOLEAN NOT NULL DEFAULT FALSE,
    in_grammy      BOOLEAN NOT NULL DEFAULT FALSE,
    is_placeholder BOOLEAN NOT NULL DEFAULT FALSE
);

-- Artista marcador para nominaciones sin artista recuperable
INSERT INTO dim_artist (artist_norm, artist_name, is_placeholder)
VALUES ('__unknown__', 'Desconocido', TRUE)
ON CONFLICT (artist_norm) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_track (
    track_key  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    track_id   TEXT NOT NULL UNIQUE,          -- llave de negocio (Spotify)
    explicit   BOOLEAN
);

-- ===== TABLAS PUENTE =====

CREATE TABLE IF NOT EXISTS bridge_track_artist (
    track_key  INTEGER NOT NULL REFERENCES dim_track (track_key),
    artist_key INTEGER NOT NULL REFERENCES dim_artist (artist_key),
    PRIMARY KEY (track_key, artist_key)
);

CREATE TABLE IF NOT EXISTS bridge_track_genre (
    track_key INTEGER NOT NULL REFERENCES dim_track (track_key),
    genre_key INTEGER NOT NULL REFERENCES dim_genre (genre_key),
    PRIMARY KEY (track_key, genre_key)
);

-- ===== HECHOS =====

-- Grano: una fila por cancion unica (track_id)
CREATE TABLE IF NOT EXISTS fact_track_audio (
    track_key    INTEGER PRIMARY KEY REFERENCES dim_track (track_key),
    popularity   SMALLINT NOT NULL CHECK (popularity BETWEEN 0 AND 100),
    danceability DOUBLE PRECISION NOT NULL CHECK (danceability BETWEEN 0 AND 1),
    energy       DOUBLE PRECISION NOT NULL CHECK (energy BETWEEN 0 AND 1),
    valence      DOUBLE PRECISION NOT NULL CHECK (valence BETWEEN 0 AND 1),
    acousticness DOUBLE PRECISION NOT NULL CHECK (acousticness BETWEEN 0 AND 1)
);

-- Grano: una fila por artista en cada nominacion Grammy
CREATE TABLE IF NOT EXISTS fact_nomination (
    nomination_id    TEXT     NOT NULL,       -- hash determinista de (year, category, nominee, artist crudo)
    artist_key       INTEGER  NOT NULL REFERENCES dim_artist (artist_key),
    category_key     INTEGER  NOT NULL REFERENCES dim_category (category_key),
    year_key         INTEGER  NOT NULL REFERENCES dim_year (year_key),
    nominee          TEXT,
    winner           BOOLEAN,
    artist_source    TEXT     NOT NULL CHECK (artist_source IN ('artist', 'workers', 'nominee', 'unknown')),
    nomination_count SMALLINT NOT NULL DEFAULT 1 CHECK (nomination_count = 1),
    PRIMARY KEY (nomination_id, artist_key)
);

CREATE INDEX IF NOT EXISTS ix_fact_nomination_artist   ON fact_nomination (artist_key);
CREATE INDEX IF NOT EXISTS ix_fact_nomination_category ON fact_nomination (category_key);
CREATE INDEX IF NOT EXISTS ix_fact_nomination_year     ON fact_nomination (year_key);
CREATE INDEX IF NOT EXISTS ix_bridge_track_artist_artist ON bridge_track_artist (artist_key);
CREATE INDEX IF NOT EXISTS ix_bridge_track_genre_genre   ON bridge_track_genre (genre_key);

-- ===== AUDITORIA DE CARGA =====

CREATE TABLE IF NOT EXISTS etl_load_audit (
    audit_id    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_run_id  TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    rows_loaded INTEGER NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

