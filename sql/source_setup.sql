DROP TABLE IF EXISTS grammy_awards;

CREATE TABLE grammy_awards (
    year          INTEGER,
    title         TEXT,
    published_at  TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    category      TEXT,
    nominee       TEXT,
    artist        TEXT,
    workers       TEXT,
    img           TEXT,
    winner        BOOLEAN
);

COPY grammy_awards
FROM '/tmp/the_grammy_awards.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

SELECT COUNT(*) AS filas_en_tabla FROM grammy_awards;
