"""DAG del Workshop-2. Version parcial: ramas de extraccion y validacion cruda.
La logica vive en src/; el DAG solo define tareas, dependencias y politica de reintentos."""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="reliable_music_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "gx", "spotify", "grammy"],
)
def reliable_music_pipeline():

    # Archivo local: un fallo de lectura no es transitorio, no se reintenta.
    @task(retries=0)
    def extract_spotify():
        from extract import extract_spotify as run
        return run()

    # Base de datos: una caida temporal puede resolverse sola, se reintenta con limite.
    @task(retries=2, retry_delay=timedelta(seconds=30))
    def extract_grammys():
        from extract import extract_grammys as run
        return run()

    # Compuertas: un fallo Critical es determinista, reintentar repite el mismo fallo.
    @task(retries=0)
    def validate_spotify_raw(raw_path):
        from validation import validate_spotify_raw as run
        return run(raw_path)

    @task(retries=0)
    def validate_grammys_raw(raw_path):
        from validation import validate_grammys_raw as run
        return run(raw_path)

    spotify_raw = extract_spotify()
    grammy_raw = extract_grammys()
    validate_spotify_raw(spotify_raw)
    validate_grammys_raw(grammy_raw)


reliable_music_pipeline()
