"""DAG del Workshop-2: extraccion, validacion cruda, transformacion, validacion de
datos preparados y carga al DW.
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

    # Transformacion: logica determinista sobre archivos, reintentar no cambia el resultado.
    @task(retries=0)
    def transform_and_integrate():
        from transform import transform_and_integrate as run
        return run()

    # Compuerta de los datos preparados: mismo criterio que las compuertas crudas.
    @task(retries=0)
    def validate_prepared():
        from validation import validate_prepared as run
        return run()

    # Carga: una sola transaccion con upsert, asi que repetirla es seguro
    # y una caida temporal de la base se puede reintentar.
    @task(retries=2, retry_delay=timedelta(seconds=30))
    def load_dw():
        from airflow.sdk import get_current_context
        from load import load_dw as run
        return run(dag_run_id=get_current_context()["run_id"])

    spotify_raw = extract_spotify()
    grammy_raw = extract_grammys()
    spotify_checked = validate_spotify_raw(spotify_raw)
    grammy_checked = validate_grammys_raw(grammy_raw)

    transformed = transform_and_integrate()
    prepared_checked = validate_prepared()
    loaded = load_dw()

    # Las dos ramas deben pasar su compuerta antes de integrar.
    [spotify_checked, grammy_checked] >> transformed >> prepared_checked >> loaded


reliable_music_pipeline()