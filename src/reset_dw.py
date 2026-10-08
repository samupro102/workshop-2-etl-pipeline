"""Deja el DW vacio (salvo el artista placeholder) para probar una primera carga limpia."""
from sqlalchemy import text

from load import _dw_engine


def reset_dw():
    with _dw_engine().begin() as conn:
        conn.execute(text(
            "TRUNCATE fact_nomination, fact_track_audio, bridge_track_artist, "
            "bridge_track_genre, dim_track, dim_genre, dim_category, dim_year, "
            "etl_load_audit RESTART IDENTITY"))
        conn.execute(text("DELETE FROM dim_artist WHERE NOT is_placeholder"))
        conn.execute(text(
            "SELECT setval(pg_get_serial_sequence('dim_artist', 'artist_key'), 1)"))
        n = conn.execute(text("SELECT COUNT(*) FROM dim_artist")).scalar()
    print(f"[reset] DW vacio; dim_artist conserva {n} fila (placeholder)")


if __name__ == "__main__":
    reset_dw()