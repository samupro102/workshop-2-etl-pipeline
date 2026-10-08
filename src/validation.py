"""Validacion con Great Expectations (compuertas crudas).
Las reglas DQxx se definen aqui (versionadas en Git). Cada expectation lleva su
rule_id en meta para poder mapear regla -> expectation."""
import json
from pathlib import Path
from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd
from great_expectations.expectations.metadata_types import FailureSeverity

from settings import DATA_DIR, GRAMMY_RAW, SPOTIFY_RAW

RESULTS_DIR = DATA_DIR / "validation_results"
E = gx.expectations

SPOTIFY_REQUIRED = ["track_id", "artists", "track_genre", "popularity",
                    "danceability", "energy", "valence", "acousticness"]
GRAMMY_REQUIRED = ["year", "category", "nominee", "artist", "workers"]
UNIT_RANGE = ["danceability", "energy", "valence", "acousticness"]


def spotify_raw_expectations():
    exps = [
        # DQ01 - columnas requeridas
        E.ExpectTableColumnsToMatchSet(
            column_set=SPOTIFY_REQUIRED, exact_match=False,
            severity="critical", meta={"rule_id": "DQ01"}),
        # DQ02 - popularity entre 0 y 100
        E.ExpectColumnValuesToBeBetween(
            column="popularity", min_value=0, max_value=100,
            severity="critical", meta={"rule_id": "DQ02"}),
        # DQ04 - unicidad de (track_id, track_genre), tolerancia 1 %
        E.ExpectCompoundColumnsToBeUnique(
            column_list=["track_id", "track_genre"], mostly=0.99,
            severity="warning", meta={"rule_id": "DQ04"}),
        # DQ05 - proporcion de ceros en popularity (solo monitoreo)
        E.ExpectColumnValuesToNotBeInSet(
            column="popularity", value_set=[0], mostly=0.9,
            severity="info", meta={"rule_id": "DQ05"}),
    ]
    # DQ03 - campos de audio entre 0 y 1
    for col in UNIT_RANGE:
        exps.append(E.ExpectColumnValuesToBeBetween(
            column=col, min_value=0, max_value=1,
            severity="critical", meta={"rule_id": "DQ03"}))
    return exps


def grammy_raw_expectations():
    return [
        # DQ06 - columnas requeridas
        E.ExpectTableColumnsToMatchSet(
            column_set=GRAMMY_REQUIRED, exact_match=False,
            severity="critical", meta={"rule_id": "DQ06"}),
        # DQ07 - category no nula
        E.ExpectColumnValuesToNotBeNull(
            column="category", severity="critical", meta={"rule_id": "DQ07"}),
        # DQ08 - al menos uno entre artist y workers, en >= 95 % de las filas
        E.ExpectColumnValuesToBeInSet(
            column="has_artist_info", value_set=[True], mostly=0.95,
            severity="warning", meta={"rule_id": "DQ08"}),
        # DQ09 - winner debe tener True y False (solo monitoreo)
        E.ExpectColumnDistinctValuesToContainSet(
            column="winner", value_set=[True, False],
            severity="info", meta={"rule_id": "DQ09"}),
    ]


def _with_artist_flag(df):
    """Columna auxiliar solo para validar DQ08. No se escribe en el archivo crudo."""
    out = df.copy()
    artist = out["artist"] if "artist" in out else pd.Series(pd.NA, index=out.index)
    workers = out["workers"] if "workers" in out else pd.Series(pd.NA, index=out.index)
    out["has_artist_info"] = artist.notna() | workers.notna()
    return out


def _run(df, stage, expectations):
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(name=f"{stage}_source")
    asset = source.add_dataframe_asset(name=f"{stage}_asset")
    batch_def = asset.add_batch_definition_whole_dataframe(f"{stage}_batch")
    suite = context.suites.add(gx.ExpectationSuite(name=f"{stage}_suite"))
    for exp in expectations:
        suite.add_expectation(exp)
    validation = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{stage}_validation", data=batch_def, suite=suite))
    return validation.run(
        batch_parameters={"dataframe": df},
        result_format={"result_format": "SUMMARY"})


def _summarize(result):
    rows = []
    for r in result.results:
        cfg = r.expectation_config
        res = r.result or {}
        rows.append({
            "rule_id": (cfg.meta or {}).get("rule_id"),
            "expectation": cfg.type,
            "column": cfg.kwargs.get("column", ""),
            "success": bool(r.success),
            "severity": getattr(getattr(cfg, "severity", None), "name", str(getattr(cfg, "severity", None))),
            "unexpected_percent": res.get("unexpected_percent"),
        })
    return rows


def _save(stage, result, rows, max_failure):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = RESULTS_DIR / f"{stage}_{stamp}.json"
    payload = {
        "stage": stage,
        "run_at_utc": stamp,
        "success": bool(result.success),
        "statistics": result.statistics,
        "max_failure_severity": max_failure.name if max_failure else None,
        "rules": rows,
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def _gate(df, stage, expectations):
    """Compuerta: Critical bloquea (lanza error); Warning e Info se registran."""
    result = _run(df, stage, expectations)
    max_failure = result.get_max_severity_failure()
    rows = _summarize(result)
    path = _save(stage, result, rows, max_failure)

    print(f"[{stage}] filas validadas={len(df)}")
    print(pd.DataFrame(rows).to_string(index=False))
    print(f"[{stage}] estadisticas={result.statistics}")
    print(f"[{stage}] severidad maxima de fallo={max_failure.name if max_failure else 'ninguna'}")
    print(f"[{stage}] resultados guardados en {path}")

    if max_failure == FailureSeverity.CRITICAL:
        raise ValueError(f"[{stage}] Validacion con fallo CRITICAL. Se bloquea el avance del pipeline.")
    if max_failure == FailureSeverity.WARNING:
        print(f"[{stage}] Hay fallos WARNING: la politica documentada permite continuar.")
    print(f"[{stage}] Compuerta superada segun la politica.")
    return str(path)


def validate_spotify_raw(path=None):
    df = pd.read_csv(path or str(SPOTIFY_RAW))
    return _gate(df, "raw_spotify", spotify_raw_expectations())


def validate_grammys_raw(path=None):
    df = pd.read_csv(path or str(GRAMMY_RAW))
    return _gate(_with_artist_flag(df), "raw_grammy", grammy_raw_expectations())


PREPARED_DIR = DATA_DIR / "work" / "prepared"
DQ13_MIN_MATCH = 0.40  # umbral de quality_rules.md (provisional)


def prepared_artist_expectations():
    return [
        # DQ11 - clave normalizada unica en dim_artist
        E.ExpectColumnValuesToBeUnique(
            column="artist_norm", severity="critical", meta={"rule_id": "DQ11"}),
        # DQ13 - proporcion de artistas de Grammy con coincidencia en Spotify
        E.ExpectColumnValuesToBeInSet(
            column="matches_spotify", value_set=[True], mostly=DQ13_MIN_MATCH,
            severity="warning", meta={"rule_id": "DQ13"}),
    ]


def prepared_nomination_expectations(expected_nominations):
    return [
        # DQ10 - toda nominacion tiene clave de artista (real o 'Desconocido')
        E.ExpectColumnValuesToNotBeNull(
            column="artist_norm", severity="critical", meta={"rule_id": "DQ10"}),
        E.ExpectColumnValuesToBeInSet(
            column="artist_in_dim", value_set=[True],
            severity="critical", meta={"rule_id": "DQ10"}),
        # DQ12 - nominaciones distintas == filas del Grammy crudo
        E.ExpectColumnUniqueValueCountToBeBetween(
            column="nomination_id", min_value=expected_nominations,
            max_value=expected_nominations,
            severity="critical", meta={"rule_id": "DQ12"}),
        # DQ14 - cada artista se asocia a exactamente una clave de dim_artist
        E.ExpectColumnValuesToBeBetween(
            column="dim_matches", min_value=1, max_value=1,
            severity="critical", meta={"rule_id": "DQ14"}),
    ]


def validate_prepared(prepared_dir=None):
    folder = Path(prepared_dir) if prepared_dir else PREPARED_DIR
    dim = pd.read_csv(folder / "dim_artist.csv", keep_default_na=False, na_values=[""])
    fact = pd.read_csv(folder / "fact_nomination.csv", keep_default_na=False, na_values=[""])
    expected = len(pd.read_csv(str(GRAMMY_RAW)))

    # Dimension de artista: DQ11 (unicidad) y DQ13 (cruce, solo artistas Grammy)
    art = dim[["artist_norm", "in_spotify", "in_grammy"]].copy()
    art["matches_spotify"] = art["in_spotify"].astype(object).where(art["in_grammy"], None)

    # Nominaciones: DQ10, DQ12 y DQ14
    counts = dim["artist_norm"].value_counts()
    nom = fact[["nomination_id", "artist_norm"]].copy()
    is_unknown = nom["artist_norm"] == "__unknown__"
    nom["dim_matches"] = nom["artist_norm"].map(counts).fillna(0).astype(int)
    nom.loc[is_unknown, "dim_matches"] = 1  # marcador sembrado en el DW
    nom["artist_in_dim"] = nom["dim_matches"] >= 1

    return [
        _gate(art, "prepared_artist", prepared_artist_expectations()),
        _gate(nom, "prepared_nomination", prepared_nomination_expectations(expected)),
    ]