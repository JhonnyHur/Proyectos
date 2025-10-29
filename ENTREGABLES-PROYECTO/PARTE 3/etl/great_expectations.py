# etl/greate_expectations.py

from pathlib import Path
import json
import pandas as pd
from great_expectations.dataset import PandasDataset

# === Rutas dentro del contenedor Airflow ===
BASE_OUTPUT = Path("/opt/airflow/data/output")
IN_FINAL = BASE_OUTPUT / "df_icfes_2024_final.csv"
GE_JSON = BASE_OUTPUT / "ge_report.json"


def _succ(x) -> bool:
    try:
        return bool(x.get("success", False))
    except Exception:
        return False


def _brief(res) -> dict:
    """Extrae información relevante de cada expectativa GE."""
    if res is None:
        return {"success": None, "note": "skipped"}
    out = {"success": res.get("success", False)}
    r = res.get("result", {})
    for k in ("unexpected_count", "unexpected_percent", "observed_value"):
        if k in r:
            out[k] = r[k]
    return out


def great_expectations():
    print("== GE: iniciando validaciones sobre df_icfes_2024_final.csv ==")

    # Cargar dataset final
    df = pd.read_csv(IN_FINAL, encoding="utf-8-sig", low_memory=False)
    print(f"[GE] Dataset leído: {IN_FINAL}  Shape={df.shape}")

    ge_df = PandasDataset(df)
    summary = {
        "dataset": str(IN_FINAL),
        "shape": {"rows": int(df.shape[0]), "cols": int(df.shape[1])},
        "expectations": {}
    }

    # =========================
    # Expectativas críticas
    # =========================
    res1 = ge_df.expect_column_values_to_not_be_null("punt_global")
    res2 = ge_df.expect_column_values_to_be_between("punt_global", min_value=0, max_value=500)
    res3 = ge_df.expect_column_values_to_not_be_null("estu_depto_reside")

    summary["expectations"]["not_null_punt_global"] = _brief(res1)
    summary["expectations"]["range_punt_global_0_500"] = _brief(res2)
    summary["expectations"]["not_null_depto_reside"] = _brief(res3)

    # =========================
    # Otras expectativas
    # =========================
    res4 = ge_df.expect_column_values_to_be_of_type("periodo", "int64") if "periodo" in ge_df.columns else None
    res5 = ge_df.expect_column_values_to_be_of_type("estu_pais_reside", "str") if "estu_pais_reside" in ge_df.columns else None
    res6 = ge_df.expect_column_values_to_be_between("pobreza_monetaria_depto", min_value=0, max_value=100) if "pobreza_monetaria_depto" in ge_df.columns else None
    res7 = ge_df.expect_column_values_to_be_between("idh_depto", min_value=0, max_value=1) if "idh_depto" in ge_df.columns else None

    summary["expectations"]["type_periodo_int64"] = _brief(res4)
    summary["expectations"]["type_pais_str"] = _brief(res5)
    summary["expectations"]["range_pobreza_0_100"] = _brief(res6)
    summary["expectations"]["range_idh_0_1"] = _brief(res7)

    #  Regla entre columnas
    if {"punt_global", "punt_matematicas"}.issubset(ge_df.columns):
        mask = ge_df["punt_global"] >= ge_df["punt_matematicas"]
        res8 = {
            "success": bool(mask.all()),
            "unexpected_count": int((~mask).sum()),
            "unexpected_percent": round((~mask).sum() / len(ge_df) * 100, 4),
        }
    else:
        res8 = None
    summary["expectations"]["rule_global_ge_math"] = _brief(res8)

    # Guardar reporte JSON estructurado
    GE_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[GE] Reporte guardado: {GE_JSON}")

    # Validar expectativas críticas
    critical_ok = all((_succ(res1), _succ(res2), _succ(res3)))
    print(f"[GE] Estado de expectativas críticas: {critical_ok}")

    if not critical_ok:
        raise AssertionError("GE: Falló alguna expectativa crítica. Revisa ge_report.json")

    print("== GE: validaciones completadas con éxito ==")
