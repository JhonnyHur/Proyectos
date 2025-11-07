# etl/load.py
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine, text

# ===== Rutas en el contenedor Airflow =====
CSV_FINAL  = Path("/opt/airflow/data/output/df_icfes_2024_final.csv")   # notebook: ../data/curated/...
SCHEMA_SQL = Path("/opt/airflow/etl/sql/schema.sql")                    # notebook: ../dw/schema.sql

# ===== Conexión al DW vía .env =====
# DW_CONN_URI=postgresql+psycopg2://airflow:airflow@postgres:5432/dw
DW_CONN_URI = os.getenv("DW_CONN_URI")

# ===== Asegurar que la BD 'dw' exista (AUTOCOMMIT) =====
def ensure_dw_exists():
    base_uri = "postgresql+psycopg2://airflow:airflow@postgres:5432/postgres"
    base_engine = create_engine(base_uri, pool_pre_ping=True)
    with base_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        exists = conn.execute(text("SELECT 1 FROM pg_database WHERE datname='dw';")).fetchone()
        if not exists:
            print("[INIT] Creando base de datos 'dw'…")
            conn.execute(text("CREATE DATABASE dw"))
            print("[INIT] BD 'dw' creada.")
        else:
            print("[INIT] BD 'dw' ya existe.")

def load():
    print("== LOAD (modo notebook) : inicio ==")

    # 0) Sanidad
    if not DW_CONN_URI:
        raise RuntimeError("DW_CONN_URI no está definido en el entorno (.env).")
    if not CSV_FINAL.exists():
        raise FileNotFoundError(f"No se encuentra el dataset final: {CSV_FINAL}")
    if not SCHEMA_SQL.exists():
        raise FileNotFoundError(f"No se encuentra el schema SQL: {SCHEMA_SQL}")

    # 1) Asegurar BD y crear engine
    ensure_dw_exists()
    engine = create_engine(DW_CONN_URI, pool_pre_ping=True)

    # 2) Ejecutar esquema (DROP/CREATE)
    print(f"[LOAD] Ejecutando schema: {SCHEMA_SQL}")
    schema_sql = SCHEMA_SQL.read_text(encoding="utf-8")
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(schema_sql))
    print("[LOAD] Esquema creado.")

    # 3) Cargar dataset final (curated)
    print(f"[LOAD] Leyendo CSV final: {CSV_FINAL}")
    df = pd.read_csv(CSV_FINAL, encoding="utf-8-sig", low_memory=False)
    print(f"[LOAD] Shape leído: {df.shape}")

    # ——— Limpiezas suaves para evitar tipos raros (no cambian la lógica del notebook)
    # Si hay cadenas vacías donde deberían ser numéricas, pandas las maneja como NaN (OK para merges).
    for col in ["fami_estratovivienda"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["estu_inse_individual", "estu_nse_individual", "estu_nse_establecimiento"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "cole_bilingue" in df.columns:
        df["cole_bilingue"] = pd.to_numeric(df["cole_bilingue"], errors="coerce")

    # 4) === Dimensiones en pandas (igual al notebook) ===
    dim_departamento = (
        df[["estu_depto_reside", "poblacion_depto", "idh_depto", "pobreza_monetaria_depto"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_municipio = (
        df[["estu_mcpio_reside", "poblacion_mcpio"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_contexto = (
        df[["estu_inse_individual", "estu_nse_individual", "fami_estratovivienda"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_colegio = (
        df[[
            "cole_area_ubicacion", "cole_bilingue", "cole_calendario", "cole_naturaleza",
            "cole_depto_ubicacion", "estu_nse_establecimiento"
        ]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_fecha = (
        df[["periodo"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    print("[DIM] Shapes:",
          "dep", dim_departamento.shape,
          "mun", dim_municipio.shape,
          "ctx", dim_contexto.shape,
          "col", dim_colegio.shape,
          "fec", dim_fecha.shape)

    # 5) Insertar dimensiones (append) — igual que el notebook
    with engine.begin() as conn:
        dim_departamento.to_sql("dim_departamento", con=conn, if_exists="append", index=False,
                                method="multi", chunksize=50_000)
        dim_municipio.to_sql("dim_municipio", con=conn, if_exists="append", index=False,
                             method="multi", chunksize=50_000)
        dim_contexto.to_sql("dim_contexto_socioeconomico", con=conn, if_exists="append", index=False,
                            method="multi", chunksize=50_000)
        dim_colegio.to_sql("dim_colegio", con=conn, if_exists="append", index=False,
                           method="multi", chunksize=50_000)
        dim_fecha.to_sql("dim_fecha", con=conn, if_exists="append", index=False,
                         method="multi", chunksize=50_000)
    print("[DIM] Dimensiones insertadas.")

    # 6) === Construir FACT en pandas (merges + índices +1) — igual al notebook ===
    fact_icfes = df.merge(
        dim_departamento.reset_index().rename(columns={"index": "id_departamento"}),
        on=["estu_depto_reside", "poblacion_depto", "idh_depto", "pobreza_monetaria_depto"],
        how="left"
    ).merge(
        dim_municipio.reset_index().rename(columns={"index": "id_municipio"}),
        on=["estu_mcpio_reside", "poblacion_mcpio"],
        how="left"
    ).merge(
        dim_contexto.reset_index().rename(columns={"index": "id_contexto"}),
        on=["estu_inse_individual", "estu_nse_individual", "fami_estratovivienda"],
        how="left"
    ).merge(
        dim_colegio.reset_index().rename(columns={"index": "id_colegio"}),
        on=["cole_area_ubicacion", "cole_bilingue", "cole_calendario", "cole_naturaleza",
            "cole_depto_ubicacion", "estu_nse_establecimiento"],
        how="left"
    ).merge(
        dim_fecha.reset_index().rename(columns={"index": "id_fecha"}),
        on="periodo",
        how="left"
    )

    fact_icfes = fact_icfes[[
        "id_departamento", "id_municipio", "id_contexto", "id_colegio", "id_fecha",
        "percentil_c_naturales", "percentil_global", "percentil_ingles",
        "percentil_lectura_critica", "percentil_matematicas", "percentil_sociales_ciudadanas",
        "punt_c_naturales", "punt_ingles", "punt_lectura_critica",
        "punt_matematicas", "punt_sociales_ciudadanas", "punt_global"
    ]]

    # Ajuste de surrogate keys: +1 (SERIAL empieza en 1)
    for k in ["id_departamento", "id_municipio", "id_contexto", "id_colegio", "id_fecha"]:
        if k in fact_icfes.columns:
            fact_icfes[k] = pd.to_numeric(fact_icfes[k], errors="coerce") + 1

    print("[FACT] Fact shape:", fact_icfes.shape)

    # 7) Insertar FACT (append, en chunks)
    with engine.begin() as conn:
        fact_icfes.to_sql("fact_icfes", con=conn, if_exists="append", index=False,
                          method="multi", chunksize=100_000)
    print("[FACT] Tabla de hechos insertada.")

    print("== LOAD (modo notebook) : fin ==")
