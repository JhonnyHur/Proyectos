# etl/transform.py  

from pathlib import Path
import pandas as pd
import numpy as np
import unicodedata

# ========= RUTAS EN CONTENEDOR =========
BASE_INPUT   = Path("/opt/airflow/data/input")
BASE_STAGING = Path("/opt/airflow/data/staging")
BASE_OUTPUT  = Path("/opt/airflow/data/output")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Entradas (respetando nombres del notebook, adaptados a Airflow)
FILE_ICFES              = BASE_INPUT   / "data_icfes_2024.csv"
FILE_DANE_POBREZA       = BASE_STAGING / "dane_pobreza_monetaria.csv"
FILE_IDH_DEPTO          = BASE_STAGING / "idh_departamentos.csv"
FILE_POBLACION_MUNICIPIO= BASE_STAGING / "poblacion_municipios.csv"

# Salida final
OUT_FINAL = BASE_OUTPUT / "df_icfes_2024_final.csv"

# ========= HELPERS =========
def _norm_txt(x: object) -> str:
    if pd.isna(x):
        return ""
    s = str(x).upper().strip()
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s

def transform():
    print("== TRANSFORM (inicio) ==")

    # ===== 1) Leer ICFES =====
    df_icfes_2024 = pd.read_csv(FILE_ICFES, encoding="utf-8", low_memory=False)
    print(f"[ICFES] shape={df_icfes_2024.shape}")

    # ===== 2) Drop de columnas EXACTO (del notebook) =====
    cols_drop = [
        "cole_genero","cole_jornada","cole_caracter","estu_genero","estu_grado",
        "estu_dedicacioninternet","estu_dedicacionlecturadiaria","estu_horassemanatrabaja",
        "fami_comecarnepescadohuevo","fami_comecerealfrutoslegumbre","fami_comelechederivados",
        "fami_cuartoshogar","fami_educacionmadre","fami_educacionpadre","fami_numlibros",
        "fami_personashogar","fami_tienecomputador","fami_tieneinternet"
    ]
    df_icfes_2024.drop(columns=[c for c in cols_drop if c in df_icfes_2024.columns], inplace=True, errors="ignore")
    print(f"[ICFES] columnas tras drop: {len(df_icfes_2024.columns)}")

    # ===== 3) Mapeos EXACTOS del notebook =====
    map_deptos = {
        "BOGOTÁ": "BOGOTÁ, D.C.","BOGOTA": "BOGOTÁ, D.C.","BOGOTÁ D.C.": "BOGOTÁ, D.C.","BOGOTA D.C.":"BOGOTÁ, D.C.",
        "VALLE": "VALLE DEL CAUCA","QUINDIO": "QUINDÍO","CUNDINAMARCA": "CUNDINAMARCA","ANTIOQUIA": "ANTIOQUIA",
        "BOLIVAR": "BOLÍVAR","CAUCA": "CAUCA","ATLANTICO": "ATLÁNTICO","NARIÑO": "NARIÑO","NARINO": "NARIÑO",
        "SANTANDER": "SANTANDER","CORDOBA": "CÓRDOBA","RISARALDA": "RISARALDA","CESAR": "CESAR","MAGDALENA": "MAGDALENA",
        "HUILA": "HUILA","CALDAS": "CALDAS","NORTE SANTANDER": "NORTE DE SANTANDER","NORTE DE SANTANDER": "NORTE DE SANTANDER",
        "TOLIMA": "TOLIMA","LA GUAJIRA": "LA GUAJIRA","META": "META","CASANARE": "CASANARE","ARAUCA": "ARAUCA","SUCRE": "SUCRE",
        "BOYACA": "BOYACÁ","CAQUETA": "CAQUETÁ","PUTUMAYO": "PUTUMAYO","GUAVIARE": "GUAVIARE","CHOCO": "CHOCÓ",
        "GUAINIA": "GUAINÍA","VICHADA": "VICHADA","AMAZONAS": "AMAZONAS",
        "SAN ANDRES": "SAN ANDRÉS, PROVIDENCIA Y SANTA CATALINA",
        "SAN ANDRES Y PROVIDENCIA": "SAN ANDRÉS, PROVIDENCIA Y SANTA CATALINA",
        "VAUPES": "VAUPÉS","VALLE DEL CAUCA": "VALLE DEL CAUCA","DESCONOCIDO": "DESCONOCIDO","EXTRANJERO": "EXTRANJERO"
    }

    map_mpios = {
        "ARMERO GUAYABAL":"ARMERO","BOGOTA":"BOGOTA D.C.","CALIMA EL DARIEN":"CALIMA","CARTAGENA":"CARTAGENA DE INDIAS",
        "CHIBOLO":"CHIVOLO","DON MATIAS":"DONMATIAS","FUENTE DE ORO":"FUENTEDEORO","GUICAN":"GUICAN DE LA SIERRA",
        "MAGUI PAYAN":"MAGUI","MARIQUITA":"SAN SEBASTIAN DE MARIQUITA","PIENDAMO":"PIENDAMO - TUNIA",
        "PURISIMA":"PURISIMA DE LA CONCEPCION","SALAZAR DE LAS PALMAS":"SALAZAR","SAN JOSE DEL PALMAR":"SAN JOSE DEL PALMAR",
        "SAN JUAN DE RIO SECO":"SAN JUAN DE RIOSECO","SAN VICENTE":"SAN VICENTE FERRER","SANTA CRUZ DE LORICA":"LORICA",
        "SANTAFE DE ANTIOQUIA":"SANTA FE DE ANTIOQUIA","SINCE":"SAN LUIS DE SINCE","TUMACO":"SAN ANDRES DE TUMACO",
        "UBATE":"VILLA DE SAN DIEGO DE UBATE",
        "RIO IRO": None,"SIPI": None
    }

    # ===== 4) Normalizar departamentos en ICFES y aplicar map_deptos =====
    cols_deptos = ["cole_depto_ubicacion","estu_depto_presentacion","estu_depto_reside"]
    for col in cols_deptos:
        if col in df_icfes_2024.columns:
            df_icfes_2024[col] = df_icfes_2024[col].map(_norm_txt).replace(map_deptos)

    # ===== 5) Reorden de columnas (como tu notebook) =====
    bloque_ubicacion = [
        "cole_depto_ubicacion","cole_mcpio_ubicacion",
        "estu_depto_presentacion","estu_mcpio_presentacion",
        "estu_depto_reside","estu_mcpio_reside",
        "estu_nacionalidad","estu_pais_reside"
    ]
    bloque_socioeco = [
        "estu_inse_individual","estu_nse_individual",
        "estu_nse_establecimiento","fami_estratovivienda"
    ]
    inicio = ["periodo","estu_estudiante","cole_area_ubicacion","cole_bilingue","cole_calendario","cole_naturaleza"]

    otras = [c for c in df_icfes_2024.columns if c not in (inicio + bloque_ubicacion + bloque_socioeco)]
    orden_columnas = inicio + [c for c in bloque_ubicacion if c in df_icfes_2024.columns] + \
                     [c for c in bloque_socioeco if c in df_icfes_2024.columns] + otras

    if "punt_global" in orden_columnas:
        orden_columnas = [c for c in orden_columnas if c != "punt_global"] + ["punt_global"]

    df_icfes_2024 = df_icfes_2024[orden_columnas]
    print("[ICFES] reorden aplicado")







# etl/transform.py  (Parte 2/3) — continuar debajo

    # ===== 6) DANE: pobreza 2024 =====
    df_pobreza_monetaria = pd.read_csv(FILE_DANE_POBREZA, encoding="utf-8", low_memory=False)
    if "Pobreza_2023" in df_pobreza_monetaria.columns:
        df_pobreza_monetaria.drop(columns=["Pobreza_2023"], inplace=True)

    # Normalización + mapeo deptos
    df_pobreza_monetaria["Departamento"] = (
        df_pobreza_monetaria["Departamento"]
        .map(_norm_txt)
        .replace(map_deptos)
    )

    # Merge por departamento de residencia
    df_icfes_2024 = df_icfes_2024.merge(
        df_pobreza_monetaria.rename(columns={"Pobreza_2024": "pobreza_monetaria_depto"}),
        how="left",
        left_on="estu_depto_reside",
        right_on="Departamento"
    ).drop(columns=["Departamento"], errors="ignore")

    # Reordenar: pobreza_monetaria_depto después de estu_pais_reside
    cols = df_icfes_2024.columns.tolist()
    if "pobreza_monetaria_depto" in cols and "estu_pais_reside" in cols:
        cols.remove("pobreza_monetaria_depto")
        idx = cols.index("estu_pais_reside") + 1
        cols.insert(idx, "pobreza_monetaria_depto")
        df_icfes_2024 = df_icfes_2024[cols]

    # ===== 7) IDH y Población a nivel departamento =====
    df_idh_departamentos = pd.read_csv(FILE_IDH_DEPTO, encoding="utf-8", low_memory=False)

    df_idh_departamentos["Entidad"] = (
        df_idh_departamentos["Entidad"]
        .map(_norm_txt)
        .replace(map_deptos)
    )

    df_icfes_2024 = df_icfes_2024.merge(
        df_idh_departamentos.rename(columns={"IDH":"idh_depto","Población":"poblacion_depto"}),
        how="left",
        left_on="estu_depto_reside",
        right_on="Entidad"
    ).drop(columns=["Entidad"], errors="ignore")

    # Reorden: idh_depto después de estu_pais_reside; poblacion_depto después de estu_mcpio_reside
    cols = df_icfes_2024.columns.tolist()
    if "idh_depto" in cols and "estu_pais_reside" in cols:
        cols.remove("idh_depto")
        idx = cols.index("estu_pais_reside") + 1
        cols.insert(idx, "idh_depto")
    if "poblacion_depto" in cols and "estu_mcpio_reside" in cols:
        cols.remove("poblacion_depto")
        idx = cols.index("estu_mcpio_reside") + 1
        cols.insert(idx, "poblacion_depto")
    df_icfes_2024 = df_icfes_2024[cols]

    # ===== 8) Población municipal (1100 filas aprox.) =====
    df_poblacion_municipios = pd.read_csv(FILE_POBLACION_MUNICIPIO, encoding="utf-8", low_memory=False)
    if "URL" in df_poblacion_municipios.columns:
        df_poblacion_municipios = df_poblacion_municipios.drop(columns=["URL"])

    # Limpieza numérica de población municipal (idéntico al notebook)
    df_poblacion_municipios["Poblacion"] = (
        df_poblacion_municipios["Poblacion"]
        .astype(str)
        .str.replace("Población","", regex=False)
        .str.replace(".","", regex=False)
        .str.strip()
        .astype(int)
    )

    # Normalización 1 (función local)
    def normalizar(s):
        if pd.isna(s):
            return ""
        s = str(s).upper().strip()
        s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
        return s

    df_icfes_2024 = df_icfes_2024.reset_index(drop=True)
    df_poblacion_municipios = df_poblacion_municipios.reset_index(drop=True)

    # Normalizar municipios en ICFES
    for c in ["estu_mcpio_reside","cole_mcpio_ubicacion","estu_mcpio_presentacion"]:
        if c in df_icfes_2024.columns:
            df_icfes_2024[c] = df_icfes_2024[c].apply(normalizar)

    # Normalizar y mapear municipios en población
    df_poblacion_municipios["Municipio_norm"] = df_poblacion_municipios["Municipio"].apply(normalizar)
    df_poblacion_municipios["Municipio_mapeado"] = (
        df_poblacion_municipios["Municipio_norm"].replace(map_mpios).fillna(df_poblacion_municipios["Municipio_norm"])
    )

    # Normalización 2: departamento en población
    df_poblacion_municipios["Departamento_norm"] = df_poblacion_municipios["Departamento"].apply(normalizar)
    df_poblacion_municipios["Departamento_mapeado"] = (
        df_poblacion_municipios["Departamento_norm"].replace(map_deptos).fillna(df_poblacion_municipios["Departamento_norm"])
    )
    # Ajuste Bogotá DC
    mask_bogota = df_poblacion_municipios["Municipio_mapeado"] == "BOGOTA D.C."
    df_poblacion_municipios.loc[mask_bogota, "Departamento_mapeado"] = "BOGOTÁ, D.C."

    # Merge con población municipal por (depto_reside, mpio_reside)
    df_icfes_2024 = df_icfes_2024.merge(
        df_poblacion_municipios.rename(columns={"Poblacion":"poblacion_mcpio"})[
            ["Departamento_mapeado","Municipio_mapeado","poblacion_mcpio"]
        ],
        how="left",
        left_on=["estu_depto_reside","estu_mcpio_reside"],
        right_on=["Departamento_mapeado","Municipio_mapeado"]
    ).drop(columns=["Departamento_mapeado","Municipio_mapeado"], errors="ignore")

    # Reorden: poblacion_mcpio después de poblacion_depto
    cols = df_icfes_2024.columns.tolist()
    if "poblacion_mcpio" in cols and "poblacion_depto" in cols:
        cols.remove("poblacion_mcpio")
        idx = cols.index("poblacion_depto") + 1
        cols.insert(idx, "poblacion_mcpio")
        df_icfes_2024 = df_icfes_2024[cols]


# etl/transform.py  (Parte 3/3) — finalizar

    # ===== 9) Tratamiento de nulos y caso EXTRANJERO =====
    m_extranjero = df_icfes_2024["estu_depto_reside"].astype(str).str.strip().str.upper().eq("EXTRANJERO")
    # 0 en poblaciones, 0.0 en idh/pobreza para EXTRANJERO
    for c in ["poblacion_depto","poblacion_mcpio"]:
        if c in df_icfes_2024.columns:
            df_icfes_2024.loc[m_extranjero, c] = 0
    for c in ["idh_depto","pobreza_monetaria_depto"]:
        if c in df_icfes_2024.columns:
            df_icfes_2024.loc[m_extranjero, c] = 0.0

    # Tipos finales (idéntico a tu notebook)
    if "poblacion_depto" in df_icfes_2024.columns:
        df_icfes_2024["poblacion_depto"] = df_icfes_2024["poblacion_depto"].astype("Int64")
    if "poblacion_mcpio" in df_icfes_2024.columns:
        df_icfes_2024["poblacion_mcpio"] = df_icfes_2024["poblacion_mcpio"].astype("Int64")
    for c in ["idh_depto","pobreza_monetaria_depto"]:
        if c in df_icfes_2024.columns:
            df_icfes_2024[c] = df_icfes_2024[c].astype(float)

    # ===== 10) Copia final y escritura =====
    df_icfes_2024_final = df_icfes_2024.copy()
    df_icfes_2024_final.to_csv(OUT_FINAL, index=False, encoding="utf-8-sig")
    print(f"[OK] OUTPUT: {OUT_FINAL}  shape={df_icfes_2024_final.shape}")
    print("== TRANSFORM (fin) ==")
