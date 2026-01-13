#!/usr/bin/env python
# coding: utf-8

# In[74]:


import pandas as pd
import unicodedata
from great_expectations.core.batch import BatchRequest
from great_expectations.dataset import PandasDataset
import great_expectations as ge
import os, json
import yaml
from great_expectations.validator.validator import Validator
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint


# ## **Limpieza y Normalización**

# **Dataset Ifces 2024**

# In[2]:


FILE_ICFES = "../data/raw/data_icfes_2024.csv"
df_icfes_2024 = pd.read_csv(FILE_ICFES, encoding="utf-8")

print("Shape:", df_icfes_2024.shape)
print(df_icfes_2024.info())


# In[3]:


cols_drop = [
    "cole_genero",
    "cole_jornada",
    "cole_caracter",
    "estu_genero",
    "estu_grado",
    "estu_dedicacioninternet",  
    "estu_dedicacionlecturadiaria",
    "estu_horassemanatrabaja",
    "fami_comecarnepescadohuevo",
    "fami_comecerealfrutoslegumbre",
    "fami_comelechederivados",
    "fami_cuartoshogar",
    "fami_educacionmadre",
    "fami_educacionpadre",
    "fami_numlibros",
    "fami_personashogar",
    "fami_tienecomputador",
    "fami_tieneinternet"
]


print("Columnas antes:", len(df_icfes_2024.columns))

df_icfes_2024.drop(columns=cols_drop, inplace=True)

print("Columnas eliminadas:", len(cols_drop))
print("Columnas después:", len(df_icfes_2024.columns))


# In[4]:


print(df_icfes_2024.head(15).to_string())


# In[5]:


cols_dep_mun = [
    "cole_depto_ubicacion",
    "cole_mcpio_ubicacion",
    "estu_depto_presentacion",
    "estu_depto_reside",
    "estu_mcpio_presentacion",
    "estu_mcpio_reside"
]

for col in cols_dep_mun:
    print(f"\nValores únicos en {col}:")
    print(df_icfes_2024[col].unique())


# In[6]:


map_deptos = {
    "BOGOTÁ": "BOGOTÁ, D.C.",
    "BOGOTA": "BOGOTÁ, D.C.",
    "BOGOTÁ D.C.": "BOGOTÁ, D.C.",
    "BOGOTA D.C.":"BOGOTÁ, D.C.",
    "VALLE": "VALLE DEL CAUCA",
    "QUINDIO": "QUINDÍO",
    "CUNDINAMARCA": "CUNDINAMARCA",
    "ANTIOQUIA": "ANTIOQUIA",
    "BOLIVAR": "BOLÍVAR",
    "CAUCA": "CAUCA",
    "ATLANTICO": "ATLÁNTICO",
    "NARIÑO": "NARIÑO",
    "NARINO": "NARIÑO",
    "SANTANDER": "SANTANDER",
    "CORDOBA": "CÓRDOBA",
    "RISARALDA": "RISARALDA",
    "CESAR": "CESAR",
    "MAGDALENA": "MAGDALENA",
    "HUILA": "HUILA",
    "CALDAS": "CALDAS",
    "NORTE SANTANDER": "NORTE DE SANTANDER",
    "NORTE DE SANTANDER": "NORTE DE SANTANDER",
    "TOLIMA": "TOLIMA",
    "LA GUAJIRA": "LA GUAJIRA",
    "META": "META",
    "CASANARE": "CASANARE",
    "ARAUCA": "ARAUCA",
    "SUCRE": "SUCRE",
    "BOYACA": "BOYACÁ",
    "CAQUETA": "CAQUETÁ",
    "PUTUMAYO": "PUTUMAYO",
    "GUAVIARE": "GUAVIARE",
    "CHOCO": "CHOCÓ",
    "GUAINIA": "GUAINÍA",
    "VICHADA": "VICHADA",
    "AMAZONAS": "AMAZONAS",
    "SAN ANDRES": "SAN ANDRÉS, PROVIDENCIA Y SANTA CATALINA",
    "SAN ANDRES Y PROVIDENCIA": "SAN ANDRÉS, PROVIDENCIA Y SANTA CATALINA",
    "VAUPES": "VAUPÉS",
    "VALLE": "VALLE DEL CAUCA",
    "VALLE DEL CAUCA": "VALLE DEL CAUCA",
    "DESCONOCIDO": "DESCONOCIDO",
    "EXTRANJERO": "EXTRANJERO"
}


# In[7]:


map_mpios = {
    # --- Diferencias reales de escritura / nombre ---
    "ARMERO GUAYABAL": "ARMERO",
    "BOGOTA": "BOGOTA D.C.",
    "CALIMA EL DARIEN": "CALIMA",
    "CARTAGENA": "CARTAGENA DE INDIAS",
    "CHIBOLO": "CHIVOLO",
    "DON MATIAS": "DONMATIAS",
    "FUENTE DE ORO": "FUENTEDEORO",
    "GUICAN": "GUICAN DE LA SIERRA",
    "MAGUI PAYAN": "MAGUI",
    "MARIQUITA": "SAN SEBASTIAN DE MARIQUITA",
    "PIENDAMO": "PIENDAMO - TUNIA",
    "PURISIMA": "PURISIMA DE LA CONCEPCION",
    "SALAZAR DE LAS PALMAS": "SALAZAR",
    "SAN JOSE DEL PALMAR": "SAN JOSE DEL PALMAR",  
    "SAN JUAN DE RIO SECO": "SAN JUAN DE RIOSECO",
    "SAN VICENTE": "SAN VICENTE FERRER",
    "SANTA CRUZ DE LORICA": "LORICA",
    "SANTAFE DE ANTIOQUIA": "SANTA FE DE ANTIOQUIA",
    "SINCE": "SAN LUIS DE SINCE",
    "TUMACO": "SAN ANDRES DE TUMACO",
    "UBATE": "VILLA DE SAN DIEGO DE UBATE",


    # --- Municipios sin correspondencia directa (presentes solo en Población) ---
    "RIO IRO": None,
    "SIPI": None,

    # --- Municipios presentes solo en ICFES (sin equivalente en Población) ---
    # (referencia para ampliar catálogo si lo deseas)
    # "BARRANCO MINAS": None,
    # "CACAHUAL": None,
    # "EL ENCANTO": None,
    # "EXTRANJERO": None,
    # "GUACHENE": None,
    # "LA CHORRERA": None,
    # "LA PEDRERA": None,
    # "MIRITI - PARANA": None,
    # "NOROSI": None,
    # "PACOA": None,
    # "PUERTO ARICA": None,
    # "SAN ANDRES DE CUERQUIA": None,
    # "SAN FELIPE": None,
    # "SAN JOSE DE URE": None,
    # "SAN PEDRO DE LOS MILAGROS": None,
    # "MANAURE BALCON DEL CESAR": None,
    # "YAVARATE": None,
    # "BELEN DE BAJIRA": None # se cruza entre dos datasets que lo tienen en dos departamentes diferentes
}


# In[8]:


# --- Normalizar departamentos ---
cols_deptos = ["cole_depto_ubicacion", "estu_depto_presentacion", "estu_depto_reside"]

for col in cols_deptos:
    df_icfes_2024[col] = df_icfes_2024[col].str.upper().replace(map_deptos)


print(df_icfes_2024[["cole_depto_ubicacion"]].head(50))


# In[9]:


bloque_ubicacion = [
    "cole_depto_ubicacion", "cole_mcpio_ubicacion",
    "estu_depto_presentacion", "estu_mcpio_presentacion",
    "estu_depto_reside", "estu_mcpio_reside",
    "estu_nacionalidad", "estu_pais_reside"
]

bloque_socioeco = [
    "estu_inse_individual", "estu_nse_individual",
    "estu_nse_establecimiento", "fami_estratovivienda"
]

# Columnas fijas iniciales (antes de ubicación)
inicio = ["periodo", "estu_estudiante", "cole_area_ubicacion", 
            "cole_bilingue", "cole_calendario", "cole_naturaleza"]

# Columnas que no entran en ubicación ni socioeconómico
otras = [col for col in df_icfes_2024.columns 
            if col not in inicio + bloque_ubicacion + bloque_socioeco]

# Orden final
orden_columnas = inicio + bloque_ubicacion + bloque_socioeco + otras


if "punt_global" in orden_columnas:
    orden_columnas.remove("punt_global")
    orden_columnas.append("punt_global")

# Reordenar el DataFrame
df_icfes_2024 = df_icfes_2024[orden_columnas]

print(df_icfes_2024.head(25).to_string())


# **Dataset Incidencia de Pobreza Monetaria 2024 DANE por Departamento**

# In[10]:


FILE_DANE = "../data/staging/dane_pobreza_monetaria.csv"
df_pobreza_monetaria = pd.read_csv(FILE_DANE, encoding="utf-8")

print("Shape:", df_pobreza_monetaria.shape)
print(df_pobreza_monetaria.info())
print(df_pobreza_monetaria.head(24))


# In[11]:


df_pobreza_monetaria.drop(columns=["Pobreza_2023"], inplace=True)

print("Shape después de eliminar:", df_pobreza_monetaria.shape)
print(df_pobreza_monetaria.head())


# In[12]:


#Normalización

df_pobreza_monetaria["Departamento"] = (
    df_pobreza_monetaria["Departamento"]
    .apply(lambda x: ''.join(
        c for c in unicodedata.normalize('NFD', str(x))
        if unicodedata.category(c) != 'Mn'
    ))
)

df_pobreza_monetaria["Departamento"] = df_pobreza_monetaria["Departamento"].str.upper()


df_pobreza_monetaria["Departamento"] = df_pobreza_monetaria["Departamento"].replace(map_deptos)


print(df_pobreza_monetaria.head(24))


# In[13]:


# Unir por departamento de residencia
df_icfes_2024 = df_icfes_2024.merge(
    df_pobreza_monetaria.rename(columns={"Pobreza_2024": "pobreza_monetaria_depto"}),
    how="left",
    left_on="estu_depto_reside",
    right_on="Departamento"
).drop(columns=["Departamento"])  # quitamos columna duplicada

# Reordenar columnas: meter pobreza_monetaria_depto después de estu_pais_reside
cols = df_icfes_2024.columns.tolist()
cols.remove("pobreza_monetaria_depto")   # quitarla temporalmente de la lista
idx = cols.index("estu_pais_reside") + 1
cols.insert(idx, "pobreza_monetaria_depto")  # insertarla en la posición deseada

df_icfes_2024 = df_icfes_2024[cols]

print(df_icfes_2024.head(15).to_string())


# **Dataset Índice de Desarrollo Humano (IDH) 2024**

# In[14]:


FILE_IDH = "../data/staging/idh_departamentos.csv"
df_idh_departamentos = pd.read_csv(FILE_IDH, encoding="utf-8")

print("Shape:", df_idh_departamentos.shape)
print(df_idh_departamentos.info())
print(df_idh_departamentos.head(33))   


# In[15]:


# Normalización
df_idh_departamentos["Entidad"] = (
    df_idh_departamentos["Entidad"]
    .apply(lambda x: ''.join(
        c for c in unicodedata.normalize('NFD', str(x))
        if unicodedata.category(c) != 'Mn'
    ))
)

df_idh_departamentos["Entidad"] = df_idh_departamentos["Entidad"].str.upper()

df_idh_departamentos["Entidad"] = df_idh_departamentos["Entidad"].replace(map_deptos)

print(df_idh_departamentos.head(33))


# In[16]:


# Unir de una sola vez con IDH y Población  por departamento de residencia
df_icfes_2024 = df_icfes_2024.merge(
    df_idh_departamentos.rename(columns={
        "IDH": "idh_depto",
        "Población": "poblacion_depto"
    }),
    how="left",
    left_on="estu_depto_reside",
    right_on="Entidad"
).drop(columns=["Entidad"])  # quitamos columna duplicada


# Reordenar columnas
cols = df_icfes_2024.columns.tolist()

# 1. Mover idh_depto después de estu_pais_reside
cols.remove("idh_depto")
idx = cols.index("estu_pais_reside") + 1
cols.insert(idx, "idh_depto")

# 2. Mover poblacion_depto después de estu_mcpio_reside
cols.remove("poblacion_depto")
idx = cols.index("estu_mcpio_reside") + 1
cols.insert(idx, "poblacion_depto")

df_icfes_2024 = df_icfes_2024[cols]

print(df_icfes_2024.head(15).to_string())



# **Dataset Población por Municipios (1100 registros 1016 municipios)**

# In[17]:


FILE_POBLACION_MUN = "../data/staging/poblacion_municipios.csv"
df_poblacion_municipios = pd.read_csv(FILE_POBLACION_MUN, encoding="utf-8")

print("Shape:", df_poblacion_municipios.shape)
print(df_poblacion_municipios.info())
print(df_poblacion_municipios.head(24))


# In[18]:


df_poblacion_municipios = df_poblacion_municipios.drop(columns=["URL"])


df_poblacion_municipios["Poblacion"] = (
    df_poblacion_municipios["Poblacion"]
    .str.replace("Población", "", regex=False) 
    .str.replace(".", "", regex=False)          
    .str.strip()                               
    .astype(int)                                
)

print("Shape después de limpieza:", df_poblacion_municipios.shape)
print(df_poblacion_municipios.info())
print(df_poblacion_municipios.head(24))


# In[19]:


# Normalización 1


# --- 1. Función de normalización ---
def normalizar(s):
    if pd.isna(s):
        return ""
    s = str(s).upper().strip()
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s

# --- 2. Alinear índices antes de aplicar normalización ---
df_icfes_2024 = df_icfes_2024.reset_index(drop=True)
df_poblacion_municipios = df_poblacion_municipios.reset_index(drop=True)

# --- 3. Normalizar columnas de ICFES ---
df_icfes_2024["estu_mcpio_reside"] = df_icfes_2024["estu_mcpio_reside"].apply(normalizar)
df_icfes_2024["cole_mcpio_ubicacion"] = df_icfes_2024["cole_mcpio_ubicacion"].apply(normalizar)
df_icfes_2024["estu_mcpio_presentacion"] = df_icfes_2024["estu_mcpio_presentacion"].apply(normalizar)

# --- 4. Normalizar columnas de Población ---
df_poblacion_municipios["Municipio_norm"] = df_poblacion_municipios["Municipio"].apply(normalizar)

# --- 5. Aplicar mapeo sobre la columna normalizada de población ---
df_poblacion_municipios["Municipio_mapeado"] = (
    df_poblacion_municipios["Municipio_norm"]
    .replace(map_mpios)
    .fillna(df_poblacion_municipios["Municipio_norm"])
)

# --- 6. Verificación extendida ---
print("\n--- Verificación general del dataset de municipios ---")

filas_antes = df_poblacion_municipios.shape[0]
filas_despues = df_poblacion_municipios["Municipio_mapeado"].shape[0]
municipios_unicos_reales = df_poblacion_municipios["Municipio_mapeado"].nunique()

# Municipios que se repiten en más de un departamento
duplicados = df_poblacion_municipios["Municipio_mapeado"].value_counts().gt(1)
municipios_repetidos = duplicados.sum()
filas_repetidas = df_poblacion_municipios["Municipio_mapeado"].duplicated().sum()

# Filas afectadas por el mapeo
filas_afectadas = (
    df_poblacion_municipios["Municipio_norm"] !=
    df_poblacion_municipios["Municipio_mapeado"]
).sum()

# --- Resultados ---
print(f"Filas totales ANTES del mapeo: {filas_antes}")
print(f"Filas totales DESPUÉS del mapeo: {filas_despues}")
print(f"Municipios distintos (sin repetir por depto): {municipios_unicos_reales}")  # Aqui faltan 2 porque son los del mapeo que no tiene equivalencia
print(f"Municipios con nombre repetido en otros deptos: {municipios_repetidos}")
print(f"Filas que corresponden a esos municipios repetidos: {filas_repetidas}")
print(f"Filas modificadas por el mapeo: {filas_afectadas}")

print("\nEjemplo de normalización y mapeo aplicado:")
print(df_poblacion_municipios[["Departamento", "Municipio", "Municipio_norm", "Municipio_mapeado"]].head(15))


# In[20]:


# Normalización 2

# --- 1. Normalización de Departamento ---
df_poblacion_municipios["Departamento_norm"] = df_poblacion_municipios["Departamento"].apply(normalizar)

# --- 2. Mapeo de Departamentos ---
df_poblacion_municipios["Departamento_mapeado"] = (
    df_poblacion_municipios["Departamento_norm"]
    .replace(map_deptos)
    .fillna(df_poblacion_municipios["Departamento_norm"])
)

# --- 3. Ajuste especial para Bogotá ---
# Si el municipio es "BOGOTA D.C." (ya normalizado y mapeado),
# se cambia el departamento a "BOGOTÁ, D.C."
mask_bogota = df_poblacion_municipios["Municipio_mapeado"] == "BOGOTA D.C."
df_poblacion_municipios.loc[mask_bogota, "Departamento_mapeado"] = "BOGOTÁ, D.C."

# --- 4. Verificación extendida ---
print("\n--- Verificación general del dataset de municipios ---")

# Totales antes y después (solo para control)
filas_antes = len(df_poblacion_municipios)
filas_despues = len(df_poblacion_municipios)

# Departamentos únicos antes y después
deptos_antes = df_poblacion_municipios["Departamento_norm"].nunique()
deptos_despues = df_poblacion_municipios["Departamento_mapeado"].nunique()

# Validar cuántas filas fueron afectadas por el ajuste especial
filas_bogota_cambiadas = mask_bogota.sum()

print(f"Filas totales ANTES: {filas_antes}")
print(f"Filas totales DESPUÉS: {filas_despues}")
print(f"Departamentos distintos ANTES: {deptos_antes}")
print(f"Departamentos distintos DESPUÉS: {deptos_despues}")
print(f"Filas modificadas por el ajuste especial del DC Bogotá: {filas_bogota_cambiadas}")

print("\nEjemplo de normalización y mapeo aplicado (primeras 15 filas):")
print(
    df_poblacion_municipios[
        ["Departamento", "Departamento_norm", "Departamento_mapeado", "Municipio_mapeado"]
    ].head(15)
)



# In[21]:


print("\nEjemplo final del dataset con mapeo aplicado:")
print(df_poblacion_municipios[["Departamento_mapeado", "Municipio_mapeado", "Poblacion"]].head(15))


# In[22]:


# === Unir con población por municipio y departamento de residencia ===
df_icfes_2024 = df_icfes_2024.merge(
    df_poblacion_municipios.rename(columns={
        "Poblacion": "poblacion_mcpio"
    })[["Departamento_mapeado", "Municipio_mapeado", "poblacion_mcpio"]],
    how="left",
    left_on=["estu_depto_reside", "estu_mcpio_reside"],
    right_on=["Departamento_mapeado", "Municipio_mapeado"]
).drop(columns=["Departamento_mapeado", "Municipio_mapeado"], errors="ignore")

# --- Reordenar columnas ---
cols = df_icfes_2024.columns.tolist()
if "poblacion_mcpio" in cols and "poblacion_depto" in cols:
    cols.remove("poblacion_mcpio")
    idx = cols.index("poblacion_depto") + 1
    cols.insert(idx, "poblacion_mcpio")
    df_icfes_2024 = df_icfes_2024[cols]

# --- Verificación rápida ---
print("\nEjemplo de unión final con población por municipio:")
print(df_icfes_2024[["estu_depto_reside", "estu_mcpio_reside", "poblacion_depto", "poblacion_mcpio"]].head(15))

# --- Resumen del merge ---
total_filas = len(df_icfes_2024)
coincidencias = df_icfes_2024["poblacion_mcpio"].notna().sum()
sin_coincidencia = df_icfes_2024["poblacion_mcpio"].isna().sum()

print("\n--- Resumen de la unión ---")
print(f"Filas totales: {total_filas}")
print(f"Filas con coincidencia (población encontrada): {coincidencias}")
print(f"Filas sin coincidencia (población no encontrada): {sin_coincidencia}")
print(f"Porcentaje de éxito del merge: {coincidencias / total_filas * 100:.2f}%")


# In[23]:


print(df_icfes_2024.head(20).to_string())


# ## **Valores nulos nuevas columnas** 

# In[24]:


print(df_icfes_2024.info())


# In[25]:


# --- Resumen rápido de nulos por columna ---
nulos = df_icfes_2024.isna().sum()
porcentaje = (nulos / len(df_icfes_2024)) * 100
resumen_nulos = pd.DataFrame({
    'Nulos': nulos,
    '% Nulos': porcentaje.round(2)
})
print("\n--- Resumen de valores nulos por columna ---")
print(resumen_nulos[resumen_nulos['Nulos'] > 0])


# In[26]:


# Filtro para EXTRANJERO
m = df_icfes_2024['estu_depto_reside'].str.strip().str.upper().eq('EXTRANJERO')

# Pone 0 (enteros) en poblaciones y 0.0 (floats) en idh/pobreza
df_icfes_2024.loc[m, ['poblacion_depto', 'poblacion_mcpio']] = 0
df_icfes_2024.loc[m, ['idh_depto', 'pobreza_monetaria_depto']] = 0.0

# Asegura tipos finales
df_icfes_2024['poblacion_depto'] = df_icfes_2024['poblacion_depto'].astype('Int64')
df_icfes_2024['poblacion_mcpio'] = df_icfes_2024['poblacion_mcpio'].astype('Int64')
df_icfes_2024[['idh_depto','pobreza_monetaria_depto']] = (
    df_icfes_2024[['idh_depto','pobreza_monetaria_depto']].astype(float)
)


# In[27]:


# --- Resumen rápido de nulos por columna ---
nulos = df_icfes_2024.isna().sum()
porcentaje = (nulos / len(df_icfes_2024)) * 100
resumen_nulos = pd.DataFrame({
    'Nulos': nulos,
    '% Nulos': porcentaje.round(2)
})
print("\n--- Resumen de valores nulos por columna ---")
print(resumen_nulos[resumen_nulos['Nulos'] > 0])


# In[29]:


df_icfes_2024_final = df_icfes_2024.copy()

df_icfes_2024_final.to_csv("../data/curated/df_icfes_2024_final.csv", index=False, encoding="utf-8-sig")

print("Archivo guardado en data/curated/df_icfes_2024_final.csv")

print("Shape:", df_icfes_2024_final.shape)



# In[30]:


print(df_icfes_2024_final.info())


# * pobreza_monetaria_depto no tiene todos los departamentos, por eso sale con datos nulos
# 
# * cuando en municipio o departamento sale "EXTRANJERO" en la otras columnas ponemos 0
# 
# * Municipios que hay en dataicfes pero que no tiene equivalencia con los Municipios de data población:  "RIO IRO": None, "SIPI": None
# 
# * Hay 16 Municipios que data ICFES tiene pero que no hay coincidencia con data Población Municipios, que corresponden a 1500 filas

# ## **GREATE EXPECTATIONS**

# In[31]:


df = pd.read_csv("../data/curated/df_icfes_2024_final.csv", encoding="utf-8-sig")

ge_df = PandasDataset(df)

print("Dataset listo para validaciones con Great Expectations 0.18.12")
print("Filas y columnas:", df.shape)


# **Expectativas críticas**

# In[32]:


# 1. Integridad
ge_df.expect_column_values_to_not_be_null("punt_global")


# 2. Dominio/rangos
ge_df.expect_column_values_to_be_between(
    "punt_global", 
    min_value=0, 
    max_value=500
)

# 3. Integridad
ge_df.expect_column_values_to_not_be_null("estu_depto_reside")



res1 = ge_df.expect_column_values_to_not_be_null("punt_global")
res2 = ge_df.expect_column_values_to_be_between("punt_global", min_value=0, max_value=500)
res3 = ge_df.expect_column_values_to_not_be_null("estu_depto_reside")

print(res1)
print(res2)
print(res3)


# **Otras Expectativas**

# In[33]:


# 4. Tipo de dato
res4 = ge_df.expect_column_values_to_be_of_type("periodo", "int64")

# 5. Tipo de dato
res5 = ge_df.expect_column_values_to_be_of_type("estu_pais_reside", "str")

# 6. Dominio/rangos
ge_df.expect_column_values_to_be_between("pobreza_monetaria_depto", min_value=0, max_value=100)

# 7. Dominio/rangos
ge_df.expect_column_values_to_be_between("idh_depto", min_value=0, max_value=1)

# 8. Regla entre columnas
mask = ge_df["punt_global"] >= ge_df["punt_matematicas"]
res8 = {
    "success": mask.all(),
    "unexpected_count": (~mask).sum(),
    "unexpected_percent": round((~mask).sum() / len(ge_df) * 100, 4)
}



res4 = ge_df.expect_column_values_to_be_of_type("periodo", "int64")

res5 = ge_df.expect_column_values_to_be_of_type("estu_pais_reside", "str")

res6 = ge_df.expect_column_values_to_be_between("pobreza_monetaria_depto", min_value=0, max_value=100)

res7 = ge_df.expect_column_values_to_be_between("idh_depto", min_value=0, max_value=1)

res8 = {"success": mask.all(),"unexpected_count": (~mask).sum(),"unexpected_percent": round((~mask).sum() / len(ge_df) * 100, 4)}


print(res4)
print(res5)
print(res6)
print(res7)
print(res8)


# **Expectation Suite**

# In[70]:


# usar gx directamente
ge_path = os.path.abspath(os.path.join("..", "src", "validation", "gx"))
context = ge.get_context(context_root_dir=ge_path)

print("Contexto cargado correctamente desde:", ge_path)


# In[71]:


suite_name = "icfes_suite"

# Crear suite si no existe
if suite_name not in [s.name for s in context.list_expectation_suites()]:
    context.add_expectation_suite(expectation_suite_name=suite_name)
    print(f"Suite '{suite_name}' creada correctamente.")
else:
    print(f"La suite '{suite_name}' ya existe.")

print("Ubicación esperada:", "gx/expectations/icfes_suite.json")


# In[72]:


# === 1. Registrar datasource ===
datasource_config = {
    "name": "runtime_pandas_datasource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"]
        }
    }
}

try:
    context.add_datasource(**datasource_config)
    print("Datasource 'runtime_pandas_datasource' agregado correctamente.")
except Exception as e:
    print("Posiblemente ya existe el datasource:", e)

# === 2. Crear RuntimeBatchRequest ===
batch_request = RuntimeBatchRequest(
    datasource_name="runtime_pandas_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="icfes_data",
    runtime_parameters={"batch_data": df_icfes_2024_final},
    batch_identifiers={"default_identifier_name": "default_id"},
)

# === 3. Crear Validator ===
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="icfes_suite"
)

print("Validator creado correctamente con el dataset:", validator.active_batch.data.dataframe.shape)


# In[73]:


# === 1. Expectativas de integridad ===
validator.expect_column_values_to_not_be_null("punt_global")
validator.expect_column_values_to_not_be_null("estu_depto_reside")

# === 2. Expectativas de dominio / rangos ===
validator.expect_column_values_to_be_between("punt_global", min_value=0, max_value=500)
validator.expect_column_values_to_be_between("pobreza_monetaria_depto", min_value=0, max_value=100)
validator.expect_column_values_to_be_between("idh_depto", min_value=0, max_value=1)

# === 3. Expectativas de tipo de dato ===
validator.expect_column_values_to_be_of_type("periodo", "int64")
validator.expect_column_values_to_be_of_type("estu_pais_reside", "str")

# === 4. Relación entre columnas ===
validator.expect_column_pair_values_A_to_be_greater_than_B(
    column_A="punt_global",
    column_B="punt_matematicas",
    or_equal=True,
    meta={"description": "punt_global debe ser >= punt_matematicas"}
)

# === 5. Guardar la suite actualizada ===
validator.save_expectation_suite(discard_failed_expectations=False)
print("Expectation suite actualizada y guardada en:")
print("src/validation/gx/expectations/icfes_suite.json")


# **Checkpoint**

# In[76]:


# === Crear el checkpoint ===
checkpoint_name = "checkpoint_icfes_curated"

checkpoint = SimpleCheckpoint(
    name=checkpoint_name,
    data_context=context,
)

# === Ejecutar el checkpoint con la validación explícita ===
results = checkpoint.run(
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "icfes_suite",
        }
    ]
)

print("Checkpoint ejecutado correctamente.")


# **reporte HTML**

# In[77]:


# === Generar y abrir Data Docs ===
context.build_data_docs()  # genera el HTML
local_site_path = context.get_docs_sites_urls()[0]["site_url"]
print("Data Docs generados en:", local_site_path)


# Las validaciones de calidad se ejecutaron sobre el dataset curated (df_icfes_2024_final), ya que esta capa representa la versión final, limpia y consolidada de los datos antes de su análisis o carga al Data Warehouse. En este punto se garantiza la integridad, los tipos de datos y los rangos definidos en las expectativas, asegurando que la información final cumpla con los estándares de calidad establecidos.
