#!/usr/bin/env python
# coding: utf-8

# In[1]:


from sqlalchemy import create_engine, text
import pandas as pd


# ## **Conexión a PostgreSQL**

# In[2]:


# Credenciales de conexión
db_user = "etl25"
db_pass = "etl25"
db_host = "localhost"
db_port = "5433"
db_name = "dw_icfes"

# Crear conexión
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

# Probar conexión
with engine.connect() as conn:
    result = conn.execute(text("SELECT version();"))
    print("Conexión exitosa a PostgreSQL")
    print(result.scalar())


# ## **Creación del Esquema**

# In[10]:


# --- Ruta del archivo SQL ---
schema_path = "../dw/schema.sql"

# --- Leer el contenido del archivo SQL ---
with open(schema_path, "r", encoding="utf-8") as f:
    schema_sql = f.read()

# --- Ejecutar el script en la base de datos ---
with engine.connect() as conn:
    conn.execute(text(schema_sql))
    conn.commit()

print("Esquema del Data Warehouse creado exitosamente.")


# ## **Insersión de Datos**

# In[4]:


# === Cargar dataset final desde curated ===
df = pd.read_csv("../data/curated/df_icfes_2024_final.csv")

print("Dataset cargado correctamente:", df.shape)
df.head(5)


# In[11]:


# === 1. dim_departamento ===
dim_departamento = (
    df[["estu_depto_reside", "poblacion_depto", "idh_depto", "pobreza_monetaria_depto"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
print("Departamentos:", dim_departamento.shape)

# === 2. dim_municipio ===
dim_municipio = (
    df[["estu_mcpio_reside", "poblacion_mcpio"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
print("Municipios:", dim_municipio.shape)

# === 3. dim_contexto_socioeconomico ===
dim_contexto = (
    df[["estu_inse_individual", "estu_nse_individual", "fami_estratovivienda"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
print("Contexto socioeconómico:", dim_contexto.shape)

# === 4. dim_colegio ===
dim_colegio = (
    df[[
        "cole_area_ubicacion", "cole_bilingue", "cole_calendario", "cole_naturaleza",
        "cole_depto_ubicacion", "estu_nse_establecimiento"
    ]]
    .drop_duplicates()
    .reset_index(drop=True)
)
print("Colegios:", dim_colegio.shape)

# === 5. dim_fecha ===
dim_fecha = (
    df[["periodo"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
print("Fechas:", dim_fecha.shape)


# In[12]:


# === Paso 3: Insertar dimensiones en PostgreSQL ===
dim_departamento.to_sql("dim_departamento", con=engine, if_exists="append", index=False)
dim_municipio.to_sql("dim_municipio", con=engine, if_exists="append", index=False)
dim_contexto.to_sql("dim_contexto_socioeconomico", con=engine, if_exists="append", index=False)
dim_colegio.to_sql("dim_colegio", con=engine, if_exists="append", index=False)
dim_fecha.to_sql("dim_fecha", con=engine, if_exists="append", index=False)

print("Dimensiones insertadas exitosamente en PostgreSQL.")


# **TABLA DE HECHOS**

# In[13]:


# === Paso 4: Construir tabla de hechos fact_icfes ===

# --- Unir con IDs de las dimensiones (solo claves necesarias) ---
fact_icfes = df.merge(dim_departamento.reset_index().rename(columns={"index": "id_departamento"}), 
                        on=["estu_depto_reside", "poblacion_depto", "idh_depto", "pobreza_monetaria_depto"], 
                        how="left")

fact_icfes = fact_icfes.merge(dim_municipio.reset_index().rename(columns={"index": "id_municipio"}), 
                                on=["estu_mcpio_reside", "poblacion_mcpio"], 
                                how="left")

fact_icfes = fact_icfes.merge(dim_contexto.reset_index().rename(columns={"index": "id_contexto"}), 
                                on=["estu_inse_individual", "estu_nse_individual", "fami_estratovivienda"], 
                                how="left")

fact_icfes = fact_icfes.merge(dim_colegio.reset_index().rename(columns={"index": "id_colegio"}), 
                                on=["cole_area_ubicacion", "cole_bilingue", "cole_calendario", 
                                    "cole_naturaleza", "cole_depto_ubicacion", 
                                    "estu_nse_establecimiento"], 
                                how="left")

fact_icfes = fact_icfes.merge(dim_fecha.reset_index().rename(columns={"index": "id_fecha"}), 
                                on="periodo", how="left")



# In[17]:


# --- Seleccionar solo métricas y claves foráneas ---
fact_icfes = fact_icfes[[
    "id_departamento", "id_municipio", "id_contexto", "id_colegio", "id_fecha",
    "percentil_c_naturales", "percentil_global", "percentil_ingles",
    "percentil_lectura_critica", "percentil_matematicas", "percentil_sociales_ciudadanas",
    "punt_c_naturales", "punt_ingles", "punt_lectura_critica",
    "punt_matematicas", "punt_sociales_ciudadanas", "punt_global"
]]

print("Fact table shape:", fact_icfes.shape)
fact_icfes.head(3)



# In[16]:


# --- Ajustar índices para que coincidan con las FK en PostgreSQL ---
fact_icfes[["id_departamento", "id_municipio", "id_contexto", "id_colegio", "id_fecha"]] += 1


# In[18]:


fact_icfes.to_sql("fact_icfes", con=engine, if_exists="append", index=False)
print("Tabla de hechos cargada exitosamente en PostgreSQL.")


# Granularidad de la fact table (fact_icfes)
# Cada registro de la tabla de hechos representa el desempeño académico individual de un estudiante en una única presentación del examen ICFES Saber 11, en un periodo determinado.
# 
# Este nivel de detalle permite analizar los resultados a nivel:
# 
# de estudiante (micro),
# 
# de institución educativa (agregando por colegio),
# 
# de municipio o departamento,
# 
# y de año o periodo de presentación.
# 
# Las medidas asociadas son los puntajes y percentiles por área, y las dimensiones relacionadas son: Departamento, Municipio, Colegio, Contexto Socioeconómico y Fecha.
