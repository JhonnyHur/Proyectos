# etl/analytics.py
from pathlib import Path
import os
import pandas as pd

# Renderizado sin interfaz (headless)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from sqlalchemy import create_engine, text

# === Rutas y conexión ===
OUTDIR = Path("/opt/airflow/data/output")
OUTDIR.mkdir(parents=True, exist_ok=True)

DW_CONN_URI = os.getenv(
    "DW_CONN_URI",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/dw"
)

def _save_fig(fig: plt.Figure, name: str):
    path = OUTDIR / f"{name}.png"
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"[ANALYTICS] Gráfico guardado: {path}")
    return path

def analytics():
    print("== ANALYTICS: inicio ==")

    # Conexión
    print(f"[ANALYTICS] Conectando a DW: {DW_CONN_URI}")
    engine = create_engine(DW_CONN_URI)

    # ------------------ Viz 1: IDH vs Tasa alto desempeño ------------------
    query_tasa = """
    SELECT 
        d.id_departamento,
        d.estu_depto_reside AS nombre_departamento,
        ROUND(CAST(d.idh_depto AS numeric), 3) AS idh,
        d.poblacion_depto,
        COUNT(f.punt_global) AS total_estudiantes,
        SUM(CASE WHEN f.punt_global >= 300 THEN 1 ELSE 0 END) AS alto_desempeno,
        ROUND(
            CAST(
                SUM(CASE WHEN f.punt_global >= 300 THEN 1 ELSE 0 END) * 1000.0 
                / NULLIF(d.poblacion_depto, 0)
            AS numeric),
        3) AS tasa_alto_desempeno_1000
    FROM fact_icfes f
    JOIN dim_departamento d 
        ON f.id_departamento = d.id_departamento
    GROUP BY d.id_departamento, d.estu_depto_reside, d.idh_depto, d.poblacion_depto
    ORDER BY tasa_alto_desempeno_1000 DESC;
    """
    df_tasa = pd.read_sql_query(text(query_tasa), engine)

    fig1 = plt.figure(figsize=(12, 7))
    sns.scatterplot(
        data=df_tasa,
        x="idh",
        y="tasa_alto_desempeno_1000",
        size="poblacion_depto",
        hue="idh",
        palette="viridis",
        sizes=(100, 1500),
        alpha=0.8,
        edgecolor="black",
        legend="brief",
    )
    top5 = df_tasa.nlargest(5, "tasa_alto_desempeno_1000")
    bottom5 = df_tasa.nsmallest(5, "tasa_alto_desempeno_1000")
    for _, r in pd.concat([top5, bottom5]).iterrows():
        plt.text(r["idh"], r["tasa_alto_desempeno_1000"], r["nombre_departamento"],
                 fontsize=9, fontweight='bold', ha='left', va='bottom')
    plt.title("IDH vs Tasa de Alto Desempeño (por cada 1000 habitantes)")
    plt.xlabel("Índice de Desarrollo Humano (IDH)")
    plt.ylabel("Tasa de Alto Desempeño por cada 1000 habitantes")
    plt.grid(alpha=0.3)
    plt.legend(title="IDH / Población", bbox_to_anchor=(1.05, 1), loc='upper left')
    _save_fig(fig1, "viz01_idh_vs_tasa")

    # --------------- Viz 2: Pobreza vs Tasa alto desempeño -----------------
    query_pobreza = """
    SELECT 
        d.id_departamento,
        d.estu_depto_reside AS nombre_depto,
        ROUND(CAST(d.pobreza_monetaria_depto AS numeric), 2) AS pobreza,
        d.poblacion_depto,
        COUNT(f.punt_global) AS total_estudiantes,
        SUM(CASE WHEN f.punt_global >= 300 THEN 1 ELSE 0 END) AS alto_desempeno,
        ROUND(
            CAST(
                SUM(CASE WHEN f.punt_global >= 300 THEN 1 ELSE 0 END) * 1000.0 
                / NULLIF(d.poblacion_depto, 0)
            AS numeric),
        3) AS tasa_alto_desempeno_1000
    FROM fact_icfes f
    JOIN dim_departamento d 
        ON f.id_departamento = d.id_departamento
    GROUP BY d.id_departamento, d.estu_depto_reside, d.poblacion_depto, d.pobreza_monetaria_depto
    ORDER BY pobreza ASC;
    """
    df_pobreza = pd.read_sql_query(text(query_pobreza), engine)

    fig2 = plt.figure(figsize=(13, 7))
    sns.scatterplot(
        data=df_pobreza,
        x="pobreza",
        y="tasa_alto_desempeno_1000",
        size="poblacion_depto",
        hue="pobreza",
        palette="RdYlGn_r",
        sizes=(50, 1500),
        alpha=0.7,
        legend="brief",
    )
    destacados = pd.concat([
        df_pobreza.nlargest(10, 'tasa_alto_desempeno_1000'),
        df_pobreza.nsmallest(10, 'tasa_alto_desempeno_1000')
    ])
    for _, r in destacados.iterrows():
        plt.text(r['pobreza'] + 0.3, r['tasa_alto_desempeno_1000'] + 0.05,
                 r['nombre_depto'], fontsize=8.5, weight='bold', color='black')
    plt.title("Pobreza Monetaria vs Tasa de Alto Desempeño (por cada 1000 habitantes)")
    plt.xlabel("Pobreza Monetaria Departamental (%)")
    plt.ylabel("Tasa de Alto Desempeño por cada 1000 habitantes")
    plt.legend(title="Pobreza / Población", loc="upper right")
    plt.grid(True, linestyle="--", alpha=0.4)
    _save_fig(fig2, "viz02_pobreza_vs_tasa")

    # ----- Viz 3: NSE promedio (municipal) vs Tasa alto desempeño (mun) ----
    query_nse = """
    SELECT 
        m.id_municipio,
        m.estu_mcpio_reside AS nombre_municipio,
        ROUND(CAST(AVG(c.estu_nse_individual) AS numeric), 2) AS nse_promedio,
        m.poblacion_mcpio,
        COUNT(f.punt_global) AS total_estudiantes,
        SUM(CASE WHEN f.punt_global >= 300 THEN 1 ELSE 0 END) AS alto_desempeno,
        ROUND(
            CAST(
                SUM(CASE WHEN f.punt_global >= 300 THEN 1 ELSE 0 END) * 1000.0 
                / NULLIF(m.poblacion_mcpio, 0)
            AS numeric),
        3) AS tasa_alto_desempeno_1000
    FROM fact_icfes f
    JOIN dim_municipio m 
        ON f.id_municipio = m.id_municipio
    JOIN dim_contexto_socioeconomico c 
        ON f.id_contexto = c.id_contexto
    GROUP BY m.id_municipio, m.estu_mcpio_reside, m.poblacion_mcpio
    ORDER BY tasa_alto_desempeno_1000 DESC;
    """
    df_nse = pd.read_sql_query(text(query_nse), engine)

    df_nse_plot = (
        df_nse.dropna(subset=["poblacion_mcpio", "tasa_alto_desempeno_1000"])
             .query("tasa_alto_desempeno_1000 < 50")
    )

    fig3 = plt.figure(figsize=(12, 7))
    sns.scatterplot(
        data=df_nse_plot,
        x="nse_promedio",
        y="tasa_alto_desempeno_1000",
        size="poblacion_mcpio",
        hue="nse_promedio",
        palette="viridis",
        sizes=(40, 800),
        alpha=0.75,
        legend="brief",
    )
    plt.title("NSE Promedio vs Tasa de Alto Desempeño (<50 por cada 1000 hab.)",
              fontsize=12, weight="bold")
    plt.xlabel("Nivel Socioeconómico Promedio (NSE)")
    plt.ylabel("Tasa de Alto Desempeño por cada 1000 habitantes")
    plt.grid(True, linestyle="--", alpha=0.4)
    _save_fig(fig3, "viz03_nse_vs_tasa")

    print("== ANALYTICS: fin ==")
