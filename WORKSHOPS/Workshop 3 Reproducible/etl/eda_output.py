from typing import Union
from pathlib import Path
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def run_eda(base_output_dir: Union[Path, str] = "/opt/airflow/data/output"):
    base = Path(base_output_dir)
    out = base / "eda_output"
    out.mkdir(parents=True, exist_ok=True)

    fact = pd.read_csv(base / "fact_transactions.csv")
    daily = pd.read_csv(base / "agg_daily.csv")

    # ---------- Calidad ----------
    nulls = fact.isna().sum().sort_values(ascending=False)
    dups = int(fact.duplicated(subset=["txn_id"]).sum())
    nulls.to_csv(out / "nulls_fact.csv", index=True)

    fact["_ts_parsed"] = pd.to_datetime(fact["ts"], errors="coerce")
    invalid_dates = int(fact["_ts_parsed"].isna().sum())

    plt.figure(figsize=(8,4))
    sns.boxplot(x=fact["amount_usd"])
    plt.title("Outliers en amount_usd")
    plt.tight_layout()
    plt.savefig(out / "outliers_amount_usd.png", dpi=150)
    plt.close()

    # ---------- Monedas ----------
    plt.figure(figsize=(8,4))
    (fact["currency"].value_counts(normalize=True).mul(100)).plot(kind="bar")
    plt.title("Distribución de monedas (%)")
    plt.ylabel("%")
    plt.tight_layout()
    plt.savefig(out / "monedas_distribution.png", dpi=150)
    plt.close()

    fig, ax = plt.subplots(1,2, figsize=(10,4))
    sns.histplot(fact["amount"], bins=30, kde=True, ax=ax[0], color="steelblue")
    sns.histplot(fact["amount_usd"], bins=30, kde=True, ax=ax[1], color="orange")
    ax[0].set_title("Montos originales")
    ax[1].set_title("Montos en USD")
    fig.tight_layout()
    fig.savefig(out / "amount_vs_amount_usd.png", dpi=150)
    plt.close(fig)

    top_regions = (
        fact.groupby("region")["amount_usd"]
        .sum().sort_values(ascending=False).head(10)
    )
    plt.figure(figsize=(8,4))
    sns.barplot(x=top_regions.index, y=top_regions.values)
    plt.title("Regiones líderes por total USD")
    plt.ylabel("Total USD")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(out / "regiones_lideres_total_usd.png", dpi=150)
    plt.close()

    # ---------- Clientes ----------
    fact["_month"] = pd.to_datetime(fact["ts"], errors="coerce").dt.to_period("M").astype(str)
    cohort = fact.groupby("_month")["customer_id"].nunique().reset_index(name="clientes_activos")
    cohort.to_csv(out / "cohort_clientes_activos.csv", index=False)

    plt.figure(figsize=(10,4))
    sns.barplot(data=cohort, x="_month", y="clientes_activos", color="skyblue")
    plt.title("Clientes activos por mes (cohorte por transacción)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(out / "cohorte_clientes_mes.png", dpi=150)
    plt.close()

    region_clientes = fact.groupby("region")["customer_id"].nunique().sort_values(ascending=False)
    plt.figure(figsize=(8,4))
    sns.barplot(x=region_clientes.index, y=region_clientes.values)
    plt.title("Clientes únicos por región")
    plt.ylabel("Clientes")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(out / "clientes_por_region.png", dpi=150)
    plt.close()

    # ---------- Productos ----------
    ticket = (
        fact.groupby("category")["amount_usd"]
        .agg(["count","mean","std","min","max"]).reset_index()
    )
    ticket.to_csv(out / "ticket_por_categoria.csv", index=False)

    plt.figure(figsize=(10,5))
    sns.barplot(data=ticket, x="category", y="mean", color="mediumseagreen")
    plt.title("Ticket promedio por categoría")
    plt.xticks(rotation=45)
    plt.ylabel("USD")
    plt.tight_layout()
    plt.savefig(out / "ticket_promedio_categoria.png", dpi=150)
    plt.close()

    combo = fact.groupby(["country","category"]).size().reset_index(name="n")
    rare = combo.sort_values("n").head(10)
    rare.to_csv(out / "combos_raros_country_category.csv", index=False)

    plt.figure(figsize=(8,5))
    sns.barplot(data=rare, x="n", y="country", hue="category")
    plt.title("Combinaciones menos frecuentes country × category")
    plt.legend(bbox_to_anchor=(1,1))
    plt.tight_layout()
    plt.savefig(out / "combos_raros_country_category.png", dpi=150)
    plt.close()

    # ---------- Temporal ----------
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
    daily = daily.sort_values("date")
    plt.figure(figsize=(10,5))
    plt.plot(daily["date"], daily["total_usd"], label="Diario (USD)", color="teal")
    plt.title("Evolución diaria del total USD")
    plt.xlabel("Fecha"); plt.ylabel("Total USD")
    plt.legend(); plt.tight_layout()
    plt.savefig(out / "evolucion_total_usd_diario.png", dpi=150)
    plt.close()

    daily["rolling7"] = daily["total_usd"].rolling(7, min_periods=1).mean()
    plt.figure(figsize=(10,5))
    plt.plot(daily["date"], daily["total_usd"], alpha=0.5, label="Diario")
    plt.plot(daily["date"], daily["rolling7"], color="red", linewidth=2, label="Media móvil 7d")
    plt.title("Picos y caídas (media móvil semanal)")
    plt.xlabel("Fecha"); plt.ylabel("USD")
    plt.legend(); plt.tight_layout()
    plt.savefig(out / "picos_caidas_semana.png", dpi=150)
    plt.close()

    summary = {
        "fact_shape": [int(fact.shape[0]), int(fact.shape[1])],
        "daily_shape": [int(daily.shape[0]), int(daily.shape[1])],
        "duplicates_txn_id": dups,
        "invalid_ts": invalid_dates,
        "top_nulls": {k: int(v) for k, v in nulls.head(10).to_dict().items()},
    }
    (out / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"[EDA] Artefactos en: {out}")