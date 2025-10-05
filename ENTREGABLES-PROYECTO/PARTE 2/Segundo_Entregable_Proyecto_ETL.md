# ETL Project – **Second Delivery**

## Getting Started
En este segundo entregable llevarás tu trabajo al siguiente nivel: **incorporarás una segunda fuente de datos mediante Web Scraping**, aplicarás **validación de datos con Great Expectations** y **cargarás un Data Warehouse (DW)** con un **modelo dimensional** para habilitar consultas analíticas y visualizaciones **conectadas a la base de datos** (no desde CSV).

> Resultado esperado: un repositorio reproducible que extrae y enriquece datos desde al menos dos fuentes (una de ellas vía scraping), valida su calidad, los transforma y los carga en un DW con **hechos y dimensiones**, dejando consultas y visualizaciones que consumen directamente desde el DW.

---

## What is Expected
1. **Fuentes de Datos**
   - **Primaria**: puede ser la misma del primer entregable (volumen sugerido: ≥ 10k filas).
   - **Segunda fuente – Web Scraping**: sitio web **real y permitido**. Revisa `robots.txt`, documenta rate limiting, manejo de errores y *backoff*. El objetivo es **enriquecer** el dataset con atributos complementarios (p. ej., categoría, especificaciones, ubicación, metadatos).

2. **Pipeline ETL Reproducible**
   - **Extract**: lectura de fuente(s) original(es) + scraping (requests/BS4/Selenium u otra librería).
   - **Transform**: estandarización, tipificación, normalización, deduplicación, *feature engineering* si aplica.
   - **Load a DW**: carga a **PostgreSQL** o **MySQL** (recomendado Docker). Implementa un **modelo dimensional** con:
     - 1 **tabla de hechos** con métricas aditivas (precio, conteos, ventas, etc.).
     - 3 **dimensiones** (obligatoria **dim_date** + 2 de negocio, p. ej., producto, ubicación, categoría, cliente).
     - Claves sustitutas en dimensiones y llaves foráneas en la tabla de hechos.
     - Granularidad de la fact definida y documentada.

3. **Validación con Great Expectations (GE)**
   - Crear una **Expectation Suite** con **≥ 8 expectativas** (mínimo **2 críticas**), ejemplos:
     - Integridad: `expect_column_values_to_not_be_null` en llaves/métricas clave.
     - Unicidad: `expect_column_values_to_be_unique` en llaves naturales.
     - Dominio/rangos: `expect_column_values_to_be_between`, `to_be_in_set`.
     - Formato: `expect_column_values_to_match_regex`.
     - Reglas entre columnas (p. ej., `price >= 0`, fechas válidas).
   - Configurar **Checkpoint** y generar **Data Docs** (HTML) o reporte equivalente.
   - Ejecutar validaciones en **staging** y/o **curated** (explica en qué capa y por qué).

4. **Consultas Analíticas y Visualizaciones**
   - **5+ consultas SQL** sobre el DW: *Top-N*, series de tiempo, cortes por dimensión, KPIs.
   - **3+ visualizaciones** (barras, líneas, mapas si aplica) **alimentadas desde el DW** (no desde CSV).
   - Explica el *insight* de cada visual.

5. **Entregables Reproducibles**
   - **Infra**: `docker-compose.yml` (o `docker run`) para la BD; `schema.sql` con el modelo dimensional; scripts/notebooks para extract/transform/load/validar.
   - **Ejecución**: `README.md` con pasos (incluye entorno con **uv**), variables de entorno, y orden de ejecución.
   - **Diagrama**: flujo (fuentes → staging → validación → curated → DW → BI) y **esquema estrella**.
   - **Bitácora**: decisiones de modelado y reglas de limpieza/conformidad.

---

## Technologies
- **Python** y **Jupyter Notebook** para extracción, transformación, validación y documentación.
- **Base de datos** relacional (**PostgreSQL** o **MySQL** en Docker) como **DW**.
- **Visualizaciones** (matplotlib/plotly/seaborn) **leyendo desde el DW**.
- **git / GitHub** para versionamiento y entrega.
- **Web Scraping**: `requests`, `beautifulsoup4`, `lxml` (o `playwright`/`selenium` si es imprescindible el rendering).
- **Great Expectations** para suites, Data Docs y checkpoints.
- **SQLAlchemy** + driver de BD (`psycopg2-binary` para Postgres o `mysqlclient`/`mysql-connector-python` para MySQL).
- Diagrama: mermaid/draw.io u otra herramienta.

> Sugerido: gestionar entorno con **uv** (Python 3.11 o 3.12 para compatibilidad con GE).

---

## Repository Structure (sugerido)
```
second_delivery/
├─ README.md
├─ pyproject.toml
├─ docker-compose.yml
├─ .env.example
├─ data/
│  ├─ raw/            # CSV(s) original(es), HTML/JSON cacheados del scraping
│  ├─ staging/        # datasets intermedios tras limpieza
│  └─ curated/        # datasets finales para carga
├─ src/
│  ├─ scraping/       # scripts de scraping (paginar, parsear, cachear)
│  ├─ transform/      # limpieza, normalización, conformidad
│  ├─ dw/             # carga de dimensiones/hechos, SCD si aplica
│  └─ validation/     # suites GE, checkpoints, Data Docs
├─ dw/
│  ├─ schema.sql      # modelo dimensional (hechos + dimensiones)
│  └─ seed.sql        # semillas opcionales
├─ notebooks/
│  ├─ 01_scraping.ipynb
│  ├─ 02_cleaning_and_ge.ipynb
│  ├─ 03_dw_load.ipynb
│  └─ 04_analytics.ipynb
└─ docs/
   ├─ diagram_flow.png
   ├─ star_schema.png
   └─ validation_report.md
```

> Puedes adaptar la estructura; lo importante es que sea **clara y reproducible**.

---

## Detailed Requirements

### 1) Web Scraping (segunda fuente)
- **Objetivo**: enriquecer el dataset con atributos complementarios (categoría, specs, ubicación estandarizada, rating, etc.).
- **Alcance mínimo**:
  - Scraping de **≥ 1.000 registros** (o agregado equivalente si el sitio limita).
  - Respeto a **`robots.txt`**, *rate limiting* y *backoff* (dormir 1–3 s entre requests; reintentos con `Retry-After`).
  - **Cache** local de HTML/JSON para reproducibilidad.
  - Limpieza de campos (tipos, unidades, fechas, duplicados).
- **Entregables**: script(s) `src/scraping/`, evidencia de ejecución (log/notebook) y datos en `data/raw/` (o `staging/`).

### 2) Validación con Great Expectations
- **Suite** con **≥ 6** expectativas (≥ 2 críticas) cubriendo integridad, unicidad, dominios, formatos y reglas entre columnas.
- **Checkpoint** y **Data Docs** generados (incluye HTML en `docs/` o carpeta de GE).
- **Reporte** de resultados (éxitos/fallos y próximos pasos).

### 3) Data Warehouse (modelo dimensional)
- **Modelo estrella** con:
  - **1 Fact Table** con métricas aditivas y FKs a dimensiones.
  - **3+ Dim Tables** (incluye **dim_date**).
  - Claves sustitutas (INT/BIGINT) en dimensiones.
  - `dw/schema.sql` y scripts de carga en `src/dw/`.
- **Criterios**: granularidad de la fact definida; conformidad de dimensiones si hay múltiples flujos.

### 4) ETL de punta a punta
- Scripts/notebooks para **E→T→L** con orden de ejecución documentado.
- Uso de Docker para BD (y opcionalmente para jobs).

### 5) Análisis y Visualizaciones (desde el DW)
- **5+ consultas SQL** representativas (agregaciones, *Top-N*, *time series*, *drill-down*).
- **3+ visualizaciones** (barras, líneas, mapas) **conectadas a la BD**.
- Explica el insight de cada visual.

---

## How to Run (ejemplo con uv + Postgres)
```bash
# 1) Crear entorno
uv python install 3.11
uv venv -p 3.11 .venv
source .venv/bin/activate   # Windows: .\.venv\Scripts\Activate.ps1

# 2) Instalar dependencias
uv add "pandas==2.1.4" "numpy==1.26.4"        "requests" "beautifulsoup4" "lxml"        "great-expectations==1.6.0"        "SQLAlchemy>=2" "psycopg2-binary"        "matplotlib" "seaborn"

# 3) Levantar la base de datos (PostgreSQL)
docker compose up -d
# DSN: postgresql://etluser:etlpass@localhost:5432/dw

# 4) Crear esquema DW
#   a) Ejecuta dw/schema.sql en la BD, o
#   b) uv run python src/dw/init_dw.py (si lo implementas)
# 5) Ejecutar scraping, limpieza, validación y carga
uv run python src/scraping/run_scraper.py
uv run python src/transform/clean_and_conform.py
uv run python src/validation/run_ge_checkpoint.py
uv run python src/dw/load_dimensions.py
uv run python src/dw/load_fact.py

# 6) Abrir notebooks de análisis (conexión directa al DW)
jupyter notebook notebooks/04_analytics.ipynb
```

> Si eliges **MySQL**, cambia driver y DSN en tus scripts.

---

## Deliverables (Checklist)
- [ ] `README.md` con pasos de ejecución y orden de jobs.
- [ ] Código de **scraping** (con cache y respeto a `robots.txt`).
- [ ] **Suites GE**, **Checkpoint** y **Data Docs** (HTML/reporte).
- [ ] **DW**: `schema.sql`, scripts de carga de dimensiones y hechos.
- [ ] **Consultas SQL** (archivo `.sql` o en notebook).
- [ ] **Visualizaciones** conectadas a la BD.
- [ ] **Diagramas** (flujo y star schema).
- [ ] **docker-compose.yml** (y/o comandos `docker run`).
- [ ] `pyproject.toml` y `uv.lock` (opcional).
- [ ] **Bitácora** de decisiones y supuestos.

---

## Evaluation Rubric
| Criterio | Peso | Excelente | Satisfactorio | Insuficiente |
|---|---:|---|---|---|
| Web Scraping | 20% | Respeta robots; datos limpios y útiles; cache y reintentos | Scraping básico sin cache ni manejo de errores | Incumple robots o scraping inestable |
| Validación (GE) | 20% | ≥ 6 expectativas, 2 críticas; Data Docs claros | Cumple mínimos sin cobertura crítica | Cobertura pobre o sin reportes |
| Modelo Dimensional | 20% | Granularidad clara; FK/SK correctas; dimensiones bien diseñadas | Modelo estrella válido con detalles menores | Modelo inconsistente o no dimensional |
| ETL Reproducible | 20% | Jobs encadenados y repetibles; uso de Docker | Reproducible con pasos manuales | Difícil de reproducir |
| Analítica & Viz (desde DW) | 15% | 5+ SQL útiles; 3+ gráficos con insight | Cumple mínimos sin mucha interpretación | Poca conexión al DW o insights débiles |
| Documentación | 5% | README y diagramas claros | Documentación básica | Documentación insuficiente |

---

## Notes & Constraints
- Cumple políticas del sitio **scrapeado** y límites de tasa.
- Los **gráficos deben consumir desde el DW** (no desde CSV).
- Mantén **commits** atómicos y mensajes descriptivos.
