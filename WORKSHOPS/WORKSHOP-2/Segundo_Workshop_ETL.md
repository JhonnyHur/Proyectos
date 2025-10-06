# Workshop #2 – De la Web al Data Warehouse (Segundo Corte)

## Objetivo
Construir un mini pipeline que combine **Web Scraping**, **Great Expectations**, **ETL** y **Data Warehouse** con modelo dimensional.

## Estructura
- `data/raw` contiene `used_cars_base.csv` (300 filas).
- `docker-compose.yml` levanta **PostgreSQL 15**.
- `dw/schema.sql` define un **star schema** mínimo (dim_date, dim_brand, dim_model, dim_location, fact_listings).
- `docs/ALT_mysql_docker_run.md` explica una alternativa en **MySQL** usando `docker run`.

## Segunda fuente (scraping)
Usa una web real **permitida** (p. ej., **Wikipedia** o portales de datos abiertos) para enriquecer dimensiones (`brand_country`, `segment`, etc.). Respeta `robots.txt` y límites de tasa. Documenta lo que hagas.

## Pasos sugeridos
1. Cargar `data/raw/used_cars_base.csv` → limpiar y normalizar (staging).
2. Scraping de segunda fuente → enriquecer dimensiones (`brand`/`model`/`location`).
3. Validar con **Great Expectations** (8+ expectativas; 2 críticas; Data Docs).
4. Generar datasets **curated** y cargar **DW** con scripts reproducibles.
5. Ejecutar 5+ consultas SQL y 3+ visualizaciones.

## Arranque rápido
```bash
uv python install 3.11
uv venv -p 3.11 .venv
source .venv/bin/activate   # Windows: .\.venv\Scripts\Activate.ps1
uv add "numpy==1.26.4" "pandas==2.1.4" "great-expectations==1.6.0"        "requests" "beautifulsoup4" "lxml"        "SQLAlchemy>=2" "psycopg2-binary" "matplotlib"

docker compose up -d
# DSN: postgresql://etluser:etlpass@localhost:5432/dw
```




## Informacion Postgress (usuarios y contraseñas:

url interfaz grafica: http://localhost:8080

Cuando abras pgAdmin te pedirá login:
Correo: admin@admin.com
Contraseña: admin

## Configuracion Servidos postgress

Pestaña General
Nombre: workshop2_DW

Pestaña Conexión
Nombre/Dirección de servidor: postgres-dw
Puerto: 5432

Base de datos de mantenimiento: dw

Nombre de usuario: etluser

Contraseña: etlpass

Marca la opción ¿Salvar contraseña? para no tener que escribirla siempre.