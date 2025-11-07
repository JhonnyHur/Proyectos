## 1. Requisitos

- **Docker** y **Docker Compose** instalados en tu máquina.
- Puertos disponibles:
  - **8080** para Airflow Webserver
  - **5432** para PostgreSQL

---

## 2. Crear archivo `.env`

En la raíz del proyecto, crea un archivo llamado `.env` con el siguiente contenido:

```env
FERNET_KEY=<TU_LLAVE_GENERADA>
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
_PIP_ADDITIONAL_REQUIREMENTS=pandas numpy matplotlib seaborn pyarrow fastparquet
```

### Generar la clave `FERNET_KEY`

Ejecuta en tu terminal:

```bash
python - << 'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
```

Esto generará algo como:

```
LXLNdvx0aVk63E8_c2igw9DkH4Tf6xg4bXWClb2ezjI=
```

Copia ese valor en `FERNET_KEY=` sin comillas.

---

## 3. Archivo `docker-compose.yml`

Asegúrate de tener este archivo en la raíz del proyecto:

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow -d airflow"]
      interval: 5s
      timeout: 5s
      retries: 10

  airflow:
    image: apache/airflow:2.8.1
    depends_on:
      postgres:
        condition: service_healthy
    env_file:
      - .env
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__WEBSERVER__RBAC: "True"
      _PIP_ADDITIONAL_REQUIREMENTS: "pandas numpy matplotlib seaborn"
    command: ["bash","-lc","airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com || true && airflow webserver -D && exec airflow scheduler"]
    volumes:
      - ./dags:/opt/airflow/dags
      - ./etl:/opt/airflow/etl
      - ./data:/opt/airflow/data
    ports:
      - "8080:8080"

volumes:
  pgdata:
```

---

## 4. Levantar los servicios

Ejecuta:

```bash
docker compose up -d
```

Luego abre Airflow en tu navegador:

```
http://localhost:8080
```

**Credenciales:**
- Usuario: `admin`
- Contraseña: `admin`

### En la UI:

1. Activa el DAG **`workshop_etl`**
2. Haz clic en **Trigger DAG**

---

## 5. Resultados esperados

```
data/output/
├─ fact_transactions.csv
├─ agg_daily.csv
├─ warehouse.sqlite
└─ eda_output/
   ├─ nulls_fact.csv
   ├─ amount_vs_amount_usd.png
   ├─ monedas_distribution.png
   ├─ regiones_lideres_total_usd.png
   ├─ cohorte_clientes_mes.png
   ├─ clientes_por_region.png
   ├─ ticket_por_categoria.csv
   ├─ ticket_promedio_categoria.png
   ├─ combos_raros_country_category.csv
   ├─ combos_raros_country_category.png
   ├─ outliers_amount_usd.png
   ├─ evolucion_total_usd_diario.png
   ├─ picos_caidas_semana.png
   └─ summary.json
```

---
