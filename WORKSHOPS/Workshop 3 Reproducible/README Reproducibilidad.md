1) Requisitos
Docker y Docker Compose instalados
Puertos libres: 8080 (Airflow), 5432 (Postgrest)

2) .env 
Crea un archivo .env en la raiz con:
FERNET_KEY="Llave generada"
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
_PIP_ADDITIONAL_REQUIREMENTS=pandas numpy matplotlib seaborn pyarrow fastparquet

luego usa los siguientes comandos en la terminal:
python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY


Esto te generara algo parecido a esto: LXLNdvx0aVk63E8_c2igw9DkH4Tf6xg4bXWClb2ezjI=

Y lo copias en el espacio del archivo .env que dice FERNET_KEY= " "
Nota: sin las comillas

3) docker-compose.yml
crea un archivo docker-compose con las siguiente configuracion:

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

4) Levantar los servicios
usa el comando en la terminal:

docker compose up -d


Abre Airflow: http://localhost:8080
Usuario: admin · Contraseña: admin

En la UI:

Activa el DAG workshop_etl.
Haz Trigger DAG

5) Resultados esperados:

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
