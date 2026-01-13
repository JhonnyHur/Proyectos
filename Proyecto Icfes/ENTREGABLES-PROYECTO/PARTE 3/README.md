# Proyecto ETL ICFES 2024 — Airflow + PostgreSQL

## Propósito

El propósito de este proyecto es construir un pipeline ETL reproducible, orquestado con Airflow, que integre y transforme los datos del examen ICFES 2024 junto con fuentes externas obtenidas mediante Web Scraping, con el fin de analizar si el contexto socioeconómico influye en el desempeño académico de los estudiantes.


---

## Pasos previos (solo la primera vez)

### 1. Generar una clave FERNET_KEY (solo si no existe)

Ejecuta en tu terminal:

```bash
python - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
```

Luego copia la clave en tu archivo `.env`:

```
AIRFLOW__CORE__FERNET_KEY=<TU_CLAVE_GENERADA>
```

Nota: Si ya tienes una clave configurada, no necesitas generar una nueva.


---

## Levantar los servicios

Ejecuta:

```bash
docker compose up -d
```

Esto iniciará:

- Postgres  
- Airflow (webserver + scheduler)  
- pgAdmin  


---

## Acceder a las interfaces

### Airflow UI

- URL: [http://localhost:8080](http://localhost:8080)  
- Usuario: `admin`  
- Contraseña: `admin`  


### pgAdmin

1. Abre [http://localhost:8081](http://localhost:8081)  

2. Ingresa:  
   - Email: `admin@example.com`  
   - Password: `admin123`  

3. Crea la conexión:  
   - Host: `postgres`  
   - Port: `5432`  
   - Username: `airflow`  
   - Password: `airflow`  
   - Database: `airflow`

4. Guarda y conéctate.  


---

## Bases de datos disponibles

| Base | Función principal |
|------|-------------------|
| airflow | Guarda la configuración y estado de tareas del orquestador. |
| dw | Almacén de datos (Data Warehouse) donde se cargan las tablas limpias y transformadas se genera con load.py. |
| postgres | Base del sistema, sin modificaciones en este proyecto. |


---



## Verificación final

Si todo está correcto, en la interfaz de Airflow deberías ver el DAG `main_etl` con las tareas en orden:

`ping → extract → transform → great_expectations → load → analytics`


---



## Fuentes de Datos

Fuente principal:

data_icfes_2024.csv: base original del ICFES 2024 (10k+ registros).

link fuente principal drive "data_icfes_2024.csv" (PARTE 3/data/input)

https://drive.google.com/drive/folders/1pckl63mjvOB83bQ98Q1BSMhdVCSr_YZZ?usp=sharing


link fuente principal final drive "data_icfes_2024_final.csv" (PARTE 3/data/output)

https://drive.google.com/drive/folders/1pckl63mjvOB83bQ98Q1BSMhdVCSr_YZZ?usp=sharing


