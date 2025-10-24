
# Proyecto Final — Pipeline ETL Orquestado con Airflow (Kafka opcional)

> **Fecha de entrega:** Domingo 9 de noviembre a las 23:59
> **Objetivo:** Entregar un **pipeline ETL completo** orquestado en **Airflow**.  
> **Kafka es opcional** (recomendado) como fuente/ingesta de eventos en tiempo (casi) real.  
> **Importante:** el tema de **pruebas unitarias NO forma parte** de este proyecto final.

---

## 1) Entregables

1. **Repositorio Git** (público o privado con acceso al docente) con:
   - Código fuente del ETL (scripts, módulos).
   - **DAGs de Airflow** (carpeta `dags/`).
   - **Docker Compose** funcional para levantar el entorno.
   - **README** del equipo con pasos de ejecución reproducibles y decisiones de diseño.
   - Datos de ejemplo mínimos (si el dataset original no es público).

2. **Presentación de 20 minutos** (ver Punto 10):
   - Arquitectura, decisiones y trade‑offs.
   - Demo en vivo o grabada.
   - Lecciones aprendidas.
   - **Hallazgos**

3. **(Opcional) Kafka** integrado (ver Punto 8).

> **No incluir** material de pruebas unitarias: no forma parte de la evaluación aquí.

---

## 2) Alcance mínimo (sin Kafka)

- **Extract**: ingestión desde al menos **dos** fuentes heterogéneas, por ejemplo:
  - Archivo CSV/JSON/Parquet + tabla relacional (SQLite/Postgres) **o**
  - Dos archivos con distintos esquemas y calidades.
- **Transform**: limpiezas/normalizaciones + reglas de negocio **claras**:
  - _Ejemplos_: parseo y estandarización de fechas/monedas, de‑duplicación, joins enriquecedores, imputaciones simples, agregaciones diarias.
- **Load**: Validacion con **Great Expectations** antes de la escritura de resultados en un DW (MySQL/SQLite/Postgres/Parquet).
- **Orquestación con Airflow**:
  - 1+ **DAGs** con tareas bien separadas: `extract` → `transform` → `load` → `report` (si aplica).
  - `schedule_interval` definido y **catchup** configurado acorde.
  - **Idempotencia** y re‑ejecución segura.
  - _Logging_ claro y artefactos de salida reproducibles.

---

## 3) Alcance recomendado (con Kafka, opcional)

Si deciden integrarlo:
- Ingesta **streaming** simple (ej. eventos de “ventas” o “web”) a un tópico.
- Un **consumer** controlado por Airflow **o** una tarea batch que consuma “ventanas” (micro‑batch) y las procese como parte del DAG.
- Persistencia final homogénea con el resto del pipeline (mismo DW/área de reportes).

> El uso de Kafka suma puntos en **diseño/arquitectura** y **dificultad técnica**, pero no es obligatorio.

---

## 4) Requisitos técnicos

- **Airflow 2.8+** (se recomienda contenedores).
- Base de datos para metadatos de Airflow: **Postgres** (recomendado) o **SQLite** (solo con `SequentialExecutor`).  
- Python 3.10+ en tareas.  
- **Docker** y **Docker Compose**.

> Apple Silicon (M‑series): preferir imágenes multi‑arch (Airflow y Kafka soportan aarch64 oficialmente en las últimas versiones).

---

## 5) Estructura sugerida del repositorio

```
proyecto-final-etl/
├─ dags/
│  ├─ main_etl.py              # DAG principal (batch)
│  └─ streaming_etl.py         # (opcional) DAG con ingesta/consumo de eventos
├─ etl/
│  ├─ extract.py               # Sin I/O “pesado” en funciones puras (recomendado)
│  ├─ transform.py
│  ├─ load.py
│  └─ utils.py
├─ data/
│  ├─ input/                   # Inputs mínimos de ejemplo (si aplica)
│  └─ output/                  # Artefactos de salida (ej.: tablas o archivos)
├─ docker/
│  ├─ airflow/                 # Config opcional (envs, scripts)
│  └─ kafka/                   # (opcional) scripts utilitarios
├─ docker-compose.yml
├─ .env.example                # Variables de entorno (copiar a .env)
└─ README.md                   # Guía del equipo
```

---

## 6) Docker Compose de referencia (Airflow + Postgres)

> Pueden adaptarlo. Este ejemplo usa **Postgres** para metadatos y mounting de `./dags` en el contenedor.

```yaml
version: "3.9"
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports: ["5432:5432"]
    volumes:
      - pgdata:/var/lib/postgresql/data

  airflow:
    image: apache/airflow:2.8.1
    depends_on: [postgres]
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW__CORE__FERNET_KEY}
      AIRFLOW__WEBSERVER__RBAC: "True"
    command: >
      bash -lc "
      airflow db migrate &&
      airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com &&
      airflow webserver &
      airflow scheduler
      "
    volumes:
      - ./dags:/opt/airflow/dags
      - ./etl:/opt/airflow/etl
      - ./data:/opt/airflow/data
    ports:
      - "8080:8080"

volumes:
  pgdata:
```

**Pasos previos (solo 1ª vez):**
1. Generar **FERNET_KEY** válido (32 bytes base64 url‑safe):  
   ```bash
   python - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
   ```
   Colocar el valor en `.env`:  
   `AIRFLOW__CORE__FERNET_KEY=...`

2. Levantar:
   ```bash
   docker compose up -d
   ```
3. UI de Airflow: <http://localhost:8080> (usuario `admin` / `admin`).

> **Alternativa simple (sin Postgres):** usar `SequentialExecutor` y SQLite cambiando la variable `AIRFLOW__CORE__EXECUTOR` y la conexión `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////home/airflow/airflow.db`. No recomendado para concurrencia.

---

## 7) Lineamientos para el/los DAG(s)

- **Separación clara** de tareas: `extract` → `transform` → `load` (y opcional `report`).  
- Tareas implementadas como **PythonOperator** o similares, importando funciones desde `etl/`.  
- **Configuración**:
  - `start_date` coherente, `schedule_interval` definido (ej.: diario).
  - `catchup` según el caso (activar si se requiere historizar; desactivar para “solo en adelante”).
  - `retries` y `retry_delay` razonables.
- **Idempotencia**: re‑ejecutar el DAG no debe duplicar ni corromper datos.
- **Artefactos**: dejar resultados visibles en `data/output/` o una tabla persistente.
- **Logging**: mensajes claros por paso y conteos (registros leídos/escritos, descartados, etc.).
- **Variables/Conexiones**: si necesitan credenciales, usen **Airflow Connections** o `.env` + `Environment Variables` (sin subir secretos al repo).

---

## 8) Integración con Kafka (opcional y recomendado)

**Objetivo**: demostrar ingesta de eventos y su integración al flujo batch.

- **Infra (compose)**: agregar servicios `zookeeper` y `kafka` (por ejemplo `confluentinc/cp-zookeeper` y `confluentinc/cp-kafka`), asegurando **coherencia** entre `listeners` y `advertised.listeners` (usar solo `PLAINTEXT://kafka:9092` en ambos para evitar errores).
- **Tópico**: `events` (o el que definan).
- **Productor simple** (script Python) que emita eventos con campos consistentes con su modelo.
- **Consumo** dentro del DAG:
  - En una tarea, consumir **una ventana** de mensajes (p. ej. durante X segundos) y volcar a `data/staging/`.
  - Transformar y cargar al mismo almacén destino del batch.
- **Métricas**: incluir en logs un pequeño resumen (eventos procesados, descartados, etc.).

> Mantener el **alcance acotado**: no se requiere latencia sub‑segundo ni exactamente‑una‑vez. La prioridad es **integrarlo** y **explicarlo**.

---

## 9) Ejecución y verificación (lo que vere yo)

1. `docker compose up -d` levanta servicios sin errores.  
2. Airflow accesible en `http://localhost:8080`.  
3. Al **ejecutar el DAG** (manual o por horario):
   - Se generan/actualizan artefactos en `data/output/` **o** tablas en la base indicada.
   - Log de tareas muestra:
     - filas leídas, transformadas, descartadas (si aplica).
     - tiempo de ejecución por tarea.
4. **(Opcional Kafka)**: se puede disparar el productor y mostrar la tarea de consumo integrándose al pipeline.

> **Reproducibilidad:** el README del equipo debe permitir que cualquier revisor levante el proyecto desde cero.

---

## 10) Presentación (20 minutos)

**Formato sugerido (recomendado):**
- **Introducción (2 min)**: problema de negocio / contexto de datos.
- **Arquitectura (5 min)**: diagrama del pipeline (fuentes, tareas, almacén, scheduler).
- **Decisiones clave (4 min)**: formatos, particiones, idempotencia, trade‑offs.
- **Demo (6–7 min)**: levantar servicios, correr DAG, ver salidas (y si aplica, Kafka).
- **Cierre (2 min)**: limitaciones y trabajo futuro.
- **Q&A (tiempo dentro de los 20 min)**.

> Preparar una **demo grabada** como respaldo (por si hay problemas locales).

---

## 11) Criterios de evaluación (rubrica)

| Criterio | Peso |
|---|---:|
| **Orquestación en Airflow** (DAGs claros, configuración correcta, logs informativos) | 30% |
| **Calidad del ETL** (**hallazgos**,transformaciones, idempotencia, separación extract/transform/load) | 30% |
| **Reproducibilidad** (Docker Compose funcional, README claro) | 20% |
| **Presentación** (arquitectura, demo, narrativa, gestión del tiempo) | 15% |
| **Kafka opcional** (integración sensata y explicada) | +5% bonus |

> **Nota**: errores de ejecución que impidan correr el demo descuentan puntos de varios criterios.

---

## 12) Recomendaciones finales

- Congelar versiones de contenedores/imágenes.
- Validar el proyecto en **otra máquina** o desde cero (limpiar volúmenes).
- Evitar cargar datos confidenciales; si usan datos reales, **anonimicen**.
- Verificar puertos en uso (8080, 5432, 9092 si usan Kafka).

---

## 13) Checklist de entrega rápida

- [ ] `docker-compose.yml` levanta sin errores.
- [ ] Airflow accesible y usuarios creados.
- [ ] DAG(s) activos y ejecutables.
- [ ] Datos de ejemplo y artefactos de salida documentados.
- [ ] README del equipo con pasos exactos.
- [ ] (Opcional) Kafka integrado y demostrable.

¡Éxitos con la entrega! Cualquier aclaración logística (fechas, modalidad de evaluación, formato de repositorio) me preguntan.
