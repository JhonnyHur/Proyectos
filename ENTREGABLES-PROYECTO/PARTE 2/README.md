# README – ETL Project: ICFES 2024 Data Warehouse
<br><br>

### **Objetivo del Proyecto**

El propósito de este proyecto es construir un pipeline ETL reproducible que integre y transforme datos del examen ICFES 2024 junto con fuentes externas obtenidas mediante Web Scraping, con el fin de analizar si el contexto socioeconómico influye en el desempeño académico de los estudiantes.

Para lograrlo, se implementó un proceso que:

Extrae los datos desde múltiples fuentes (CSV + Web Scraping).

Limpia, valida y consolida la información mediante Great Expectations.

Carga los resultados en un Data Warehouse modelado en esquema estrella.

Permite ejecutar consultas y visualizaciones directamente desde la base de datos.
<br><br>



### **Fuentes de Datos**

**Fuente principal:**

data_icfes_2024.csv: base original del ICFES 2024 (10k+ registros).

link fuente principal drive  "data_icfes_2024.csv"
(PARTE 2/data/raw)

https://drive.google.com/drive/folders/1y1--OmcA7TKS-Ugct5aikFMzOTa8Mp1h?usp=sharing


link fuente principal final drive "data_icfes_2024_final.csv"
(PARTE 2/data/curated)


https://drive.google.com/drive/folders/1y1--OmcA7TKS-Ugct5aikFMzOTa8Mp1h?usp=sharing
<br>



**Fuentes complementarias (Web Scraping):**

dane_pobreza_monetaria.html → Indicadores de pobreza monetaria por departamento.

municipios_colombia.html → Población por municipoos.

wikipedia_departamentos_idh.html → Índice de desarrollo humano (IDH) por departamento.

Estas fuentes enriquecen el dataset original, permitiendo un análisis contextual más completo sobre la relación entre desempeño académico y condiciones socioeconómicas.
<br><br>






### **Configuración del Entorno**

**Crear entorno virtual con uv**


uv python install 3.11
uv venv -p 3.11 .venv
source .venv/bin/activate   # Windows: .\.venv\Scripts\Activate.ps1
<br><br>



### **Base de Datos (Docker + PostgreSQL)**


**Levantar la base de datos**

docker compose up -d


**Configuración pgAdmin**

**General:**

Nombre: project2-postgresqlDB



**Conexión:**

Host / Dirección: postgres-dw

Puerto: 5432

Base de datos: dw_icfes

Usuario: etl25

Contraseña: etl25

¿Salvar contraseña?: Sí
<br><br>


Equipo ETL Group 1 – Universidad Autónoma de Occidente (UAO)







