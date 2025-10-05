# PROYECTO-ETL-ANALISIS DATASET ICFES

## Nombres

- Bryan Burbano
- Franco Ortiz
- Didieth Baron
- Jhonny Hurtado

## Links

- **Página DATA ICFES:** [https://www.icfes.gov.co/investigaciones/data-icfes/](https://www.icfes.gov.co/investigaciones/data-icfes/)
- **Base de datos:** Bases DataIcfes - Documentos - Todos los documentos
- **También puedes descargarlos desde [Google Drive](https://drive.google.com/drive/folders/1eHoPka9OG6G_temBh9PbEKdK4k6n5IGG)**

## Descripción del dataset

Este dataset recopila los resultados de los estudiantes que presentaron el Examen Saber 11 en el primer y segundo semestre del año 2024. Cada registro representa a un estudiante e incluye información sobre:

- **Características institucionales:** ubicación urbana/rural, naturaleza del colegio (oficial o no oficial), bilingüismo, carácter (académico/técnico), calendario escolar (A o B).
- **Resultados académicos:** puntajes y percentiles en las áreas de lectura crítica, matemáticas, ciencias naturales, sociales y ciudadanas, inglés y puntaje global.
- **Datos de contexto:** variables socioeconómicas y de entorno relacionadas con los estudiantes y sus colegios.

## Objetivo General

Analizar los resultados de los estudiantes en el Examen Saber 11 – 2024-1 y 2024-2, identificando tendencias en el rendimiento, brechas educativas y factores asociados al desempeño, con el fin de orientar acciones para la mejora de la calidad educativa en Colombia.

## Preguntas a resolver

1. ¿Existen diferencias significativas en el desempeño entre colegios oficiales y no oficiales?
2. ¿Qué tan marcada es la diferencia entre estudiantes de colegios urbanos y rurales?
3. ¿Qué relación hay entre el número de libros en el hogar y el puntaje en lectura crítica?
4. ¿Influye el acceso a internet o computador en los puntajes de matemáticas o ciencias naturales?
5. ¿Cuál es el impacto del estrato socioeconómico en el rendimiento académico?
6. ¿Qué variables explican mejor el percentil global de un estudiante?
7. ¿Influye el bilingüismo (colegio bilingüe vs. no bilingüe) en los resultados de inglés y globales?
8. ¿Qué diferencias se observan entre los departamentos o regiones del país en los puntajes globales?
9. ¿El tamaño del hogar (número de personas) se asocia con variaciones en el rendimiento académico?
10. ¿Tener acceso a recursos como televisión, consola de videojuegos o motocicleta influye positiva o negativamente en el desempeño?
11. ¿Qué tanto predicen los hábitos de estudio (dedicación a internet, horas de lectura, horas de trabajo) el desempeño en áreas específicas?

## Tecnologías a utilizar

- **Python** → Proceso de Análisis Exploratorio de Datos (EDA).
- **MySQL** → Base de datos 1 (antes de la limpieza, transformación y estandarización).
- **PostgreSQL** → Base de datos 2 (para almacenar el dataset resultante).
- **Power BI** → Visualización de los datos.

## Requisitos previos

- Docker Desktop
- Terminal con entorno **bash** _(opcional)_

## Instalación y configuración

**1. Clonar el repositorio**

**2. Descargar los [datos necesarios](https://drive.google.com/drive/folders/1eHoPka9OG6G_temBh9PbEKdK4k6n5IGG) para la ejecución del proyecto**

**3. Copia los datos descargados en la carpeta del proyecto**

- Coloca los dos archivos dentro de: `/data_row`

**4. Levantar los contenedores con Docker**

- Ejecutar en la raíz del proyecto `docker compose up -d`

**5. Instalar dependencias con uv**

- Crear el entorno y descargar las dependencias:`uv sync`
- Activar el entorno: `source .venv/Scripts/activate`
  > **Anotaciones:**
  >
  > - Los comandos utilizados son para un entorno tipo **_bash_**, si está utilizando una terminal con un entorno diferente, deberá adaptarlos a su sistema.
  > - Asegurarse de tener instalado **_uv_** y que el _entorno de desarrollo_ que esté utilizando lo reconozca.

**6. Ejecutar el proceso de carga a MySQL**

- Activar el entorno virtual si no está activo
- Abrir el archivo Jupyter Notebook: `notebooks/load_to_mysql.ipynb`
- Ejecuta todas las celdas en orden

**7. EDA - Exploratory Data Analysis**

- Activar el entorno virtual si no está activo
- Abrir el archivo Jupyter Notebook: `notebooks/EDA.ipynb`
- Ejecuta todas las celdas en orden

**8. Ejecutar el proceso de carga a PostgreSQL**

**Configuración**

- En el navegador, dirijase a **pgadmin** con el siguiente enlace http://localhost:8080/
- Iniciar sesión con las credenciales: _email_: `etl@uao.com` /_password_: `etl25`
- Agregar el servidor PostgreSQL: Clic derecho sobre `Servers` -> `Register` -> `Server`
- En la pestaña **General**:
  - Name: `PostgreSQL_ETL_SERV`
- En la pestaña **Connection**:
  - Host name/address: `postgres-dw`
  - Port: `5432`,
  - Maintenance database: `icfes_transformed`,
  - Username: `transformed`,
  - Password: `etl25`
  - Click en `Save`

**Ejecución**

- Activar el entorno virtual si no está activo
- Abrir el archivo Jupyter Notebook: `notebooks/load_to_postgresql.ipynb`
- Ejecuta todas las celdas en orden
