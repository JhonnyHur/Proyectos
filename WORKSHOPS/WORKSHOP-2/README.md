# README – ETL Workshop2

<br><br>



## Informacion Postgress (usuarios y contraseñas:

url interfaz grafica: http://localhost:8080

Login:
Correo: admin@admin.com
Contraseña: admin


## Configuracion Servidos postgress desde interfaz grafica

- Pestaña General
Nombre: workshop2_DW


- Pestaña Conexión
Nombre/Dirección de servidor: postgres-dw
Puerto: 5432
Base de datos de mantenimiento: dw
Nombre de usuario: etluser
Contraseña: etlpass
Marca la opción ¿Salvar contraseña? para no tener que escribirla siempre.





<br><br>

### **Objetivo del Workshop2**

El propósito de este workshop es construir un pipeline ETL reproducible que integre y transforme datos de una compañia de venta de autos usados junto con fuentes externas obtenidas mediante Web Scraping, con el fin de analizar de que pais es la marca del vehiculo y cual es su precio en venta de fabrica de los vehiculos.

Para lograrlo, se implementó un proceso que:

Extrae los datos desde múltiples fuentes (CSV + Web Scraping).

Limpia, valida y consolida la información mediante Great Expectations.

Carga los resultados en un Data Warehouse modelado en esquema estrella.

Permite ejecutar consultas y visualizaciones directamente desde la base de datos.
<br><br>



### **Fuentes de Datos**

**Fuente principal:**

used_cars_base.csv: dataset original de los vehiculos


<br>



**Fuentes complementarias (Web Scraping):**

Paises por marca → Indica de que pais es cada marca. (https://www.motor.mapfre.es/coches/noticias-coches/marcas-coche/)

precios de fabrica → Precios de fabrica de los vehiculos. (https://www.autocosmos.com.co/auto/nuevo?pr=400&cd=5995&pidx=1)


Estas fuentes enriquecen el dataset original, permitiendo un análisis contextual más completo sobre la diferencia de precios entre un carro usado y uno de fabrica.
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

Nombre: workshop2_DW



**Conexión:**

Nombre/Dirección de servidor: postgres-dw
Puerto: 5432
Base de datos de mantenimiento: dw
Nombre de usuario: etluser
Contraseña: etlpass
Marca la opción: Salvar contraseña
<br><br>


Equipo ETL Group 1 – Universidad Autónoma de Occidente (UAO)