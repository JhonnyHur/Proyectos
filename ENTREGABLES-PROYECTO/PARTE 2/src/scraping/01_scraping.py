#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os, time, random, re, sqlite3
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
import urllib.robotparser as rp
import time, random, requests
import re, ast


# ## **Fuentes**

# In[2]:


# Configuración general
UA = "ETL-Class/1.0 (contact: student@example.com)"
HEADERS = {"User-Agent": UA}

# Directorio de datos según estructura
DATA_DIR = Path("../data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)
print("Carpeta RAW:", DATA_DIR.resolve())


# **Fuente 1 Wikipedia IDH departamentos 2024**

# In[3]:


URL_IDH = "https://es.wikipedia.org/wiki/Anexo:Departamentos_de_Colombia_por_IDH"
FILE_IDH = DATA_DIR / "wikipedia_departamentos_idh.html"

if not FILE_IDH.exists():
    resp = requests.get(URL_IDH, headers=HEADERS)
    resp.raise_for_status()
    FILE_IDH.write_text(resp.text, encoding="utf-8")
    print("Descargado:", FILE_IDH)
else:
    print("Usando CACHE:", FILE_IDH)


# **Fuente 2 DANE pobreza monetaria 2024**

# In[16]:


URL_DANE = "https://www.dane.gov.co/index.php/estadisticas-por-tema/pobreza-y-condiciones-de-vida/pobreza-monetaria"
FILE_DANE = DATA_DIR / "dane_pobreza_monetaria.html"

if not FILE_DANE.exists():
    resp = requests.get(URL_DANE, headers=HEADERS)
    resp.raise_for_status()
    FILE_DANE.write_text(resp.text, encoding="utf-8")
    print("Descargado:", FILE_DANE)
else:
    print("Usando CACHE:", FILE_DANE)


# **Fuente 3 Población municipios (1100 registros)**

# In[3]:


def fetch_html(url, headers=None, max_retries=5, base_sleep=1.5, verbose=True):
    headers = headers or HEADERS
    variants = [url, url.rstrip('/'), url.replace('https://','http://')]

    for v in variants:
        for attempt in range(1, max_retries+1):
            try:
                r = requests.get(v, headers=headers, timeout=20, verify=False)  
                if verbose:
                    print(f"GET {v} -> {r.status_code}")

                if r.ok:
                    return r.text

                if r.status_code in [429, 503] and "Retry-After" in r.headers:
                    wait = int(r.headers["Retry-After"])
                    if verbose: print(f"⏳ Servidor pidió esperar {wait}s")
                    time.sleep(wait)
                    continue

                if r.status_code == 404:
                    break

            except Exception as e:
                if verbose:
                    print("Error de red:", e)

            sleep = random.uniform(1, 3) * (2 ** (attempt-1))
            time.sleep(sleep)

    return None



# **Descarga robusta con rate limiting y backoff**
# 
# Implementamos fetch_html que:
# 
# * Usa un User-Agent propio (ETL-Class/1.0).
# 
# * Respeta robots.txt (se valida aparte).
# 
# * Reintenta en caso de errores temporales (SSL, timeout).
# 
# * Aplica rate limiting y backoff (1–3 s entre intentos, exponencial).
# 
# * Maneja Retry-After si el servidor lo exige.
# 
# * Prueba variantes de URL (/ final, http/https).
# 
# * Devuelve None si no logra obtener el HTML tras varios intentos.
# 

# In[4]:


URL_MUN = "https://www.municipios.com.co/municipios"
FILE_MUN = DATA_DIR / "municipios_colombia.html"

if not FILE_MUN.exists():
    html_text = fetch_html(URL_MUN, headers=HEADERS, max_retries=5, base_sleep=1.5)
    if html_text:
        FILE_MUN.write_text(html_text, encoding="utf-8")
        print("Descargado:", FILE_MUN)
    else:
        print("No se pudo descargar después de varios intentos:", URL_MUN)
else:
    print("Usando CACHE:", FILE_MUN)


# ## **Reglas del sitio: robots.txt y cortesía**

# **Fuente 1 Wikipedia IDH departamentos 2024**

# In[8]:


rp_idh = rp.RobotFileParser()

try:
    rp_idh.set_url("https://es.wikipedia.org/robots.txt")
    rp_idh.read()
    print("robots.txt leído de https://es.wikipedia.org/robots.txt")
    print("¿Permitido acceder a:", URL_IDH, "?",
            rp_idh.can_fetch(UA, URL_IDH))
except Exception as e:
    print("No se pudo leer robots.txt:", e)


# **Fuente 2 DANE pobreza monetaria por departamento 2024**

# In[9]:


rp_dane = rp.RobotFileParser()

try:
    rp_dane.set_url("https://www.dane.gov.co/robots.txt")
    rp_dane.read()
    print("robots.txt leído de https://www.dane.gov.co/robots.txt")
    print("¿Permitido acceder a:", URL_DANE, "?",
            rp_dane.can_fetch(UA, URL_DANE))
except Exception as e:
    print("No se pudo leer robots.txt:", e)


# **Fuente 3 Población Municipios (1100 registros)**

# In[5]:


rp_mun = rp.RobotFileParser()

try:
    rp_mun.set_url("https://www.municipios.com.co/robots.txt")
    rp_mun.read()
    print("robots.txt leído de https://www.municipios.com.co/robots.txt")
    print("¿Permitido acceder a:", URL_MUN, "?",
            rp_mun.can_fetch(UA, URL_MUN))
except Exception as e:
    print("No se pudo leer robots.txt:", e)


# ## **Parseo con BeautifulSoup**

# **Fuente 1 Wikipedia IDH departamentos 2024**

# In[12]:


FILE_IDH = DATA_DIR / "wikipedia_departamentos_idh.html"
html = FILE_IDH.read_text(encoding="utf-8")

soup = BeautifulSoup(html, "lxml")


table = soup.find("table", {"class": "wikitable"})

rows = []
for tr in table.select("tr"):
    cols = tr.find_all("td")
    if len(cols) != 3:
        continue  # saltar filas que no tienen 3 columnas (ej: subtítulos)

    entidad = cols[0].get_text(strip=True)
    idh = cols[1].get_text(strip=True).replace(",", ".")  
    poblacion = cols[2].get_text(strip=True).replace("\xa0", "").replace(" ", "")

    rows.append({
        "Entidad": entidad,
        "IDH": float(idh),
        "Población": int(poblacion)
    })


df_idh_departamento = pd.DataFrame(rows)

print(df_idh_departamento.head(33))
print("Total filas extraídas:", len(df_idh_departamento))


FILE_IDH_STAGING = Path("../data/staging/idh_departamentos.csv")
df_idh_departamento.to_csv(FILE_IDH_STAGING, index=False, encoding="utf-8")
print("Guardado en STAGING:", FILE_IDH_STAGING)


# **Fuente 2 DANE pobreza monetaria 2024**

# In[22]:


# --- Buscar y guardar el iframe correcto ---
soup = BeautifulSoup(FILE_DANE.read_text(encoding="utf-8"), "lxml")

iframe = soup.find("iframe", src=lambda s: s and "gra-PMDepartamental" in s)
if iframe:
    iframe_url = urljoin("https://www.dane.gov.co", iframe["src"])
    print("URL del iframe:", iframe_url)

    FILE_IFRAME = DATA_DIR / "dane_pobreza_monetaria_visualizacion.html"
    if not FILE_IFRAME.exists():
        html_iframe = fetch_html(iframe_url, headers=HEADERS)
        FILE_IFRAME.write_text(html_iframe, encoding="utf-8")
        print("Guardado:", FILE_IFRAME)
else:
    print("No se encontró el iframe esperado.")


# In[23]:


# --- Descargar el iframe que contiene la visualización ---

URL_IFRAME = iframe_url
FILE_IFRAME = DATA_DIR / "dane_pobreza_monetaria_visualizacion.html"

if not FILE_IFRAME.exists():
    html_iframe = fetch_html(URL_IFRAME, headers=HEADERS, max_retries=5, base_sleep=1.5)
    if html_iframe:
        FILE_IFRAME.write_text(html_iframe, encoding="utf-8")
        print("Iframe descargado y guardado en:", FILE_IFRAME)
    else:
        print("No se pudo descargar el iframe después de varios intentos:", URL_IFRAME)
else:
    print("Usando CACHE existente:", FILE_IFRAME)


# In[24]:


# Leer el iframe descargado
html_iframe = FILE_IFRAME.read_text(encoding="utf-8")
soup_iframe = BeautifulSoup(html_iframe, "lxml")

# Buscar el <script> que contiene los datos del gráfico
script = next(s.text for s in soup_iframe.find_all("script") if "labels" in s.text)

# Extraer departamentos (labels) con regex
departamentos = ast.literal_eval("[" + re.search(r"labels:\s*\[(.*?)\]", script, re.S).group(1) + "]")

# Extraer datos 2023
data_2023 = ast.literal_eval("[" + re.search(r"label:\s*'2023'.*?data:\s*\[(.*?)\]", script, re.S).group(1) + "]")

# Extraer datos 2024
data_2024 = ast.literal_eval("[" + re.search(r"label:\s*'2024'.*?data:\s*\[(.*?)\]", script, re.S).group(1) + "]")

print("Departamentos:", len(departamentos))
print("Ejemplo:", departamentos[:5])
print("Datos 2023:", data_2023[:5])
print("Datos 2024:", data_2024[:5])


# In[27]:


df_pobreza_monetaria = pd.DataFrame({
    "Departamento": departamentos,
    "Pobreza_2023": data_2023,
    "Pobreza_2024": data_2024
})

print(df_pobreza_monetaria.head(24))
print("Total filas:", len(df_pobreza_monetaria))


FILE_DF = DATA_DIR.parent / "staging" / "dane_pobreza_monetaria.csv"

df_pobreza_monetaria.to_csv(FILE_DF, index=False, encoding="utf-8-sig")
print("Guardado en:", FILE_DF)


# **Fuente 3 Población Municipios (1100 registros)**

# In[6]:


# Leer HTML desde el archivo descargado
html_mun = FILE_MUN.read_text(encoding="utf-8")
soup = BeautifulSoup(html_mun, "lxml")

departamentos = []
municipios = []
urls = []

# Buscar todos los bloques de departamentos
for div in soup.find_all("div", class_="departamento_slide"):
    # Nombre del departamento
    dep = div.find("h3").get_text(strip=True)

    # Municipios dentro del ul correspondiente
    for a in div.select("ul.municipiosDepartamento li a"):
        departamentos.append(dep)
        municipios.append(a.get_text(strip=True))
        urls.append(a["href"])


df_municipios = pd.DataFrame({
    "Departamento": departamentos,
    "Municipio": municipios,
    "URL": urls
})

print(df_municipios.head(15))
print("Total municipios extraídos:", len(df_municipios))


# In[ ]:


# --- Scraping población para TODOS los municipios ---

resultados = []

for i, row in df_municipios.iterrows():
    url_muni = row["URL"]
    html_muni = fetch_html(url_muni, headers=HEADERS, verbose=False)
    if not html_muni:
        poblacion_num = None
    else:
        soup_muni = BeautifulSoup(html_muni, "lxml")
        poblacion_divs = soup_muni.find_all("div", class_="col-6 col-md-4 py-4")

        poblacion_num = None
        for div in poblacion_divs:
            text = div.get_text(" ", strip=True)
            if "habitantes" in text.lower():
                poblacion_num = text.replace("habitantes", "").strip()
                break

    resultados.append({
        "Departamento": row["Departamento"],
        "Municipio": row["Municipio"],
        "URL": url_muni,
        "Poblacion": poblacion_num
    })


df_poblacion = pd.DataFrame(resultados)

print(df_poblacion.head(10))
print("Total municipios procesados:", len(df_poblacion))


FILE_DF = DATA_DIR.parent / "staging" / "poblacion_municipios.csv"
df_poblacion.to_csv(FILE_DF, index=False, encoding="utf-8-sig")
print("Guardado en:", FILE_DF)


