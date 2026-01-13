# etl/extract.py
import time
import random
import re
import ast
from pathlib import Path
from urllib.parse import urljoin
import urllib.robotparser as rp

import requests
from bs4 import BeautifulSoup
import pandas as pd

# =========================
# RUTAS DENTRO DEL CONTENEDOR AIRFLOW
# =========================
BASE_INPUT = Path("/opt/airflow/data/input")
BASE_STAGING = Path("/opt/airflow/data/staging")
BASE_INPUT.mkdir(parents=True, exist_ok=True)
BASE_STAGING.mkdir(parents=True, exist_ok=True)

# =========================
# CONFIG
# =========================
UA = "ETL-Class/1.0 (contact: student@example.com)"
HEADERS = {"User-Agent": UA}

# =========================
# UTILIDADES
# =========================
def _allowed_by_robots(robots_url: str, target_url: str, ua: str = UA) -> bool:
    try:
        parser = rp.RobotFileParser()
        parser.set_url(robots_url)
        parser.read()
        allowed = parser.can_fetch(ua, target_url)
        print(f"[robots] {robots_url} → can_fetch({target_url}) = {allowed}")
        return allowed
    except Exception as e:
        print(f"[robots] No se pudo leer {robots_url}: {e} → Continuamos con prudencia.")
        return True  # si falla lectura de robots, seguimos con prudencia

def fetch_html(url, headers=None, max_retries=5, base_sleep=1.5, verbose=True):
    """
    Igual a tu notebook: reintentos + backoff + variantes de URL.
    (verify=False para evitar issues SSL como en el notebook)
    """
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

                # throttling simple
                if r.status_code in [429, 503] and "Retry-After" in r.headers:
                    try:
                        wait = int(r.headers["Retry-After"])
                    except Exception:
                        wait = base_sleep * attempt
                    if verbose:
                        print(f"[THROTTLE] Esperando {wait}s (Retry-After)")
                    time.sleep(wait)
                    continue

                if r.status_code == 404:
                    break  # no seguir intentando esta variante

            except Exception as e:
                if verbose:
                    print("Error de red:", e)

            sleep = random.uniform(1, 3) * (2 ** (attempt-1))
            time.sleep(sleep)

    return None

# =========================
# TAREA PRINCIPAL (Airflow)
# =========================
def extract():
    print("== EXTRACT: empezando (descarga HTML a input y CSV a staging) ==")

    # ---------- 1) Wikipedia: IDH por departamento ----------
    URL_IDH = "https://es.wikipedia.org/wiki/Anexo:Departamentos_de_Colombia_por_IDH"
    FILE_IDH_HTML = BASE_INPUT / "wikipedia_departamentos_idh.html"

    if not FILE_IDH_HTML.exists():
        html = fetch_html(URL_IDH, headers=HEADERS)
        if not html:
            raise RuntimeError("No se pudo descargar Wikipedia IDH")
        FILE_IDH_HTML.write_text(html, encoding="utf-8")
        print(f"[OK] Guardado: {FILE_IDH_HTML}")
    else:
        print(f"[CACHE] {FILE_IDH_HTML}")

    # Parsear tabla y guardar CSV (staging) como en el notebook
    html = FILE_IDH_HTML.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"class": "wikitable"})

    rows = []
    for tr in table.select("tr"):
        cols = tr.find_all("td")
        if len(cols) != 3:
            continue
        entidad = cols[0].get_text(strip=True)
        idh = cols[1].get_text(strip=True).replace(",", ".")
        poblacion = cols[2].get_text(strip=True).replace("\xa0", "").replace(" ", "")
        rows.append({
            "Entidad": entidad,
            "IDH": float(idh),
            "Población": int(poblacion)
        })

    df_idh_departamento = pd.DataFrame(rows)
    FILE_IDH_STAGING = BASE_STAGING / "idh_departamentos.csv"
    df_idh_departamento.to_csv(FILE_IDH_STAGING, index=False, encoding="utf-8")
    print(f"[OK] STAGING: {FILE_IDH_STAGING} ({len(df_idh_departamento)} filas)")

    # ---------- 2) DANE: pobreza monetaria (página + iframe) ----------
    URL_DANE = "https://www.dane.gov.co/index.php/estadisticas-por-tema/pobreza-y-condiciones-de-vida/pobreza-monetaria"
    FILE_DANE_MAIN = BASE_INPUT / "dane_pobreza_monetaria.html"
    FILE_DANE_IFRAME = BASE_INPUT / "dane_pobreza_monetaria_visualizacion.html"

    if not FILE_DANE_MAIN.exists():
        html_main = fetch_html(URL_DANE, headers=HEADERS)
        if not html_main:
            raise RuntimeError("No se pudo descargar página principal del DANE")
        FILE_DANE_MAIN.write_text(html_main, encoding="utf-8")
        print(f"[OK] Guardado: {FILE_DANE_MAIN}")
    else:
        html_main = FILE_DANE_MAIN.read_text(encoding="utf-8")
        print(f"[CACHE] {FILE_DANE_MAIN}")

    # Extraer iframe y guardarlo
    soup_dane = BeautifulSoup(html_main, "lxml")
    iframe = soup_dane.find("iframe", src=lambda s: s and "gra-PMDepartamental" in s)
    if not iframe or not iframe.get("src"):
        raise RuntimeError("No se encontró el iframe de pobreza monetaria en DANE")

    iframe_url = urljoin("https://www.dane.gov.co", iframe["src"])
    if not FILE_DANE_IFRAME.exists():
        html_iframe = fetch_html(iframe_url, headers=HEADERS)
        if not html_iframe:
            raise RuntimeError("No se pudo descargar el iframe de la visualización del DANE")
        FILE_DANE_IFRAME.write_text(html_iframe, encoding="utf-8")
        print(f"[OK] Guardado: {FILE_DANE_IFRAME}")
    else:
        html_iframe = FILE_DANE_IFRAME.read_text(encoding="utf-8")
        print(f"[CACHE] {FILE_DANE_IFRAME}")

    # Parseo del <script> del iframe → CSV en staging (igual que notebook)
    soup_iframe = BeautifulSoup(html_iframe, "lxml")
    script = next(s.text for s in soup_iframe.find_all("script") if "labels" in s.text)

    departamentos = ast.literal_eval("[" + re.search(r"labels:\s*\[(.*?)\]", script, re.S).group(1) + "]")
    data_2023 = ast.literal_eval("[" + re.search(r"label:\s*'2023'.*?data:\s*\[(.*?)\]", script, re.S).group(1) + "]")
    data_2024 = ast.literal_eval("[" + re.search(r"label:\s*'2024'.*?data:\s*\[(.*?)\]", script, re.S).group(1) + "]")

    df_pobreza = pd.DataFrame({
        "Departamento": departamentos,
        "Pobreza_2023": data_2023,
        "Pobreza_2024": data_2024
    })

    FILE_POBREZA_STAGING = BASE_STAGING / "dane_pobreza_monetaria.csv"
    df_pobreza.to_csv(FILE_POBREZA_STAGING, index=False, encoding="utf-8-sig")
    print(f"[OK] STAGING: {FILE_POBREZA_STAGING} ({len(df_pobreza)} filas)")

    # ---------- 3) Municipios.com.co: listado y población ----------
    URL_MUN = "https://www.municipios.com.co/municipios"
    FILE_MUN_HTML = BASE_INPUT / "municipios_colombia.html"

    if not FILE_MUN_HTML.exists():
        html_mun = fetch_html(URL_MUN, headers=HEADERS)
        if not html_mun:
            raise RuntimeError("No se pudo descargar municipios.com.co")
        FILE_MUN_HTML.write_text(html_mun, encoding="utf-8")
        print(f"[OK] Guardado: {FILE_MUN_HTML}")
    else:
        html_mun = FILE_MUN_HTML.read_text(encoding="utf-8")
        print(f"[CACHE] {FILE_MUN_HTML}")

    # Parsear listado de municipios (igual al notebook)
    soup_mun = BeautifulSoup(html_mun, "lxml")
    departamentos = []
    municipios = []
    urls = []

    for div in soup_mun.find_all("div", class_="departamento_slide"):
        dep = div.find("h3").get_text(strip=True)
        for a in div.select("ul.municipiosDepartamento li a"):
            departamentos.append(dep)
            municipios.append(a.get_text(strip=True))
            urls.append(a["href"])

    df_municipios = pd.DataFrame({
        "Departamento": departamentos,
        "Municipio": municipios,
        "URL": urls
    })

    # Scraping de población por municipio (como en tu notebook)
    resultados = []
    for _, row in df_municipios.iterrows():
        url_muni = row["URL"]
        html_det = fetch_html(url_muni, headers=HEADERS, verbose=False)
        poblacion_num = None
        if html_det:
            soup_det = BeautifulSoup(html_det, "lxml")
            blocks = soup_det.find_all("div", class_="col-6 col-md-4 py-4")
            for div in blocks:
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
    FILE_POBLACION_STAGING = BASE_STAGING / "poblacion_municipios.csv"
    df_poblacion.to_csv(FILE_POBLACION_STAGING, index=False, encoding="utf-8-sig")
    print(f"[OK] STAGING: {FILE_POBLACION_STAGING} ({len(df_poblacion)} filas)")

    print("== EXTRACT completado: 4 HTML en input + 3 CSV en staging ==")
