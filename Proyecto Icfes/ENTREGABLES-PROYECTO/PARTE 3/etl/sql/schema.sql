-- ===============================
-- SCHEMA DIMENSIONAL: DW ICFES
-- ===============================

-- Eliminar tablas si ya existen (para recargar sin errores)
DROP TABLE IF EXISTS fact_icfes CASCADE;
DROP TABLE IF EXISTS dim_municipio CASCADE;
DROP TABLE IF EXISTS dim_departamento CASCADE;
DROP TABLE IF EXISTS dim_colegio CASCADE;
DROP TABLE IF EXISTS dim_contexto_socioeconomico CASCADE;
DROP TABLE IF EXISTS dim_fecha CASCADE;

-- ===============================
-- DIMENSIONES
-- ===============================

-- 1. Dimensión Departamento
CREATE TABLE dim_departamento (
    id_departamento SERIAL PRIMARY KEY,
    estu_depto_reside VARCHAR(100),
    poblacion_depto BIGINT,
    idh_depto FLOAT,
    pobreza_monetaria_depto FLOAT
);

-- 2. Dimensión Municipio
CREATE TABLE dim_municipio (
    id_municipio SERIAL PRIMARY KEY,
    estu_mcpio_reside VARCHAR(150),
    poblacion_mcpio BIGINT
);

-- 3. Dimensión Contexto Socioeconómico
CREATE TABLE dim_contexto_socioeconomico (
    id_contexto SERIAL PRIMARY KEY,
    estu_inse_individual FLOAT,
    estu_nse_individual FLOAT,
    fami_estratovivienda INT
);

-- 4. Dimensión Colegio
CREATE TABLE dim_colegio (
    id_colegio SERIAL PRIMARY KEY,
    cole_area_ubicacion VARCHAR(50),
    cole_bilingue INT,
    cole_calendario VARCHAR(30),
    cole_naturaleza VARCHAR(50),
    cole_depto_ubicacion VARCHAR(100),
    estu_nse_establecimiento FLOAT
);

-- 5. Dimensión Fecha
CREATE TABLE dim_fecha (
    id_fecha SERIAL PRIMARY KEY,
    periodo INT
);

-- ===============================
-- TABLA DE HECHOS
-- ===============================

CREATE TABLE fact_icfes (
    id_fact SERIAL PRIMARY KEY,
    id_municipio INT,
    id_departamento INT,
    id_contexto INT,
    id_colegio INT,
    id_fecha INT,

    -- Métricas principales
    punt_global INT,
    punt_matematicas INT,
    punt_lectura_critica INT,
    punt_sociales_ciudadanas INT,
    punt_c_naturales INT,
    punt_ingles INT,

    percentil_global INT,
    percentil_matematicas INT,
    percentil_lectura_critica INT,
    percentil_sociales_ciudadanas INT,
    percentil_c_naturales INT,
    percentil_ingles INT,

    -- Relaciones
    CONSTRAINT fk_municipio FOREIGN KEY (id_municipio) REFERENCES dim_municipio (id_municipio),
    CONSTRAINT fk_departamento FOREIGN KEY (id_departamento) REFERENCES dim_departamento (id_departamento),
    CONSTRAINT fk_contexto FOREIGN KEY (id_contexto) REFERENCES dim_contexto_socioeconomico (id_contexto),
    CONSTRAINT fk_colegio FOREIGN KEY (id_colegio) REFERENCES dim_colegio (id_colegio),
    CONSTRAINT fk_fecha FOREIGN KEY (id_fecha) REFERENCES dim_fecha (id_fecha)
);
