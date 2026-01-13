-- Star Schema actualizado para 'used cars listings'

-- ===============================
-- DIMENSIONES
-- ===============================

CREATE TABLE IF NOT EXISTS dim_date (
  date_id        INTEGER PRIMARY KEY,
  full_date      DATE NOT NULL,
  year           INTEGER NOT NULL,
  quarter        INTEGER NOT NULL,
  month          INTEGER NOT NULL,
  day            INTEGER NOT NULL,
  day_name       VARCHAR(20),
  weekofyear     INTEGER
);

CREATE TABLE IF NOT EXISTS dim_brand (
  brand_id       SERIAL PRIMARY KEY,
  brand_name     VARCHAR(100) UNIQUE NOT NULL,
  brand_country  VARCHAR(80)
);

CREATE TABLE IF NOT EXISTS dim_model (
  model_id       SERIAL PRIMARY KEY,
  brand_id       INTEGER NOT NULL REFERENCES dim_brand(brand_id),
  model_name     VARCHAR(120) NOT NULL,
  segment        VARCHAR(60),
  UNIQUE (brand_id, model_name)
);

CREATE TABLE IF NOT EXISTS dim_location (
  location_id    SERIAL PRIMARY KEY,
  city           VARCHAR(120),
  state          VARCHAR(120),
  country        VARCHAR(120) NOT NULL
);

-- ===============================
-- TABLA DE HECHOS
-- ===============================

CREATE TABLE IF NOT EXISTS fact_listings (
  listing_sk          BIGSERIAL PRIMARY KEY,
  listing_natural_id  VARCHAR(64) UNIQUE NOT NULL, -- corresponde a listing_id
  listing_key         VARCHAR(64),                 -- corresponde a key
  model_id            INTEGER REFERENCES dim_model(model_id),
  location_id         INTEGER REFERENCES dim_location(location_id),
  listing_date_id     INTEGER REFERENCES dim_date(date_id),

  seller_type         VARCHAR(30),
  fuel                VARCHAR(30),
  transmission        VARCHAR(30),
  year                INTEGER,
  mileage_km          INTEGER,
  price_usd           NUMERIC(12,2),
  price_clean         NUMERIC(12,2),
  currency            VARCHAR(10)                  -- nueva columna
);