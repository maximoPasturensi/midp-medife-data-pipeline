-- =============================================================================
-- MIDP — Medifé Intelligent Data Pipeline
-- Script: bigquery_star_schema.sql
-- Target: Google BigQuery — Dataset medife_dw
-- Modelo: Star Schema optimizado para QlikSense BI
-- =============================================================================

-- Crear dataset con configuración de región y expiración de tablas temporales
-- (ejecutar una sola vez desde Cloud Shell o Terraform)
-- CREATE SCHEMA IF NOT EXISTS `medife-project.medife_dw`
--   OPTIONS (location = 'southamerica-east1', default_table_expiration_ms = NULL);


-- =============================================================================
-- DIMENSIÓN: dim_prestador
-- SCD Tipo 2 — Historia completa de prestadores
-- =============================================================================
CREATE TABLE IF NOT EXISTS `medife-project.medife_dw.dim_prestador` (
    -- Surrogate key (BQ genera con GENERATE_UUID() en la carga)
    prestador_sk       STRING    NOT NULL,

    -- Natural key
    prestador_id       STRING    NOT NULL,

    -- Atributos descriptivos
    nombre             STRING,
    especialidad       STRING,
    region             STRING,
    cuit               STRING,
    cuit_valido        BOOL,
    habilitado         BOOL      NOT NULL,

    -- SCD Tipo 2
    fecha_inicio       DATE      NOT NULL,
    fecha_fin          DATE,
    es_vigente         BOOL      NOT NULL  DEFAULT TRUE,

    -- Auditoría
    _source            STRING,
    _ingestion_ts      TIMESTAMP
)
PARTITION BY fecha_inicio
CLUSTER BY region, especialidad
OPTIONS (
    description = 'Dimensión de prestadores médicos — SCD Tipo 2. Particionada por fecha de alta.',
    labels = [('env', 'prod'), ('team', 'data-engineering'), ('project', 'midp')]
);


-- =============================================================================
-- DIMENSIÓN: dim_medicamento
-- Nomenclador Nacional de Medicamentos (ANMAT/SNS)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `medife-project.medife_dw.dim_medicamento` (
    medicamento_sk     STRING    NOT NULL,

    -- Natural key — código oficial SNS
    codigo_sns         STRING    NOT NULL,

    -- Atributos
    nombre             STRING,
    laboratorio        STRING,
    unidad_medida      STRING,
    precio_ref         NUMERIC,   -- NUMERIC para exactitud financiera
    vigente            BOOL,
    fecha_vigencia     DATE,

    -- Auditoría
    _source            STRING,
    _ingestion_ts      TIMESTAMP
)
PARTITION BY fecha_vigencia
CLUSTER BY laboratorio, codigo_sns
OPTIONS (
    description = 'Dimensión del Nomenclador Nacional de Medicamentos. Precio de referencia ANMAT.',
    labels = [('env', 'prod'), ('team', 'data-engineering'), ('project', 'midp')]
);


-- =============================================================================
-- DIMENSIÓN: dim_tiempo
-- Calendario completo — generado una sola vez
-- =============================================================================
CREATE TABLE IF NOT EXISTS `medife-project.medife_dw.dim_tiempo` (
    periodo_id         STRING    NOT NULL,   -- YYYYMM
    anio               INT64     NOT NULL,
    mes                INT64     NOT NULL,
    trimestre          INT64     NOT NULL,
    semestre           INT64     NOT NULL,
    nombre_mes         STRING,
    es_fin_de_anio     BOOL,

    -- Flags Medifé específicos
    es_periodo_alta    BOOL      COMMENT 'Pico estacional de demanda',
    es_periodo_cierre  BOOL      COMMENT 'Cierre presupuestario trimestral'
)
OPTIONS (
    description = 'Dimensión calendario. Enriquecida con estacionalidad del sector salud.'
);

-- Populate dim_tiempo para 2020-2026
INSERT INTO `medife-project.medife_dw.dim_tiempo`
SELECT
    FORMAT_DATE('%Y%m', d)                      AS periodo_id,
    EXTRACT(YEAR  FROM d)                       AS anio,
    EXTRACT(MONTH FROM d)                       AS mes,
    CAST(CEIL(EXTRACT(MONTH FROM d) / 3.0) AS INT64) AS trimestre,
    CAST(CEIL(EXTRACT(MONTH FROM d) / 6.0) AS INT64) AS semestre,
    FORMAT_DATE('%B', d)                        AS nombre_mes,
    EXTRACT(MONTH FROM d) = 12                  AS es_fin_de_anio,
    EXTRACT(MONTH FROM d) IN (1, 6, 7)          AS es_periodo_alta,
    EXTRACT(MONTH FROM d) IN (3, 6, 9, 12)      AS es_periodo_cierre
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2026-12-01', INTERVAL 1 MONTH)) AS d;


-- =============================================================================
-- FACT TABLE: fact_costos_prestacionales
-- Tabla central — granularidad: prestador x medicamento x período
-- =============================================================================
CREATE TABLE IF NOT EXISTS `medife-project.medife_dw.fact_costos_prestacionales` (

    -- Surrogate key de la fact
    fact_id            STRING    NOT NULL,

    -- Foreign keys al Star Schema
    prestador_sk       STRING    NOT NULL,
    medicamento_sk     STRING    NOT NULL,
    periodo_id         STRING    NOT NULL,   -- FK a dim_tiempo

    -- Natural keys (para debugging y reconciliación)
    prestador_id       STRING    NOT NULL,
    codigo_sns         STRING    NOT NULL,
    periodo            STRING    NOT NULL,

    -- Medidas financieras (NUMERIC: precisión exacta, crítico para auditoría)
    costo_real         NUMERIC   NOT NULL,
    costo_ppto         NUMERIC   NOT NULL,
    cantidad           INT64,

    -- Métricas derivadas — calculadas en PySpark
    desvio_pct         FLOAT64,    -- (real - ppto) / ppto
    desvio_abs_ars     NUMERIC,    -- en pesos ARS
    costo_real_acum_prestador NUMERIC,  -- acumulado del prestador en el período

    -- Clasificación de negocio
    alerta_flag        BOOL,
    severidad          STRING,     -- NORMAL / MODERADO / ALTO / CRITICO
    tendencia          STRING,     -- NUEVO / ESTABLE / MEJORA / EMPEORA
    rank_desvio_periodo INT64,     -- posición en el ranking del período

    -- Metadatos de auditoría (trazabilidad end-to-end)
    run_id             STRING,
    _calc_ts           TIMESTAMP,
    _pipeline_version  STRING
)
PARTITION BY DATE(PARSE_TIMESTAMP('%Y%m', periodo))
CLUSTER BY prestador_id, severidad, alerta_flag
OPTIONS (
    description = 'Fact table de costos prestacionales. Granularidad: prestador x medicamento x período mensual. Incluye métricas de desvío vs presupuesto para BI Medifé / QlikSense.',
    labels = [('env', 'prod'), ('team', 'data-engineering'), ('project', 'midp'), ('sensitivity', 'high')]
);


-- =============================================================================
-- VISTAS analíticas para QlikSense
-- =============================================================================

-- Vista: Resumen ejecutivo de desvíos por período
CREATE OR REPLACE VIEW `medife-project.medife_dw.v_resumen_desvios_periodo` AS
SELECT
    f.periodo_id,
    t.nombre_mes,
    t.anio,
    t.trimestre,
    COUNT(DISTINCT f.prestador_id)                           AS total_prestadores,
    COUNT(*)                                                  AS total_transacciones,
    SUM(f.costo_real)                                         AS costo_real_total,
    SUM(f.costo_ppto)                                         AS costo_ppto_total,
    SUM(f.desvio_abs_ars)                                     AS desvio_total_ars,
    SAFE_DIVIDE(SUM(f.desvio_abs_ars), SUM(f.costo_ppto))   AS desvio_pct_consolidado,
    COUNTIF(f.alerta_flag = TRUE)                             AS total_alertas,
    COUNTIF(f.severidad = 'CRITICO')                          AS alertas_criticas,
    COUNTIF(f.tendencia = 'EMPEORA')                          AS tendencias_negativas
FROM `medife-project.medife_dw.fact_costos_prestacionales` f
LEFT JOIN `medife-project.medife_dw.dim_tiempo` t USING (periodo_id)
GROUP BY 1, 2, 3, 4;


-- Vista: Top 20 prestadores con mayor desvío (para dashboard QlikSense)
CREATE OR REPLACE VIEW `medife-project.medife_dw.v_top_prestadores_desvio` AS
WITH ranked AS (
    SELECT
        f.prestador_id,
        p.nombre                                              AS nombre_prestador,
        p.region,
        p.especialidad,
        f.periodo_id,
        SUM(f.costo_real)                                     AS costo_real,
        SUM(f.costo_ppto)                                     AS costo_ppto,
        SAFE_DIVIDE(
            SUM(f.desvio_abs_ars), SUM(f.costo_ppto)
        )                                                     AS desvio_pct_prestador,
        COUNTIF(f.severidad = 'CRITICO')                      AS eventos_criticos,
        MAX(f.tendencia)                                      AS tendencia_predominante,
        ROW_NUMBER() OVER (
            PARTITION BY f.periodo_id
            ORDER BY ABS(SAFE_DIVIDE(SUM(f.desvio_abs_ars), SUM(f.costo_ppto))) DESC
        )                                                     AS rank_periodo
    FROM `medife-project.medife_dw.fact_costos_prestacionales` f
    LEFT JOIN `medife-project.medife_dw.dim_prestador` p
           ON f.prestador_id = p.prestador_id AND p.es_vigente = TRUE
    GROUP BY 1, 2, 3, 4, 5
)
SELECT * FROM ranked WHERE rank_periodo <= 20;


-- =============================================================================
-- Validación del modelo — queries de smoke test post-carga
-- =============================================================================

-- Test 1: Integridad referencial fact → dim_prestador
SELECT
    'integridad_prestador' AS test,
    COUNT(*) AS huerfanos
FROM `medife-project.medife_dw.fact_costos_prestacionales` f
LEFT JOIN `medife-project.medife_dw.dim_prestador` p USING (prestador_id)
WHERE p.prestador_id IS NULL;

-- Test 2: Sin filas con desvío NULL en alertas activas
SELECT
    'alertas_sin_desvio' AS test,
    COUNTIF(desvio_pct IS NULL AND alerta_flag = TRUE) AS inconsistencias
FROM `medife-project.medife_dw.fact_costos_prestacionales`;

-- Test 3: Chequeo de rangos de desvío
SELECT
    severidad,
    COUNT(*) AS cantidad,
    MIN(desvio_pct) AS desvio_min,
    MAX(desvio_pct) AS desvio_max,
    AVG(desvio_pct) AS desvio_avg
FROM `medife-project.medife_dw.fact_costos_prestacionales`
GROUP BY severidad
ORDER BY desvio_avg DESC;
