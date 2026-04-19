# MIDP — Medifé Intelligent Data Pipeline

> Pipeline de datos end-to-end para el procesamiento, validación y análisis de costos prestacionales en el sector de salud privada.  
> Stack: **PySpark · BigQuery · GCP · FastAPI · GitHub Actions**

[![CI/CD Pipeline](https://github.com/tu-usuario/midp/actions/workflows/midp_pipeline.yml/badge.svg)](https://github.com/tu-usuario/midp/actions)
[![Code Quality](https://img.shields.io/badge/code%20style-ruff%20%2B%20black-000000)](https://github.com/tu-usuario/midp)
[![Coverage](https://img.shields.io/badge/coverage-≥80%25-brightgreen)](https://github.com/tu-usuario/midp)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue)](https://www.python.org)
[![PySpark 3.5](https://img.shields.io/badge/pyspark-3.5-orange)](https://spark.apache.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Contexto de negocio

Las prepagas de salud en Argentina operan bajo un entorno de alta variabilidad tarifaria: costos de medicamentos indexados al Nomenclador Nacional (ANMAT/SNS), prestadores con estructura de precios heterogénea y presupuestos proyectados que deben reconciliarse mensualmente para mantener la sostenibilidad financiera del plan médico.

MIDP resuelve el problema de **visibilidad tardía sobre desvíos presupuestarios**: en el modelo sin este pipeline, los equipos de control de gestión detectaban desvíos >15% recién al cierre del período fiscal. Con MIDP, el desvío se calcula en tiempo cuasi-real (T+1 día hábil) y se expone en dashboards QlikSense con alertas de severidad clasificadas.

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│ INGESTA                                                         │
│  Nomenclador Nacional (Selenium/API) ──┐                        │
│  Listado de Prestadores (FastAPI mock) ├──► GCS Landing (raw/)  │
│  Presupuesto Proyectado (CSV/PG seed)  ┘     Parquet particionado│
└────────────────────────────────────────────────────────────────-┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│ PROCESAMIENTO — PySpark (Dataproc Serverless)                   │
│  1. Limpieza: dedup, nulos, normalización de strings            │
│  2. Data Quality: null rate <2%, schema validation, rangos SNS  │
│  3. Métrica core: desvío_pct = (real - ppto) / ppto             │
│     Window functions: LAG (tendencia), RANK (ranking período)   │
│     Clasificación: NORMAL / MODERADO / ALTO / CRÍTICO           │
└─────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│ DATA WAREHOUSE — BigQuery (Star Schema)                         │
│  fact_costos_prestacionales  ◄── dim_prestador (SCD Tipo 2)     │
│                              ◄── dim_medicamento                 │
│                              ◄── dim_tiempo                     │
│  Particionado por período · Clustered por prestador/severidad   │
└─────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│ CONSUMO                                                         │
│  QlikSense BI · FastAPI /metrics · Alertas Slack (umbral ≥15%)  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Estructura del repositorio

```
midp/
├── src/
│   └── midp/
│       ├── __init__.py
│       ├── transform.py          # Core PySpark — limpieza, DQ, métricas
│       ├── ingest/
│       │   ├── scraper_nomenclador.py
│       │   ├── api_prestadores.py
│       │   └── mock_data.py
│       └── api/
│           └── main.py           # FastAPI — endpoint /metrics
├── tests/
│   ├── unit/
│   │   ├── test_clean.py
│   │   ├── test_data_quality.py
│   │   └── test_desvio.py
│   └── integration/
│       └── test_pipeline_e2e.py
├── sql/
│   └── bigquery_star_schema.sql  # DDL idempotente — Star Schema
├── .github/
│   └── workflows/
│       └── midp_pipeline.yml     # CI/CD — lint, tests, DQ, deploy
├── requirements.txt
├── requirements-dev.txt
├── docker-compose.yml            # Dev local: Spark + PostgreSQL
└── README.md
```

---

## Características técnicas destacadas

### Escalabilidad

El pipeline está diseñado para escalar horizontalmente sin modificaciones de código:

- **Dataproc Serverless** gestiona la asignación dinámica de ejecutores Spark según la carga del día (pico de procesamiento en cierre trimestral vs. día ordinario).
- **Adaptive Query Execution (AQE)** activo en SparkSession: el plan de ejecución se reoptimiza en runtime según la distribución real de los datos, eliminando el tuning manual de `spark.sql.shuffle.partitions`.
- Las tablas BigQuery están **particionadas por período mensual y clustered** por `prestador_id + severidad`, lo que reduce el costo de queries analíticas en QlikSense en ~60–80% respecto a un esquema plano (eliminación de particiones + pruning de clusters).
- El modelo Star Schema es **aditivo**: agregar nuevas fuentes de datos (ej. prestaciones de internación, cobertura de crónicos) implica crear nuevas dimensiones sin alterar la fact table central.

### Gobierno de datos

El proyecto implementa los tres pilares del Data Governance dentro de un pipeline operativo:

**Linaje de datos (Data Lineage)**  
Cada fila de la `fact_costos_prestacionales` lleva las columnas `run_id`, `_calc_ts` y `_pipeline_version`, lo que permite reconstruir exactamente qué versión del código generó qué resultado. Esto es especialmente crítico para auditorías regulatorias del sector salud (Superintendencia de Servicios de Salud).

**Catalogación**  
Todas las tablas BigQuery incluyen `OPTIONS(description=...)` y labels estructurados (`env`, `team`, `project`, `sensitivity`). Compatible con Data Catalog de GCP para linaje automático.

**Control de acceso**  
La autenticación con GCP usa **Workload Identity Federation** en lugar de service account keys estáticos, eliminando el riesgo de credenciales comprometidas en el repositorio. El KMS Key de GCP cifra los datos en reposo en Dataproc.

**SCD Tipo 2 en dim_prestador**  
Los prestadores habilitados/inhabilitados se versionan con `fecha_inicio / fecha_fin / es_vigente`, preservando el historial completo para análisis retroactivos. Un prestador dado de baja en marzo sigue siendo consultable en las queries de enero–febrero sin contaminar la vista actual.

### Integridad de la información

La integridad se garantiza en tres capas independientes:

**Capa 1 — Schema enforcement**  
Todos los DataFrames PySpark se crean con schemas tipados explícitos (`StructType`). Un campo `precio_ref` que llegue como string en lugar de `DoubleType` falla el job antes de procesar un solo registro, evitando la propagación silenciosa de errores de tipo.

**Capa 2 — Data Quality checks**  
La clase `DataQualityReport` evalúa, por DataFrame:
- Tasa de nulos en columnas críticas (`null_rate ≤ 2%`)
- Unicidad de claves naturales (`uniqueness ≥ 99%`)
- Rangos de precio válidos (evita ceros incorrectos que distorsionan el KPI de desvío)

Si cualquier check crítico falla, el pipeline **detiene la ejecución y notifica** antes de escribir datos inválidos al Data Warehouse. El principio es: es preferible no tener datos que tener datos incorrectos en BI.

**Capa 3 — Smoke tests post-deploy**  
Tras cada escritura a BigQuery en producción, el script `smoke_test_bigquery.py` ejecuta queries de integridad referencial (fact → dims) y verifica que no existan alertas activas con `desvio_pct = NULL`.

---

## Métrica de negocio: desvío prestacional

```python
desvio_pct = (costo_real - costo_ppto) / costo_ppto

# Clasificación automática
severidad = CASE
  WHEN |desvio_pct| >= 0.30 THEN 'CRITICO'
  WHEN |desvio_pct| >= 0.15 THEN 'ALTO'      ← umbral de alerta en BI
  WHEN |desvio_pct| >= 0.05 THEN 'MODERADO'
  ELSE 'NORMAL'
```

Las **Window Functions** de PySpark agregan dos dimensiones analíticas:
- `rank_desvio_periodo`: posición del prestador en el ranking de desvíos del mes (alimenta el Top-20 de QlikSense).
- `tendencia` (LAG): detecta si el desvío de un prestador está mejorando o empeorando período a período, antes de que cruce el umbral de alerta.

---

## Setup local

```bash
# 1. Clonar e instalar dependencias
git clone https://github.com/tu-usuario/midp.git
cd midp
pip install -r requirements.txt -r requirements-dev.txt

# 2. Levantar infraestructura local con Docker
docker-compose up -d   # PostgreSQL + Spark standalone

# 3. Ejecutar pipeline con datos sintéticos (sin GCP)
python -m midp.transform --use-mock

# 4. Correr tests
pytest tests/unit/ -v --cov=src/midp

# 5. API de métricas local
uvicorn src.midp.api.main:app --reload
# → http://localhost:8000/docs
```

---

## CI/CD — GitHub Actions

El workflow `.github/workflows/midp_pipeline.yml` implementa un pipeline de entrega en 5 etapas:

| Etapa | Herramienta | Criterio de paso |
|-------|-------------|-----------------|
| Lint & type check | Ruff · Black · MyPy · Bandit | 0 errores |
| Unit tests | pytest + pyspark local | Cobertura ≥ 80% |
| Data Quality | DataQualityReport (mock) | 0 checks FAIL |
| Deploy Staging | Dataproc Serverless | Job exitoso |
| Deploy Producción | Dataproc + BigQuery DDL | Smoke tests OK + aprobación manual |

Cada push a `develop` dispara hasta Staging. Los merges a `main` requieren aprobación en GitHub Environments antes de desplegar a Producción.

---

## Stack tecnológico

| Área | Tecnología | Justificación |
|------|-----------|---------------|
| Procesamiento | PySpark 3.5 | Escalabilidad horizontal, Window functions nativas |
| Cloud compute | Dataproc Serverless | Sin gestión de clúster, costo por uso |
| Storage | GCS (Parquet) + BigQuery | Separación landing/warehouse, costos óptimos |
| Modelado BI | Star Schema (BigQuery) | Compatible con QlikSense, queries OLAP eficientes |
| Orquestación | GitHub Actions | CI/CD nativo, trazabilidad por commit |
| API | FastAPI | Async, autodocs OpenAPI, tipado con Pydantic |
| Calidad de código | Ruff · Black · MyPy | Estándares de equipo Senior |
| Seguridad | Workload Identity Fed. + KMS | Sin service account keys estáticos |

---

## Autor

**Maximo Pasturensi** — Estudiante de Analista Programador · UAI  
Experiencia en operaciones de salud, ETL pipelines y análisis de desvíos presupuestarios.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5)](https://www.linkedin.com/in/maximo-pasturensi-806820333/)
[![GitHub](https://img.shields.io/badge/GitHub-Portfolio-181717)](https://github.com/maximoPasturensi)

---

> *Este proyecto fue desarrollado como portafolio técnico end-to-end, demostrando capacidades de Data Engineering aplicadas al dominio de salud privada en Argentina.*
