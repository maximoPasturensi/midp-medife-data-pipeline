"""
MIDP — Medifé Intelligent Data Pipeline
Módulo: transform.py
Autor: Portfolio Project — UAI Analista Programador
Descripción: Pipeline de transformación PySpark con Data Quality,
             limpieza avanzada y cálculo de desvío prestacional.
"""

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType, BooleanType
)

# ---------------------------------------------------------------------------
# Configuración de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("MIDP.transform")


# ---------------------------------------------------------------------------
# Schemas de validación (Data Contract)
# ---------------------------------------------------------------------------

SCHEMA_NOMENCLADOR = StructType([
    StructField("codigo_sns",     StringType(),    nullable=False),
    StructField("nombre",         StringType(),    nullable=False),
    StructField("laboratorio",    StringType(),    nullable=True),
    StructField("precio_ref",     DoubleType(),    nullable=False),
    StructField("unidad_medida",  StringType(),    nullable=True),
    StructField("vigente",        BooleanType(),   nullable=False),
    StructField("fecha_vigencia", TimestampType(), nullable=True),
])

SCHEMA_PRESTADORES = StructType([
    StructField("prestador_id",   StringType(),    nullable=False),
    StructField("nombre",         StringType(),    nullable=False),
    StructField("especialidad",   StringType(),    nullable=True),
    StructField("region",         StringType(),    nullable=True),
    StructField("habilitado",     BooleanType(),   nullable=False),
    StructField("cuit",           StringType(),    nullable=True),
])

SCHEMA_COSTOS = StructType([
    StructField("prestador_id",   StringType(),    nullable=False),
    StructField("periodo",        StringType(),    nullable=False),   # YYYYMM
    StructField("codigo_sns",     StringType(),    nullable=False),
    StructField("costo_real",     DoubleType(),    nullable=False),
    StructField("costo_ppto",     DoubleType(),    nullable=False),
    StructField("cantidad",       IntegerType(),   nullable=True),
])

# Thresholds de Data Quality
DQ_MAX_NULL_RATE  = 0.02   # 2 % máximo de nulos en columnas críticas
DQ_MIN_PRECIO     = 0.01   # Precio mínimo aceptable (evitar ceros incorrectos)
DQ_DESVIO_ALERTA  = 0.15   # 15 % → flag de alerta en BI


# ---------------------------------------------------------------------------
# 1. Inicialización de SparkSession
# ---------------------------------------------------------------------------

def get_spark(app_name: str = "MIDP-Transform") -> SparkSession:
    """
    Crea o recupera la SparkSession con configuraciones optimizadas
    para workloads de Data Warehouse (BigQuery connector listo).
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")            # AQE activo
        .config("spark.sql.shuffle.partitions", "50")            # tuning para dataset mediano
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession iniciada: %s", app_name)
    return spark


# ---------------------------------------------------------------------------
# 2. Limpieza y normalización — nivel Senior
# ---------------------------------------------------------------------------

def clean_nomenclador(df: DataFrame) -> DataFrame:
    """
    Limpieza del Nomenclador Nacional de Medicamentos.
    Aplica:
    - Deduplicación basada en clave natural (codigo_sns + fecha_vigencia)
    - Normalización de strings (strip, lower, replace de caracteres inválidos)
    - Filtrado de registros con precio fuera de rango
    - Casteo seguro de tipos
    """
    logger.info("Limpiando Nomenclador — registros entrada: %d", df.count())

    df_clean = (
        df
        # Deduplicación: si hay duplicados, conserva el más reciente
        .withColumn(
            "_row_num",
            F.row_number().over(
                Window.partitionBy("codigo_sns")
                      .orderBy(F.desc("fecha_vigencia"))
            )
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")

        # Normalización de strings
        .withColumn("codigo_sns",  F.trim(F.upper(F.col("codigo_sns"))))
        .withColumn("nombre",      F.trim(F.initcap(F.col("nombre"))))
        .withColumn("laboratorio", F.trim(F.initcap(F.col("laboratorio"))))

        # Coalesce de nulos en columnas no críticas
        .withColumn("laboratorio",   F.coalesce(F.col("laboratorio"), F.lit("SIN DATO")))
        .withColumn("unidad_medida", F.coalesce(F.col("unidad_medida"), F.lit("UN")))

        # Filtro de negocio: precio válido
        .filter(F.col("precio_ref") >= DQ_MIN_PRECIO)
        .filter(F.col("vigente") == True)

        # Metadata de auditoría
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source", F.lit("nomenclador_nacional"))
    )

    logger.info("Limpiando Nomenclador — registros salida: %d", df_clean.count())
    return df_clean


def clean_prestadores(df: DataFrame) -> DataFrame:
    """
    Limpieza del Listado de Prestadores.
    Reglas de negocio Medifé:
    - Solo prestadores habilitados pasan a la capa de BI
    - CUIT validado: 11 dígitos sin guiones
    - Región normalizada al catálogo oficial
    """
    logger.info("Limpiando Prestadores — registros entrada: %d", df.count())

    REGIONES_VALIDAS = ["GBA NORTE", "GBA SUR", "GBA OESTE", "CABA",
                        "INTERIOR", "PATAGONIA", "NOA", "NEA", "CUYO"]

    df_clean = (
        df
        .dropDuplicates(["prestador_id"])
        .filter(F.col("habilitado") == True)

        # Normalizar CUIT: eliminar guiones
        .withColumn("cuit", F.regexp_replace(F.col("cuit"), r"[^0-9]", ""))

        # Marcar CUITs inválidos sin dropear (trazabilidad)
        .withColumn(
            "cuit_valido",
            F.length(F.col("cuit")) == 11
        )

        # Normalizar región
        .withColumn("region", F.trim(F.upper(F.col("region"))))
        .withColumn(
            "region",
            F.when(F.col("region").isin(REGIONES_VALIDAS), F.col("region"))
             .otherwise(F.lit("OTRAS"))
        )

        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source", F.lit("listado_prestadores"))
    )

    logger.info("Limpiando Prestadores — registros salida: %d", df_clean.count())
    return df_clean


# ---------------------------------------------------------------------------
# 3. Validación de Data Quality
# ---------------------------------------------------------------------------

class DataQualityReport:
    """
    Genera un reporte de calidad de datos por DataFrame.
    Diseñado para ser exportado a BigQuery como tabla de auditoría.
    """
    def __init__(self, df_name: str):
        self.df_name = df_name
        self.checks: list[dict] = []
        self.passed = True

    def check_null_rate(self, df: DataFrame, col_name: str) -> "DataQualityReport":
        total   = df.count()
        nulls   = df.filter(F.col(col_name).isNull()).count()
        rate    = nulls / total if total > 0 else 0
        status  = "PASS" if rate <= DQ_MAX_NULL_RATE else "FAIL"
        if status == "FAIL":
            self.passed = False
        self.checks.append({
            "df": self.df_name, "check": f"null_rate:{col_name}",
            "value": round(rate, 4), "threshold": DQ_MAX_NULL_RATE, "status": status
        })
        logger.info("DQ [%s] null_rate(%s) = %.4f — %s", self.df_name, col_name, rate, status)
        return self

    def check_uniqueness(self, df: DataFrame, col_name: str) -> "DataQualityReport":
        total   = df.count()
        unique  = df.select(col_name).distinct().count()
        rate    = unique / total if total > 0 else 0
        status  = "PASS" if rate >= 0.99 else "WARN"
        self.checks.append({
            "df": self.df_name, "check": f"uniqueness:{col_name}",
            "value": round(rate, 4), "threshold": 0.99, "status": status
        })
        logger.info("DQ [%s] uniqueness(%s) = %.4f — %s", self.df_name, col_name, rate, status)
        return self

    def check_price_range(self, df: DataFrame, col_name: str,
                          min_val: float, max_val: float) -> "DataQualityReport":
        out_of_range = df.filter(
            (F.col(col_name) < min_val) | (F.col(col_name) > max_val)
        ).count()
        status = "PASS" if out_of_range == 0 else "FAIL"
        if status == "FAIL":
            self.passed = False
        self.checks.append({
            "df": self.df_name, "check": f"price_range:{col_name}",
            "value": out_of_range, "threshold": 0, "status": status
        })
        logger.info("DQ [%s] price_range(%s) fuera de rango: %d — %s",
                    self.df_name, col_name, out_of_range, status)
        return self

    def summary(self) -> dict:
        return {
            "df": self.df_name,
            "run_ts": datetime.utcnow().isoformat(),
            "total_checks": len(self.checks),
            "failed": sum(1 for c in self.checks if c["status"] == "FAIL"),
            "warned": sum(1 for c in self.checks if c["status"] == "WARN"),
            "overall": "PASS" if self.passed else "FAIL",
            "checks": self.checks,
        }


def validate_nomenclador(df: DataFrame) -> DataQualityReport:
    report = DataQualityReport("nomenclador")
    (
        report
        .check_null_rate(df, "codigo_sns")
        .check_null_rate(df, "precio_ref")
        .check_uniqueness(df, "codigo_sns")
        .check_price_range(df, "precio_ref", 0.01, 500_000)
    )
    return report


def validate_prestadores(df: DataFrame) -> DataQualityReport:
    report = DataQualityReport("prestadores")
    (
        report
        .check_null_rate(df, "prestador_id")
        .check_null_rate(df, "nombre")
        .check_uniqueness(df, "prestador_id")
    )
    return report


# ---------------------------------------------------------------------------
# 4. Cálculo de la métrica core: Desvío Prestacional
# ---------------------------------------------------------------------------

def calcular_desvio_prestacional(df_costos: DataFrame) -> DataFrame:
    """
    Calcula el desvío porcentual entre costo real y presupuesto proyectado
    por prestador y período.

    Lógica de negocio:
    - desvio_pct  = (costo_real - costo_ppto) / costo_ppto
    - alerta_flag = True si |desvio_pct| >= DQ_DESVIO_ALERTA (15 %)
    - rank_desvio = ranking de prestadores por desvío absoluto (Window)
    - Tendencia:  comparación vs período anterior (LAG)

    Returns:
        DataFrame enriquecido con métricas de desvío listo para BigQuery.
    """
    logger.info("Calculando desvío prestacional — registros: %d", df_costos.count())

    # Window: ranking de desvío absoluto por período
    w_periodo = Window.partitionBy("periodo").orderBy(F.desc(F.abs(F.col("desvio_pct"))))

    # Window: tendencia vs período anterior (LAG) por prestador + medicamento
    w_prestador_med = (
        Window
        .partitionBy("prestador_id", "codigo_sns")
        .orderBy("periodo")
    )

    df_metricas = (
        df_costos

        # Desvío porcentual — protegido contra división por cero
        .withColumn(
            "desvio_pct",
            F.when(
                F.col("costo_ppto") > 0,
                (F.col("costo_real") - F.col("costo_ppto")) / F.col("costo_ppto")
            ).otherwise(F.lit(None).cast(DoubleType()))
        )

        # Desvío absoluto en pesos (ARS)
        .withColumn("desvio_abs_ars", F.col("costo_real") - F.col("costo_ppto"))

        # Flag de alerta para QlikSense
        .withColumn(
            "alerta_flag",
            F.abs(F.col("desvio_pct")) >= DQ_DESVIO_ALERTA
        )

        # Clasificación de severidad del desvío
        .withColumn(
            "severidad",
            F.when(F.abs(F.col("desvio_pct")) >= 0.30, F.lit("CRITICO"))
             .when(F.abs(F.col("desvio_pct")) >= 0.15, F.lit("ALTO"))
             .when(F.abs(F.col("desvio_pct")) >= 0.05, F.lit("MODERADO"))
             .otherwise(F.lit("NORMAL"))
        )

        # Ranking por período (para Top-N en dashboard)
        .withColumn("rank_desvio_periodo", F.rank().over(w_periodo))

        # Tendencia: desvío período anterior
        .withColumn(
            "desvio_pct_anterior",
            F.lag("desvio_pct", 1).over(w_prestador_med)
        )
        .withColumn(
            "tendencia",
            F.when(F.col("desvio_pct_anterior").isNull(), F.lit("NUEVO"))
             .when(F.col("desvio_pct") > F.col("desvio_pct_anterior"), F.lit("EMPEORA"))
             .when(F.col("desvio_pct") < F.col("desvio_pct_anterior"), F.lit("MEJORA"))
             .otherwise(F.lit("ESTABLE"))
        )

        # Costo promedio ponderado por prestador (para normalización)
        .withColumn(
            "costo_real_acum_prestador",
            F.sum("costo_real").over(
                Window.partitionBy("prestador_id", "periodo")
            )
        )

        # Metadata
        .withColumn("_calc_ts", F.current_timestamp())
        .withColumn("_pipeline_version", F.lit("1.0.0"))
    )

    logger.info("Desvío calculado — alertas activas: %d",
                df_metricas.filter(F.col("alerta_flag") == True).count())
    return df_metricas


# ---------------------------------------------------------------------------
# 5. Generación de datos sintéticos para demo
# ---------------------------------------------------------------------------

def generate_mock_data(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Genera DataFrames sintéticos que simulan las fuentes de datos Medifé.
    Útil para demos, pruebas unitarias y CI/CD sin acceso a producción.
    """
    from pyspark.sql import Row

    # Nomenclador mock
    nomenclador_rows = [
        Row(codigo_sns="M001", nombre="Ibuprofeno 400mg", laboratorio="Bago",
            precio_ref=250.50, unidad_medida="COMP", vigente=True,
            fecha_vigencia=datetime(2024, 1, 1)),
        Row(codigo_sns="M002", nombre="Amoxicilina 500mg", laboratorio="Pfizer",
            precio_ref=890.00, unidad_medida="CAPS", vigente=True,
            fecha_vigencia=datetime(2024, 1, 1)),
        Row(codigo_sns="M003", nombre="Metformina 850mg", laboratorio="Roemmers",
            precio_ref=320.00, unidad_medida="COMP", vigente=True,
            fecha_vigencia=datetime(2024, 1, 1)),
        # Duplicado intencional para probar dedup
        Row(codigo_sns="M001", nombre="Ibuprofeno 400mg", laboratorio="Bago",
            precio_ref=255.00, unidad_medida="COMP", vigente=True,
            fecha_vigencia=datetime(2024, 3, 1)),
    ]

    # Prestadores mock
    prestadores_rows = [
        Row(prestador_id="P001", nombre="Clínica San Martín", especialidad="Clínica médica",
            region="CABA", habilitado=True, cuit="30-71234567-8"),
        Row(prestador_id="P002", nombre="Farmacia del Sol", especialidad="Farmacia",
            region="GBA NORTE", habilitado=True, cuit="20-25432198-6"),
        Row(prestador_id="P003", nombre="Centro Médico Sur", especialidad="Cardiología",
            region="GBA SUR", habilitado=False, cuit="30-65432198-2"),  # inhabilitado
    ]

    # Costos mock con desvíos variados
    costos_rows = [
        Row(prestador_id="P001", periodo="202401", codigo_sns="M001",
            costo_real=125_000.00, costo_ppto=100_000.00, cantidad=500),
        Row(prestador_id="P001", periodo="202402", codigo_sns="M001",
            costo_real=135_000.00, costo_ppto=100_000.00, cantidad=540),  # trend: empeora
        Row(prestador_id="P002", periodo="202401", codigo_sns="M002",
            costo_real=98_000.00, costo_ppto=110_000.00, cantidad=110),   # desvío favorable
        Row(prestador_id="P002", periodo="202401", codigo_sns="M003",
            costo_real=50_000.00, costo_ppto=42_000.00, cantidad=156),
        Row(prestador_id="P001", periodo="202401", codigo_sns="M003",
            costo_real=310_000.00, costo_ppto=260_000.00, cantidad=970),  # crítico
    ]

    df_nomenclador = spark.createDataFrame(nomenclador_rows, schema=SCHEMA_NOMENCLADOR)
    df_prestadores = spark.createDataFrame(prestadores_rows, schema=SCHEMA_PRESTADORES)
    df_costos      = spark.createDataFrame(costos_rows, schema=SCHEMA_COSTOS)

    return df_nomenclador, df_prestadores, df_costos


# ---------------------------------------------------------------------------
# 6. Escritura a BigQuery / Parquet
# ---------------------------------------------------------------------------

def write_to_bigquery(df: DataFrame, table: str, mode: str = "append") -> None:
    """
    Escribe un DataFrame a BigQuery usando el conector oficial de Spark.
    En entorno de dev, escribe Parquet local para validación offline.
    """
    try:
        (
            df.write
            .format("bigquery")
            .option("table", table)
            .option("temporaryGcsBucket", "medife-midp-temp")
            .option("partitionField", "periodo")
            .option("clusteredFields", "prestador_id,codigo_sns")
            .mode(mode)
            .save()
        )
        logger.info("Escritura BQ exitosa — tabla: %s | modo: %s", table, mode)
    except Exception as e:
        # Fallback a Parquet local para entorno de dev/CI
        logger.warning("BigQuery no disponible (%s). Escribiendo Parquet local.", str(e))
        out_path = f"/tmp/midp_output/{table.replace('.', '_')}"
        df.write.mode(mode).parquet(out_path)
        logger.info("Parquet escrito en: %s", out_path)


# ---------------------------------------------------------------------------
# 7. Pipeline principal — orquestación local
# ---------------------------------------------------------------------------

def run_pipeline(
    spark: Optional[SparkSession] = None,
    use_mock: bool = True,
    bq_dataset: str = "medife_dw.prod"
) -> dict:
    """
    Orquesta el pipeline completo MIDP:
    1. Ingesta (mock o real)
    2. Limpieza
    3. Validación DQ
    4. Cálculo de métricas
    5. Escritura a BigQuery

    Returns:
        dict con resumen de ejecución y reportes DQ.
    """
    spark = spark or get_spark()
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    logger.info("=" * 60)
    logger.info("MIDP Pipeline START — run_id: %s", run_id)
    logger.info("=" * 60)

    # ── 1. Ingesta ──────────────────────────────────────────────────
    if use_mock:
        logger.info("Modo: MOCK DATA")
        df_nom_raw, df_pre_raw, df_cos_raw = generate_mock_data(spark)
    else:
        logger.info("Modo: PRODUCCIÓN (GCS)")
        gcs_base = "gs://medife-midp-landing/raw"
        df_nom_raw = spark.read.schema(SCHEMA_NOMENCLADOR).parquet(f"{gcs_base}/nomenclador/")
        df_pre_raw = spark.read.schema(SCHEMA_PRESTADORES).parquet(f"{gcs_base}/prestadores/")
        df_cos_raw = spark.read.schema(SCHEMA_COSTOS).parquet(f"{gcs_base}/costos/")

    # ── 2. Limpieza ─────────────────────────────────────────────────
    df_nom_clean = clean_nomenclador(df_nom_raw)
    df_pre_clean = clean_prestadores(df_pre_raw)

    # ── 3. Data Quality ─────────────────────────────────────────────
    dq_nom = validate_nomenclador(df_nom_clean)
    dq_pre = validate_prestadores(df_pre_clean)

    for report in [dq_nom, dq_pre]:
        summary = report.summary()
        logger.info("DQ Summary [%s]: %s", summary["df"], summary["overall"])
        if summary["overall"] == "FAIL":
            raise RuntimeError(
                f"Data Quality FAIL en {summary['df']} — pipeline detenido. "
                f"Checks fallidos: {summary['failed']}"
            )

    # ── 4. Cálculo de métricas ──────────────────────────────────────
    df_metricas = calcular_desvio_prestacional(df_cos_raw)

    # Join enriquecido para la fact table final
    df_fact = (
        df_metricas
        .join(
            df_pre_clean.select("prestador_id",
                                F.col("nombre").alias("nombre_prestador"),
                                "region", "especialidad"),
            on="prestador_id", how="left"
        )
        .join(
            df_nom_clean.select("codigo_sns",
                                F.col("nombre").alias("nombre_medicamento"),
                                "precio_ref"),
            on="codigo_sns", how="left"
        )
    )

    # ── 5. Escritura ────────────────────────────────────────────────
    write_to_bigquery(df_fact,      f"{bq_dataset}.fact_costos_prestacionales")
    write_to_bigquery(df_pre_clean, f"{bq_dataset}.dim_prestador")
    write_to_bigquery(df_nom_clean, f"{bq_dataset}.dim_medicamento")

    logger.info("=" * 60)
    logger.info("MIDP Pipeline COMPLETADO — run_id: %s", run_id)
    logger.info("=" * 60)

    return {
        "run_id": run_id,
        "status": "SUCCESS",
        "dq_nomenclador": dq_nom.summary(),
        "dq_prestadores": dq_pre.summary(),
        "records_fact": df_fact.count(),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    spark = get_spark()
    result = run_pipeline(spark=spark, use_mock=True)
    print("\n" + "=" * 60)
    print("MIDP RESULT:")
    print(json.dumps(result, indent=2, default=str))
    spark.stop()
