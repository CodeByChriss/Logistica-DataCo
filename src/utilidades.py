"""Utilidades que usan Spark/Delta (se ejecutan en Databricks, no en los tests locales)."""
from pyspark.sql import functions as F


def tabla_existe(spark, tabla: str) -> bool:
    return spark.catalog.tableExists(tabla)


def escribir_lote(spark, df, tabla: str, lote_id: str, modo_esquema: str = "append"):
    """Escribe un lote de forma idempotente: si el lote ya estaba, se sustituye."""
    if tabla_existe(spark, tabla):
        spark.sql(f"DELETE FROM {tabla} WHERE lote_id = '{lote_id}'")
    df.write.mode("append").option("mergeSchema", "false").saveAsTable(tabla)


def crear_tabla_vacia_si_no_existe(spark, tabla: str, df):
    """Crea una tabla Delta con el esquema de df si todavía no existe."""
    if not tabla_existe(spark, tabla):
        df.limit(0).write.saveAsTable(tabla)


def ultima_version(spark, tabla: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").select("version").first()[0]


def merge_con_registro(spark, sql: str, catalogo: str, tabla_corta: str, lote_id: str, log_tabla: str):
    """Ejecuta un MERGE y guarda en la tabla de control cuántas filas insertó/actualizó.

    Las métricas salen del historial de Delta (operationMetrics). Si el MERGE no cambió nada
    puede no generar versión nueva: en ese caso se registran ceros.
    """
    tabla = f"{catalogo}.{tabla_corta}"
    v0 = ultima_version(spark, tabla)
    spark.sql(sql)
    h = spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").first()
    if h["version"] == v0:
        fila = (lote_id, tabla_corta, int(v0), "SIN_CAMBIOS", 0, 0, 0)
    else:
        m = h["operationMetrics"] or {}
        fila = (lote_id, tabla_corta, int(h["version"]), h["operation"],
                int(m.get("numTargetRowsInserted", 0)),
                int(m.get("numTargetRowsUpdated", 0)),
                int(m.get("numTargetRowsDeleted", 0)))
    df = spark.createDataFrame(
        [fila],
        "lote_id string, tabla string, version_delta long, operacion string, "
        "filas_insertadas long, filas_actualizadas long, filas_borradas long",
    ).withColumn("fecha_proceso", F.current_timestamp())
    log = f"{catalogo}.{log_tabla}"
    if tabla_existe(spark, log):
        spark.sql(f"DELETE FROM {log} WHERE lote_id = '{lote_id}' AND tabla = '{tabla_corta}'")
    df.write.mode("append").saveAsTable(log)
    return fila
