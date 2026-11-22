"""Transformaciones PySpark reutilizables (sin dependencia de Delta).

Todo lo que se puede probar en local sin Delta vive aquí: lectura, contrato,
tipado, reglas de calidad, deduplicación y preparación del SCD2.
Las sentencias MERGE (que sí requieren Delta) están en los notebooks.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.config import COLUMNAS_CONTRATO
from src.esquema import a_snake_case, exigir_contrato

# --------------------------------------------------------------------------
# Lectura y Bronze
# --------------------------------------------------------------------------


def leer_csv_lote(spark, ruta: str) -> DataFrame:
    """Lee los CSV de un lote como texto (sin inferir tipos), codificación latin-1."""
    return (
        spark.read.option("header", True)
        .option("encoding", "ISO-8859-1")
        .option("mode", "PERMISSIVE")
        .csv(ruta)
    )


def preparar_bronze(df: DataFrame, lote_id: str):
    """Aplica el contrato: valida columnas, renombra a snake_case y descarta sensibles.

    Devuelve (dataframe_bronze, informe_contrato). Lanza ErrorContrato si falta
    alguna columna obligatoria (el job debe fallar aquí, antes de escribir nada).
    """
    informe = exigir_contrato(df.columns)
    seleccion = [
        F.col(f"`{c}`").alias(a_snake_case(c))
        for c in df.columns
        if a_snake_case(c) in COLUMNAS_CONTRATO
    ]
    out = df.select(*seleccion, F.col("_metadata.file_path").alias("archivo_origen"))
    out = out.select(*COLUMNAS_CONTRATO, "archivo_origen")  # orden estable
    out = out.withColumn("lote_id", F.lit(lote_id)).withColumn(
        "fecha_ingesta", F.current_timestamp()
    )
    return out, informe


# --------------------------------------------------------------------------
# Tipado y reglas de calidad
# --------------------------------------------------------------------------

_FECHA_HORA = (
    "coalesce(try_to_timestamp({c}, 'M/d/yyyy H:mm'),"
    " try_to_timestamp({c}, 'yyyy-MM-dd HH:mm:ss'),"
    " try_to_timestamp({c}, 'yyyy-MM-dd'))"
)

# columna (contrato) -> expresión SQL de conversión segura (nunca lanza error)
TIPADO = {
    "order_date_dateorders": _FECHA_HORA,
    "shipping_date_dateorders": _FECHA_HORA,
    "fecha_extraccion": "try_cast(trim({c}) AS date)",
    "late_delivery_risk": "try_cast(trim({c}) AS int)",
    "days_for_shipping_real": "try_cast(trim({c}) AS int)",
    "days_for_shipment_scheduled": "try_cast(trim({c}) AS int)",
    "order_item_quantity": "try_cast(trim({c}) AS int)",
    "product_price": "try_cast(trim({c}) AS double)",
    "order_item_discount": "try_cast(trim({c}) AS double)",
    "sales": "try_cast(trim({c}) AS double)",
    "order_item_total": "try_cast(trim({c}) AS double)",
    "order_profit_per_order": "try_cast(trim({c}) AS double)",
}
TEXTO_INITCAP = ["customer_city", "customer_state", "customer_country",
                 "order_city", "order_state", "order_country", "order_region"]
TEXTO_UPPER = ["order_status"]
RENOMBRAR_SILVER = {
    "order_date_dateorders": "fecha_pedido",
    "shipping_date_dateorders": "fecha_envio",
}

# Reglas: nombre -> condición que debe CUMPLIRSE (sobre columnas tipadas t__*)
REGLAS = {
    "R01_clave_nula": "t__order_id IS NOT NULL AND t__order_item_id IS NOT NULL",
    "R02_cliente_nulo": "t__customer_id IS NOT NULL",
    "R03_fecha_pedido_invalida": "t__order_date_dateorders IS NOT NULL",
    "R04_importe_negativo_o_invalido": "t__sales >= 0 AND t__order_item_total >= 0",
    "R05_cantidad_invalida": "t__order_item_quantity > 0",
    "R06_envio_anterior_a_pedido": "t__shipping_date_dateorders >= t__order_date_dateorders",
}


def _texto_limpio(c: str) -> str:
    """trim + vacío -> NULL."""
    return f"nullif(trim({c}), '')"


def separar_validos_rechazados(bronze_lote: DataFrame):
    """Tipa, aplica las reglas de calidad y separa válidos de rechazados.

    Devuelve (validos, rechazados, filas_por_regla, filas_entrada)
      - validos: columnas tipadas y con nombres de Silver (incluye duplicados)
      - rechazados: fila original (texto) + motivo_rechazo
      - filas_por_regla: dict regla -> nº de filas que la incumplen
      - filas_entrada: nº de filas del lote en Bronze
    Una fila puede incumplir varias reglas (se cuenta en cada una).
    """
    cols = COLUMNAS_CONTRATO
    t = bronze_lote
    for c in cols:
        expr = TIPADO.get(c, _texto_limpio("{c}")).format(c=f"`{c}`")
        t = t.withColumn(f"t__{c}", F.expr(expr))

    # Resultado de cada regla (NULL cuenta como incumplida)
    for nombre, cond in REGLAS.items():
        t = t.withColumn(f"ok__{nombre}", F.coalesce(F.expr(cond), F.lit(False)))
    motivos = F.filter(
        F.array(*[F.when(~F.col(f"ok__{n}"), F.lit(n)) for n in REGLAS]),
        lambda x: x.isNotNull(),
    )
    t = t.withColumn("motivos", motivos)  # sin cache(): serverless no lo soporta

    filas_entrada = t.count()
    agg = t.agg(*[F.sum(F.when(~F.col(f"ok__{n}"), 1).otherwise(0)).alias(n) for n in REGLAS]).first()
    filas_por_regla = {n: int(agg[n] or 0) for n in REGLAS}

    # Rechazados: fila original (texto) + motivo
    originales = [F.col(f"`{c}`") for c in cols] + [
        F.col("archivo_origen"), F.col("lote_id")]
    rechazados = (
        t.filter(F.size("motivos") > 0)
        .select(*originales, F.concat_ws(",", "motivos").alias("motivo_rechazo"))
        .withColumn("fecha_proceso", F.current_timestamp())
    )

    # Válidos: columnas tipadas, normalizadas y con nombres de Silver
    sel = []
    for c in cols:
        col = F.col(f"t__{c}")
        if c in TEXTO_INITCAP:
            col = F.initcap(col)
        if c in TEXTO_UPPER:
            col = F.upper(col)
        sel.append(col.alias(RENOMBRAR_SILVER.get(c, c)))
    validos = t.filter(F.size("motivos") == 0).select(
        *sel, F.col("lote_id"), F.col("archivo_origen"))
    return validos, rechazados, filas_por_regla, filas_entrada


def deduplicar(validos: DataFrame) -> DataFrame:
    """Una fila por (order_id, order_item_id): la de extracción más reciente.

    Si dos filas empatan en fecha de extracción se desempata con una huella del contenido
    para que el resultado sea siempre el mismo (determinista y reproducible).
    """
    huella = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in validos.columns]), 256)
    w = Window.partitionBy("order_id", "order_item_id").orderBy(
        F.col("fecha_extraccion").desc(), F.col("fecha_pedido").desc(), huella.desc())
    return validos.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# --------------------------------------------------------------------------
# Modelo dimensional
# --------------------------------------------------------------------------


def _hash_columnas(cols):
    return F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]),
        256,
    )


# Columnas que NO entran en el hash de fila de Silver/Fact: metadatos de carga y atributos de
# cliente (el histórico de cliente vive en gold.dim_cliente, no en cada fila de hechos).
EXCLUIDAS_HASH_FILA = {
    "lote_id", "archivo_origen", "fecha_extraccion", "hash_fila",
    "customer_segment", "customer_city", "customer_state", "customer_country",
}


def anadir_hash_fila(df: DataFrame) -> DataFrame:
    """Añade hash_fila: huella del contenido de negocio de la fila.

    Permite que el MERGE solo actualice cuando algo cambió de verdad (un reenvío idéntico
    no genera ninguna escritura).
    """
    cols = [c for c in df.columns if c not in EXCLUIDAS_HASH_FILA]
    return df.withColumn("hash_fila", _hash_columnas(cols))


def con_hash_atributos(df: DataFrame, atributos) -> DataFrame:
    return df.withColumn("hash_atributos", _hash_columnas(atributos))


def ultimo_por_clave(df: DataFrame, claves) -> DataFrame:
    """Última fila (por fecha_extraccion) para cada clave de negocio."""
    claves = [claves] if isinstance(claves, str) else list(claves)
    w = Window.partitionBy(*claves).orderBy(F.col("fecha_extraccion").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def id_ubicacion():
    """Clave determinista de ubicación (hash de sus atributos)."""
    return F.substring(
        _hash_columnas(["market", "order_region", "order_country", "order_state", "order_city"]),
        1, 16)


def preparar_staging_scd2(entrante: DataFrame, vigentes: DataFrame, clave: str, atributos):
    """Prepara el 'staging' del patrón SCD2 con MERGE de Delta.

    - entrante: filas del lote (se reduce a la última por clave)
    - vigentes: versiones actuales de la dimensión (es_vigente = true),
                con columnas <clave> y hash_atributos
    Devuelve filas con columna merge_key:
      * merge_key = NULL   -> versión nueva a INSERTAR (cliente nuevo o con cambios)
      * merge_key = <clave> -> versión vigente a CERRAR (solo si cambió)
    Si el lote no trae cambios, el resultado está vacío (idempotente).
    """
    ent = ultimo_por_clave(entrante, clave).withColumn(
        "hash_atributos", _hash_columnas(atributos))
    v = vigentes.select(clave, F.col("hash_atributos").alias("hash_vigente"))
    j = ent.join(v, clave, "left")
    cambiados = j.filter(
        F.col("hash_vigente").isNull() | (F.col("hash_vigente") != F.col("hash_atributos")))
    nuevas = cambiados.withColumn("merge_key", F.lit(None).cast("string"))
    cierres = cambiados.filter(F.col("hash_vigente").isNotNull()).withColumn(
        "merge_key", F.col(clave).cast("string"))
    return nuevas.unionByName(cierres).drop("hash_vigente")
