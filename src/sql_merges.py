"""Sentencias MERGE INTO (Delta Lake) como funciones que devuelven SQL.

Se mantienen aquí, y no incrustadas en los notebooks, para poder revisarlas y
comprobar su sintaxis en los tests. Los nombres de tabla llegan ya cualificados
(catalogo.esquema.tabla).

Idempotencia: ejecutar dos veces el mismo lote no cambia el resultado
(la condición de UPDATE compara hash_fila / hash_atributos).
"""
from src.config import FECHA_FIN_VIGENTE

COLS_ATRIB_CLIENTE = ["customer_segment", "customer_city", "customer_state", "customer_country"]
COLS_ATRIB_PRODUCTO = ["product_name", "category_id", "category_name", "department_name", "product_price"]
COLS_FACT_MUTABLES = [
    "fecha_envio", "order_status", "delivery_status", "shipping_mode",
    "days_for_shipping_real", "days_for_shipment_scheduled", "late_delivery_risk", "es_tardio",
    "order_item_quantity", "sales", "order_item_discount", "order_item_total",
    "order_profit_per_order", "hash_fila", "fecha_extraccion", "lote_id",
]


def merge_silver(tabla: str, vista: str) -> str:
    """Silver: upsert por clave de negocio (order_id, order_item_id)."""
    return f"""
MERGE INTO {tabla} AS t
USING {vista} AS s
  ON t.order_id = s.order_id AND t.order_item_id = s.order_item_id
WHEN MATCHED AND s.hash_fila <> t.hash_fila AND s.fecha_extraccion >= t.fecha_extraccion
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""".strip()


def merge_dim_cliente_scd2(tabla: str, vista_staging: str) -> str:
    """SCD Tipo 2. El staging trae, por cada cambio, DOS filas:
      - merge_key = customer_id -> cierra la versión vigente (WHEN MATCHED)
      - merge_key = NULL        -> inserta la nueva versión (WHEN NOT MATCHED)
    Intervalos semiabiertos: la versión vale en [fecha_inicio, fecha_fin).
    """
    cols = ", ".join(COLS_ATRIB_CLIENTE)
    vals = ", ".join(f"s.{c}" for c in COLS_ATRIB_CLIENTE)
    return f"""
MERGE INTO {tabla} AS t
USING {vista_staging} AS s
  ON t.customer_id = s.merge_key AND t.es_vigente = true
WHEN MATCHED THEN UPDATE SET
  t.es_vigente = false,
  t.fecha_fin = s.fecha_extraccion
WHEN NOT MATCHED THEN INSERT
  (customer_sk, customer_id, {cols}, hash_atributos, fecha_inicio, fecha_fin, es_vigente)
VALUES
  (xxhash64(s.customer_id, CAST(s.fecha_extraccion AS STRING)), s.customer_id, {vals},
   s.hash_atributos, s.fecha_extraccion, DATE'{FECHA_FIN_VIGENTE}', true)
""".strip()


def merge_dim_producto_scd1(tabla: str, vista: str) -> str:
    """SCD Tipo 1: se sobrescribe el atributo, sin historial."""
    sets = ", ".join(f"t.{c} = s.{c}" for c in COLS_ATRIB_PRODUCTO + ["hash_atributos"])
    return f"""
MERGE INTO {tabla} AS t
USING {vista} AS s
  ON t.product_card_id = s.product_card_id
WHEN MATCHED AND t.hash_atributos <> s.hash_atributos THEN UPDATE SET {sets}
WHEN NOT MATCHED THEN INSERT *
""".strip()


def merge_dim_ubicacion(tabla: str, vista: str) -> str:
    """La ubicación se identifica por un hash de sus atributos: solo se insertan las nuevas."""
    return f"""
MERGE INTO {tabla} AS t
USING {vista} AS s
  ON t.ubicacion_id = s.ubicacion_id
WHEN NOT MATCHED THEN INSERT *
""".strip()


def merge_fact(tabla: str, vista: str) -> str:
    """Hechos: upsert por (order_id, order_item_id). customer_sk NO se actualiza:
    el hecho conserva la versión de cliente que estaba vigente cuando se cargó."""
    sets = ", ".join(f"t.{c} = s.{c}" for c in COLS_FACT_MUTABLES)
    return f"""
MERGE INTO {tabla} AS t
USING {vista} AS s
  ON t.order_id = s.order_id AND t.order_item_id = s.order_item_id
WHEN MATCHED AND s.hash_fila <> t.hash_fila AND s.fecha_extraccion >= t.fecha_extraccion
  THEN UPDATE SET {sets}
WHEN NOT MATCHED THEN INSERT *
""".strip()
