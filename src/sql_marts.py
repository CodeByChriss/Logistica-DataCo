"""Data marts de gold (se reconstruyen en cada ejecución a partir del hecho y las dimensiones)."""
from src.config import T_MART_ENTREGAS, T_MART_RETRASO, T_MART_VENTAS


def mart_entregas_modo_region(c: str) -> str:
    return f"""
CREATE OR REPLACE TABLE {c}.{T_MART_ENTREGAS}
COMMENT 'Data mart 1: entregas y retrasos por modo de envío y región de destino (excluye cancelados)'
AS
SELECT
  f.shipping_mode,
  u.order_region,
  count(DISTINCT f.order_id)                                   AS pedidos,
  count(*)                                                     AS lineas,
  sum(CASE WHEN f.es_tardio THEN 1 ELSE 0 END)                 AS lineas_tardias,
  round(100 * avg(CASE WHEN f.es_tardio THEN 1.0 ELSE 0.0 END), 2) AS pct_tardias,
  round(avg(f.days_for_shipping_real), 2)                      AS dias_envio_real_medio,
  round(avg(f.days_for_shipment_scheduled), 2)                 AS dias_envio_previsto_medio
FROM {c}.gold.fact_envios f
JOIN {c}.gold.dim_ubicacion u ON u.ubicacion_id = f.ubicacion_id
WHERE f.delivery_status <> 'Shipping canceled'
GROUP BY f.shipping_mode, u.order_region
""".strip()


def mart_ventas_mensuales(c: str) -> str:
    return f"""
CREATE OR REPLACE TABLE {c}.{T_MART_VENTAS}
COMMENT 'Data mart 2: pedidos, unidades, ventas y beneficio por mes de pedido (excluye cancelados)'
AS
SELECT
  date_trunc('MONTH', f.fecha_pedido)                          AS mes,
  count(DISTINCT f.order_id)                                   AS pedidos,
  sum(f.order_item_quantity)                                   AS unidades,
  round(sum(f.order_item_total), 2)                            AS ventas_netas,
  round(sum(f.order_item_discount), 2)                         AS descuentos,
  round(sum(f.order_profit_per_order), 2)                      AS beneficio
FROM {c}.gold.fact_envios f
WHERE f.order_status <> 'CANCELED'
GROUP BY date_trunc('MONTH', f.fecha_pedido)
""".strip()


def mart_retraso_categoria_segmento(c: str) -> str:
    return f"""
CREATE OR REPLACE TABLE {c}.{T_MART_RETRASO}
COMMENT 'Data mart 3: porcentaje de entregas tardías por categoría de producto y segmento de cliente (versión de cliente vigente al cargar el hecho)'
AS
SELECT
  p.category_name,
  cl.customer_segment,
  count(*)                                                     AS lineas,
  sum(CASE WHEN f.es_tardio THEN 1 ELSE 0 END)                 AS lineas_tardias,
  round(100 * avg(CASE WHEN f.es_tardio THEN 1.0 ELSE 0.0 END), 2) AS pct_tardias
FROM {c}.gold.fact_envios f
JOIN {c}.gold.dim_producto p ON p.product_card_id = f.product_card_id
JOIN {c}.gold.dim_cliente  cl ON cl.customer_sk = f.customer_sk
WHERE f.delivery_status <> 'Shipping canceled'
GROUP BY p.category_name, cl.customer_segment
""".strip()


def todos_los_marts(c: str):
    return [mart_entregas_modo_region(c), mart_ventas_mensuales(c), mart_retraso_categoria_segmento(c)]
