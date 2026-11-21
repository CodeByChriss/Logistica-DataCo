"""Configuración central del proyecto.

Aquí vive todo lo que NO es lógica: nombres de objetos, contrato de columnas y listas
de columnas sensibles. Así el código de los notebooks no tiene valores "mágicos" repartidos.
"""

CATALOGO_POR_DEFECTO = "logistica_dataco"
VOLUMEN = "landing"
ESQUEMAS = ("bronze", "silver", "gold")

# --- Tablas ----
T_BRONZE = "bronze.envios_raw"
T_SILVER = "silver.envios_limpios"
T_RECHAZOS = "silver.envios_rechazados"
T_METRICAS = "silver.calidad_metricas"
T_DIM_CLIENTE = "gold.dim_cliente"
T_DIM_PRODUCTO = "gold.dim_producto"
T_DIM_UBICACION = "gold.dim_ubicacion"
T_FACT = "gold.fact_envios"

# --- Contrato de entrada ----
# Columnas OBLIGATORIAS (ya en snake_case). Si falta alguna, el lote se rechaza
# completo en la tarea de ingesta (ver src/esquema.py).
COLUMNAS_CONTRATO = [
    "order_id",
    "order_item_id",
    "order_date_dateorders",
    "shipping_date_dateorders",
    "order_status",
    "delivery_status",
    "late_delivery_risk",
    "shipping_mode",
    "days_for_shipping_real",
    "days_for_shipment_scheduled",
    "customer_id",
    "customer_segment",
    "customer_city",
    "customer_state",
    "customer_country",
    "product_card_id",
    "product_name",
    "category_id",
    "category_name",
    "department_name",
    "product_price",
    "order_item_quantity",
    "order_item_discount",
    "sales",
    "order_item_total",
    "order_profit_per_order",
    "order_region",
    "order_country",
    "order_state",
    "order_city",
    "market",
    "fecha_extraccion",  # la añade src/simulacion.py (fecha de la "extracción")
]

# Columnas que pueden venir en el fichero pero NO se llevan a Bronze
# (datos personales o sin valor analítico). Es una decisión de gobierno.
COLUMNAS_SENSIBLES = [
    "customer_email",
    "customer_password",
    "customer_fname",
    "customer_lname",
    "customer_street",
    "product_image",
    "product_description",
]

# --- Atributos de las dimensiones ----
ATRIBUTOS_CLIENTE = [
    "customer_segment",
    "customer_city",
    "customer_state",
    "customer_country",
]
ATRIBUTOS_PRODUCTO = [
    "product_name",
    "category_id",
    "category_name",
    "department_name",
    "product_price",
]
ATRIBUTOS_UBICACION = ["market", "order_region", "order_country", "order_state", "order_city"]

FECHA_FIN_VIGENTE = "9999-12-31"

T_LOG = "silver.log_cargas"
T_STG = "silver.stg_validos"
T_MART_ENTREGAS = "gold.mart_entregas_modo_region"
T_MART_VENTAS = "gold.mart_ventas_mensuales"
T_MART_RETRASO = "gold.mart_retraso_categoria_segmento"

# Columnas de gold.fact_envios (el MERGE hace INSERT * por nombre)
COLUMNAS_FACT = [
    "order_id", "order_item_id", "customer_sk", "product_card_id", "ubicacion_id",
    "fecha_pedido", "fecha_envio", "order_status", "delivery_status", "shipping_mode",
    "days_for_shipping_real", "days_for_shipment_scheduled", "late_delivery_risk", "es_tardio",
    "order_item_quantity", "sales", "order_item_discount", "order_item_total",
    "order_profit_per_order", "hash_fila", "fecha_extraccion", "lote_id",
]
