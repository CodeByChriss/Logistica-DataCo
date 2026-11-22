"""Comprobaciones SQL de la tarea final del job. Cada consulta debe devolver 0 (= nada incorrecto)."""
from src import config as C


def comprobaciones(cat: str, lote: str) -> dict:
    t = lambda nombre: f"{cat}.{nombre}"
    return {
        "dim_cliente: un único registro vigente por cliente": f"""
            SELECT count(*) FROM (SELECT customer_id FROM {t(C.T_DIM_CLIENTE)}
            GROUP BY customer_id HAVING sum(CASE WHEN es_vigente THEN 1 ELSE 0 END) <> 1)""",
        "dim_cliente: versiones sin solapes ni huecos": f"""
            SELECT count(*) FROM (SELECT fecha_fin, lead(fecha_inicio) OVER (PARTITION BY customer_id ORDER BY fecha_inicio) AS siguiente
            FROM {t(C.T_DIM_CLIENTE)}) WHERE siguiente IS NOT NULL AND siguiente <> fecha_fin""",
        "silver: sin claves de negocio duplicadas": f"""
            SELECT count(*) FROM (SELECT order_id, order_item_id FROM {t(C.T_SILVER)}
            GROUP BY order_id, order_item_id HAVING count(*) > 1)""",
        "fact: sin claves de negocio duplicadas": f"""
            SELECT count(*) FROM (SELECT order_id, order_item_id FROM {t(C.T_FACT)}
            GROUP BY order_id, order_item_id HAVING count(*) > 1)""",
        "fact y silver tienen las mismas filas": f"""
            SELECT abs((SELECT count(*) FROM {t(C.T_FACT)}) - (SELECT count(*) FROM {t(C.T_SILVER)}))""",
        "fact: sin cliente huérfano": f"""
            SELECT count(*) FROM {t(C.T_FACT)} f LEFT ANTI JOIN {t(C.T_DIM_CLIENTE)} d ON f.customer_sk = d.customer_sk""",
        "fact: sin producto huérfano": f"""
            SELECT count(*) FROM {t(C.T_FACT)} f LEFT ANTI JOIN {t(C.T_DIM_PRODUCTO)} d ON f.product_card_id = d.product_card_id""",
        "fact: sin ubicación huérfana": f"""
            SELECT count(*) FROM {t(C.T_FACT)} f LEFT ANTI JOIN {t(C.T_DIM_UBICACION)} d ON f.ubicacion_id = d.ubicacion_id""",
        f"calidad ({lote}): entrada = válidas + rechazadas": f"""
            SELECT coalesce(abs(max(CASE WHEN metrica='filas_entrada' THEN valor END)
                     - max(CASE WHEN metrica='filas_validas' THEN valor END)
                     - max(CASE WHEN metrica='filas_rechazadas' THEN valor END)), -1)
            FROM {t(C.T_METRICAS)} WHERE lote_id = '{lote}'""",
        "marts: los tres tienen datos": f"""
            SELECT CAST(3 - (SELECT sign(count(*)) FROM {t(C.T_MART_ENTREGAS)})
                     - (SELECT sign(count(*)) FROM {t(C.T_MART_VENTAS)})
                     - (SELECT sign(count(*)) FROM {t(C.T_MART_RETRASO)}) AS BIGINT)""",
    }


def ejecutar(spark, cat: str, lote: str):
    """Devuelve [(nombre, filas_incorrectas, 'OK'|'FALLO')]."""
    out = []
    for nombre, sql in comprobaciones(cat, lote).items():
        malas = spark.sql(sql).first()[0]
        malas = -1 if malas is None else int(malas)
        out.append((nombre, malas, "OK" if malas == 0 else "FALLO"))
    return out
