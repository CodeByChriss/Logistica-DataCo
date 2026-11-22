-- ============================================================================
-- 02 · Consultas de verificación y evidencias (ejecútalas en el editor SQL tras cada lote)
-- Sustituye 'logistica_dataco' si usas otro catálogo.
-- ============================================================================

-- 1) Origen vs. Bronze: filas por lote
SELECT lote_id, count(*) AS filas FROM logistica_dataco.bronze.envios_raw GROUP BY lote_id ORDER BY lote_id;

-- 2) Calidad: métricas por lote y regla
SELECT lote_id, metrica, valor FROM logistica_dataco.silver.calidad_metricas ORDER BY lote_id, metrica;

-- 3) Calidad: filas rechazadas y motivo
SELECT lote_id, order_id, order_item_id, customer_id, sales, motivo_rechazo
FROM logistica_dataco.silver.envios_rechazados ORDER BY lote_id, motivo_rechazo;

-- 4) Carga incremental: qué hizo cada MERGE en cada lote (insertadas / actualizadas)
SELECT lote_id, tabla, version_delta, filas_insertadas, filas_actualizadas
FROM logistica_dataco.silver.log_cargas ORDER BY lote_id, tabla;

-- 5) Sin duplicados en Silver y en el hecho (deben devolver 0 filas)
SELECT order_id, order_item_id, count(*) FROM logistica_dataco.silver.envios_limpios GROUP BY 1, 2 HAVING count(*) > 1;
SELECT order_id, order_item_id, count(*) FROM logistica_dataco.gold.fact_envios      GROUP BY 1, 2 HAVING count(*) > 1;

-- 6) SCD2: clientes con más de una versión
SELECT customer_id, count(*) AS versiones
FROM logistica_dataco.gold.dim_cliente GROUP BY customer_id HAVING count(*) > 1 ORDER BY versiones DESC;

-- 7) SCD2: historial completo de un cliente (cambia el id por uno de la consulta anterior)
SELECT customer_id, customer_segment, customer_city, fecha_inicio, fecha_fin, es_vigente
FROM logistica_dataco.gold.dim_cliente WHERE customer_id = '14' ORDER BY fecha_inicio;

-- 8) Historial de versiones de Delta (una fila por escritura)
DESCRIBE HISTORY logistica_dataco.gold.fact_envios;
DESCRIBE HISTORY logistica_dataco.gold.dim_cliente;

-- 9) Antes y después: estado del hecho en una versión anterior vs. la actual
--    (usa los números de versión que veas en DESCRIBE HISTORY)
SELECT order_status, count(*) AS lineas FROM logistica_dataco.gold.fact_envios VERSION AS OF 2 GROUP BY 1 ORDER BY 1;
SELECT order_status, count(*) AS lineas FROM logistica_dataco.gold.fact_envios                                    GROUP BY 1 ORDER BY 1;

-- 10) Data marts
SELECT * FROM logistica_dataco.gold.mart_entregas_modo_region ORDER BY pct_tardias DESC;
SELECT * FROM logistica_dataco.gold.mart_ventas_mensuales ORDER BY mes;
SELECT * FROM logistica_dataco.gold.mart_retraso_categoria_segmento ORDER BY pct_tardias DESC;

-- 11) Linaje: en Catalog Explorer abre gold.fact_envios -> pestaña "Lineage" (no se consulta por SQL).
-- NO ejecutes VACUUM: reduciría el historial recuperable (VERSION AS OF) que se usa en la consulta 9.
