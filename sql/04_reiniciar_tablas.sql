-- ============================================================================
-- 04 · Reiniciar SOLO las tablas (no toca catálogo, esquemas ni volúmenes: los lotes siguen en landing)
-- Úsalo para volver al estado "antes de la carga inicial". Se pierde el historial de Delta de estas tablas.
-- Sustituye 'logistica_dataco' si usas otro catálogo.
-- NO uses DROP SCHEMA ... CASCADE para esto: borraría también los volúmenes con tus ficheros.
-- ============================================================================

DROP TABLE IF EXISTS logistica_dataco.bronze.envios_raw;

DROP TABLE IF EXISTS logistica_dataco.silver.envios_limpios;
DROP TABLE IF EXISTS logistica_dataco.silver.envios_rechazados;
DROP TABLE IF EXISTS logistica_dataco.silver.calidad_metricas;
DROP TABLE IF EXISTS logistica_dataco.silver.log_cargas;
DROP TABLE IF EXISTS logistica_dataco.silver.stg_validos;

DROP TABLE IF EXISTS logistica_dataco.gold.dim_cliente;
DROP TABLE IF EXISTS logistica_dataco.gold.dim_producto;
DROP TABLE IF EXISTS logistica_dataco.gold.dim_ubicacion;
DROP TABLE IF EXISTS logistica_dataco.gold.fact_envios;
DROP TABLE IF EXISTS logistica_dataco.gold.mart_entregas_modo_region;
DROP TABLE IF EXISTS logistica_dataco.gold.mart_ventas_mensuales;
DROP TABLE IF EXISTS logistica_dataco.gold.mart_retraso_categoria_segmento;
