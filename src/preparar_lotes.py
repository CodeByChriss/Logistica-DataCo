"""Divide el CSV de DataCo (Kaggle) en lotes para simular cargas incrementales.

El dataset es una foto estática; se ordena por fecha de pedido y se reparte así:

    lote_01     70 %   carga inicial (los pedidos más antiguos)
    lote_02     15 %   segunda carga
    lote_03     10 %   tercera carga
    lote_error   5 %   los pedidos más recientes, SIN la columna `Customer Id` (error provocado)

Cada lote se guarda en <salida>/<lote>/envios.csv con una columna extra, `fecha_extraccion`
(la fecha de pedido más reciente del lote), que el pipeline usa como fecha de la carga.
Los lotes 01-03 son datos del CSV, con una sola modificación a propósito: en lote_02 y lote_03 se cambia el
`Customer Segment` de CLIENTES_CON_CAMBIO clientes que ya estaban en lote_01 (pasan al siguiente segmento).
Así `dim_cliente` (SCD Type 2) cierra una versión y abre otra para esos clientes.
"""
import os

import pandas as pd

COL_FECHA_PEDIDO = "order date (DateOrders)"
CLIENTES_CON_CAMBIO = 20  # clientes cuyo segmento cambia en cada uno de lote_02 y lote_03
SEGMENTOS = ["Consumer", "Corporate", "Home Office"]  # DataCo; el cambio rota al siguiente
LOTES = [("lote_01", 0.70), ("lote_02", 0.15), ("lote_03", 0.10), ("lote_error", 0.05)]


def _siguiente_segmento(segmento: str) -> str:
    if segmento not in SEGMENTOS:
        return segmento
    return SEGMENTOS[(SEGMENTOS.index(segmento) + 1) % len(SEGMENTOS)]


def _cambiar_segmento(parte: pd.DataFrame, clientes_previos: set) -> pd.DataFrame:
    """Cambia el segmento de los primeros CLIENTES_CON_CAMBIO clientes (por id) que ya estaban en un lote anterior."""
    candidatos = sorted(set(parte["Customer Id"]) & clientes_previos, key=lambda c: (len(c), c))
    elegidos = parte["Customer Id"].isin(candidatos[:CLIENTES_CON_CAMBIO])
    parte.loc[elegidos, "Customer Segment"] = parte.loc[elegidos, "Customer Segment"].map(_siguiente_segmento)
    return parte


def preparar(entrada_csv: str, salida: str) -> dict:
    """Devuelve {lote: nº de filas}."""
    if not os.path.exists(entrada_csv):
        raise FileNotFoundError(
            f"No encuentro el CSV en '{entrada_csv}'. Súbelo al volumen 'raw' "
            "(Catalog > bronze > raw > Upload to this volume) o indica la ruta en el parámetro csv_origen.")

    df = pd.read_csv(entrada_csv, encoding="latin-1", dtype=str, keep_default_na=False)
    df["_fecha"] = pd.to_datetime(df[COL_FECHA_PEDIDO], format="%m/%d/%Y %H:%M")
    df = df.sort_values(["_fecha", "Order Id", "Order Item Id"]).reset_index(drop=True)

    resumen, inicio, acumulado = {}, 0, 0.0
    clientes_lote_01 = set()
    for lote, porcentaje in LOTES:
        acumulado += porcentaje
        fin = len(df) if lote == LOTES[-1][0] else round(len(df) * acumulado)
        parte = df.iloc[inicio:fin].copy()
        parte["fecha_extraccion"] = parte["_fecha"].max().strftime("%Y-%m-%d")
        parte = parte.drop(columns="_fecha")
        if lote == "lote_01":
            clientes_lote_01 = set(parte["Customer Id"])
        elif lote in ("lote_02", "lote_03"):
            parte = _cambiar_segmento(parte, clientes_lote_01)  # cambio simulado para el SCD2
        if lote == "lote_error":
            parte = parte.drop(columns="Customer Id")  # error provocado: falta una columna del contrato

        os.makedirs(os.path.join(salida, lote), exist_ok=True)
        parte.to_csv(os.path.join(salida, lote, "envios.csv"), index=False, encoding="latin-1")
        resumen[lote] = len(parte)
        inicio = fin
    return resumen
