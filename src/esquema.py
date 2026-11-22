"""Normalización de nombres y comprobación del contrato de columnas.

Python puro (sin Spark) para poder probarlo en cualquier sitio.
"""
import re
import unicodedata

from src.config import COLUMNAS_CONTRATO, COLUMNAS_SENSIBLES


class ErrorContrato(Exception):
    """El fichero de entrada no cumple el contrato de columnas."""


def a_snake_case(nombre: str) -> str:
    """'Days for shipping (real)' -> 'days_for_shipping_real'."""
    ascii_ = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-zA-Z]+", "_", ascii_).strip("_").lower()


def comprobar_contrato(columnas, requeridas=None):
    """Compara las columnas de entrada con el contrato.

    Devuelve un diccionario con:
      - faltan:     columnas obligatorias que no están (bloquean el lote)
      - extra:      columnas desconocidas (schema drift tolerado: se ignoran)
      - sensibles:  columnas sensibles presentes (se descartan)
      - duplicadas: nombres que colisionan tras normalizar
    """
    requeridas = requeridas or COLUMNAS_CONTRATO
    normalizadas = [a_snake_case(c) for c in columnas]
    vistas, duplicadas = set(), []
    for n in normalizadas:
        if n in vistas and n not in duplicadas:
            duplicadas.append(n)
        vistas.add(n)
    return {
        "faltan": [c for c in requeridas if c not in vistas],
        "extra": [c for c in normalizadas if c not in requeridas and c not in COLUMNAS_SENSIBLES],
        "sensibles": [c for c in normalizadas if c in COLUMNAS_SENSIBLES],
        "duplicadas": duplicadas,
    }


def exigir_contrato(columnas, requeridas=None):
    """Lanza ErrorContrato si falta alguna columna obligatoria o hay colisiones.

    Las columnas extra NO bloquean: se informa y se ignoran (schema drift tolerado).
    """
    r = comprobar_contrato(columnas, requeridas)
    if r["faltan"] or r["duplicadas"]:
        raise ErrorContrato(
            "El lote no cumple el contrato. "
            f"Columnas obligatorias que faltan: {r['faltan']}. "
            f"Nombres duplicados tras normalizar: {r['duplicadas']}."
        )
    return r
