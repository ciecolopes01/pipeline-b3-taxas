"""
Transformador de dados de taxas referenciais B3.
Separa, enriquece e normaliza os dados por curva.
"""

import logging
import math
from datetime import date, datetime
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Curvas alvo para o pipeline
CURVE_NAMES = {
    "DI x Pré": "PRE",
    "Ajuste Pré": "APR",
    "DI x TR": "TR",
}


def _calculate_discount_factor(taxa_252: float, dias_uteis: int) -> float:
    """
    Calcula o fator de desconto a partir da taxa ao ano base 252.

    FD = 1 / (1 + taxa/100) ^ (dias_uteis / 252)
    """
    if taxa_252 is None or taxa_252 == 0 or dias_uteis == 0:
        return 1.0

    try:
        return 1.0 / math.pow(1.0 + taxa_252 / 100.0, dias_uteis / 252.0)
    except (OverflowError, ValueError, ZeroDivisionError):
        logger.warning(f"Erro ao calcular fator de desconto: taxa={taxa_252}, dias={dias_uteis}")
        return None


def _calculate_rate_360(taxa_252: float) -> float:
    """
    Converte taxa base 252 (dias úteis) para base 360 (dias corridos).

    Taxa_360 = ((1 + taxa_252/100)^(360/252) - 1) * 100
    """
    if taxa_252 is None or taxa_252 == 0:
        return 0.0

    try:
        return (math.pow(1.0 + taxa_252 / 100.0, 360.0 / 252.0) - 1.0) * 100.0
    except (OverflowError, ValueError):
        logger.warning(f"Erro ao converter taxa: {taxa_252}")
        return None


def filter_curve(df: pd.DataFrame, curve_name: str) -> pd.DataFrame:
    """
    Filtra o DataFrame para uma curva específica.

    Args:
        df: DataFrame parseado com todos os registros
        curve_name: Nome da curva (ex: "DI x Pré", "Ajuste Pré", "DI x TR")

    Returns:
        DataFrame filtrado para a curva especificada
    """
    if curve_name not in CURVE_NAMES:
        logger.warning(f"Curva desconhecida: {curve_name}. Curvas válidas: {list(CURVE_NAMES.keys())}")
        return pd.DataFrame()

    mask = df["curva_nome"] == curve_name
    filtered = df[mask].copy()

    if filtered.empty:
        logger.warning(f"Nenhum registro encontrado para curva: {curve_name}")

    return filtered


def enrich_curve(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece os dados de uma curva com campos derivados.

    Adiciona:
    - taxa_252: taxa base 252 (renomeia o campo taxa original)
    - taxa_360: taxa convertida para base 360
    - fator_desconto: fator de desconto calculado
    - data_processamento: timestamp do processamento
    """
    if df.empty:
        return df

    result = df.copy()

    # Renomear taxa -> taxa_252 (assumindo que a taxa da B3 é base 252)
    result = result.rename(columns={"taxa": "taxa_252"})

    # Calcular taxa base 360
    result["taxa_360"] = result.apply(
        lambda row: _calculate_rate_360(row["taxa_252"]),
        axis=1
    )

    # Calcular fator de desconto
    result["fator_desconto"] = result.apply(
        lambda row: _calculate_discount_factor(row["taxa_252"], row["dias_uteis"]),
        axis=1
    )

    # Adicionar timestamp de processamento
    result["data_processamento"] = datetime.now()

    # Reordenar e selecionar colunas
    columns = [
        "data_referencia",
        "curva_nome",
        "curva_id",
        "dias_corridos",
        "dias_uteis",
        "taxa_252",
        "taxa_360",
        "fator_desconto",
        "data_processamento",
    ]

    # Manter apenas colunas que existem
    available_cols = [c for c in columns if c in result.columns]
    result = result[available_cols]

    return result


def transform(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Pipeline completo de transformação.
    Separa os dados por curva e enriquece cada uma.

    Args:
        df: DataFrame parseado com todos os registros

    Returns:
        Dicionário com chave = nome da curva, valor = DataFrame enriquecido
    """
    if df.empty:
        logger.warning("DataFrame vazio, nenhuma transformação realizada")
        return {}

    results = {}
    curves_found = df["curva_nome"].unique() if "curva_nome" in df.columns else []

    logger.info(f"Curvas encontradas no arquivo: {list(curves_found)}")

    for curve_name in CURVE_NAMES.keys():
        logger.info(f"Processando curva: {curve_name}")

        # Filtrar
        curve_df = filter_curve(df, curve_name)
        if curve_df.empty:
            logger.info(f"  -> Sem dados para {curve_name}")
            continue

        # Enriquecer
        enriched = enrich_curve(curve_df)

        # Ordenar por vértice (dias úteis)
        if "dias_uteis" in enriched.columns:
            enriched = enriched.sort_values("dias_uteis").reset_index(drop=True)

        results[curve_name] = enriched
        logger.info(f"  -> {len(enriched)} registros processados para {curve_name}")

    return results
