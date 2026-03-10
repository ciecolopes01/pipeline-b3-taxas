"""
Módulo de persistência de dados para o pipeline de taxas referenciais B3.
Suporta armazenamento local (filesystem) e S3 (via variáveis de ambiente).
"""

import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Formato padrão de armazenamento
DEFAULT_FORMAT = "parquet"
SUPPORTED_FORMATS = {"parquet", "csv"}


class StorageConfig:
    """Configuração de armazenamento."""

    def __init__(
        self,
        base_dir: str = "data",
        storage_format: str = DEFAULT_FORMAT,
        s3_bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None,
    ):
        self.base_dir = base_dir
        self.storage_format = storage_format
        self.s3_bucket = s3_bucket or os.environ.get("B3_S3_BUCKET")
        self.s3_prefix = s3_prefix or os.environ.get("B3_S3_PREFIX", "taxas_referenciais")

    @property
    def use_s3(self) -> bool:
        return self.s3_bucket is not None

    @property
    def raw_dir(self) -> str:
        return os.path.join(self.base_dir, "raw")

    @property
    def processed_dir(self) -> str:
        return os.path.join(self.base_dir, "processed")

    @property
    def published_dir(self) -> str:
        return os.path.join(self.base_dir, "published")


def _build_partition_path(base_dir: str, curve_name: str, dt: date, fmt: str) -> str:
    """
    Constrói o path particionado por curva e data.
    Formato: base_dir/curva={curve_name}/ano={YYYY}/mes={MM}/dia={DD}/data.{fmt}
    """
    safe_curve = curve_name.replace(" ", "_").replace("x", "x").lower()
    partition_dir = os.path.join(
        base_dir,
        f"curva={safe_curve}",
        f"ano={dt.year}",
        f"mes={dt.month:02d}",
    )
    filename = f"{dt.isoformat()}.{fmt}"
    return os.path.join(partition_dir, filename)


def save_processed(
    curves: Dict[str, pd.DataFrame],
    config: StorageConfig,
    reference_date: date,
) -> Dict[str, str]:
    """
    Salva os dados processados por curva em formato particionado.

    Args:
        curves: Dicionário curva -> DataFrame
        config: Configuração de armazenamento
        reference_date: Data de referência dos dados

    Returns:
        Dicionário curva -> path do arquivo salvo
    """
    saved_paths = {}

    for curve_name, df in curves.items():
        if df.empty:
            logger.warning(f"DataFrame vazio para {curve_name}, pulando...")
            continue

        filepath = _build_partition_path(
            config.processed_dir,
            curve_name,
            reference_date,
            config.storage_format,
        )

        # Criar diretório
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Salvar
        if config.storage_format == "parquet":
            # Converter datas para tipos compatíveis com Parquet
            df_save = df.copy()
            if "data_referencia" in df_save.columns:
                df_save["data_referencia"] = pd.to_datetime(df_save["data_referencia"])
            if "data_processamento" in df_save.columns:
                df_save["data_processamento"] = pd.to_datetime(df_save["data_processamento"])
            df_save.to_parquet(filepath, index=False, engine="pyarrow")
        elif config.storage_format == "csv":
            df.to_csv(filepath, index=False, sep=";", encoding="utf-8")
        else:
            raise ValueError(f"Formato não suportado: {config.storage_format}")

        logger.info(f"Salvo {curve_name}: {filepath} ({len(df)} registros)")
        saved_paths[curve_name] = filepath

    return saved_paths


def save_published(
    curves: Dict[str, pd.DataFrame],
    config: StorageConfig,
) -> Dict[str, str]:
    """
    Salva/atualiza os dados publicados para consumo.
    Consolida os dados em um arquivo por curva (append ou upsert).

    Args:
        curves: Dicionário curva -> DataFrame
        config: Configuração de armazenamento

    Returns:
        Dicionário curva -> path do arquivo publicado
    """
    saved_paths = {}

    for curve_name, df in curves.items():
        if df.empty:
            continue

        safe_curve = curve_name.replace(" ", "_").replace("x", "x").lower()
        filepath = os.path.join(
            config.published_dir,
            f"taxas_{safe_curve}.{config.storage_format}",
        )

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Carregar dados existentes (se houver) e fazer upsert
        existing_df = None
        if os.path.exists(filepath):
            try:
                if config.storage_format == "parquet":
                    existing_df = pd.read_parquet(filepath)
                elif config.storage_format == "csv":
                    existing_df = pd.read_csv(filepath, sep=";", encoding="utf-8")
            except Exception as e:
                logger.warning(f"Erro ao ler arquivo existente {filepath}: {e}")

        if existing_df is not None and not existing_df.empty:
            # Converter data_referencia para o mesmo tipo
            df_save = df.copy()
            if "data_referencia" in df_save.columns:
                df_save["data_referencia"] = pd.to_datetime(df_save["data_referencia"])
            if "data_referencia" in existing_df.columns:
                existing_df["data_referencia"] = pd.to_datetime(existing_df["data_referencia"])
            if "data_processamento" in df_save.columns:
                df_save["data_processamento"] = pd.to_datetime(df_save["data_processamento"])
            if "data_processamento" in existing_df.columns:
                existing_df["data_processamento"] = pd.to_datetime(existing_df["data_processamento"])

            # Upsert: remover linhas existentes para a mesma data_referencia e dias_uteis
            key_cols = ["data_referencia", "dias_uteis"]
            available_keys = [c for c in key_cols if c in existing_df.columns and c in df_save.columns]

            if available_keys:
                # Criar chave composta para merge
                existing_df["_key"] = existing_df[available_keys].astype(str).agg("_".join, axis=1)
                df_save["_key"] = df_save[available_keys].astype(str).agg("_".join, axis=1)

                # Remover registros antigos que serão substituídos
                existing_df = existing_df[~existing_df["_key"].isin(df_save["_key"])]
                existing_df = existing_df.drop(columns=["_key"])
                df_save = df_save.drop(columns=["_key"])

            # Concatenar
            combined = pd.concat([existing_df, df_save], ignore_index=True)

            # Ordenar
            sort_cols = [c for c in ["data_referencia", "dias_uteis"] if c in combined.columns]
            if sort_cols:
                combined = combined.sort_values(sort_cols).reset_index(drop=True)
        else:
            combined = df.copy()
            if "data_referencia" in combined.columns:
                combined["data_referencia"] = pd.to_datetime(combined["data_referencia"])
            if "data_processamento" in combined.columns:
                combined["data_processamento"] = pd.to_datetime(combined["data_processamento"])

        # Salvar
        if config.storage_format == "parquet":
            combined.to_parquet(filepath, index=False, engine="pyarrow")
        elif config.storage_format == "csv":
            combined.to_csv(filepath, index=False, sep=";", encoding="utf-8")

        logger.info(f"Publicado {curve_name}: {filepath} ({len(combined)} registros totais)")
        saved_paths[curve_name] = filepath

    return saved_paths


def load_published(
    curve_name: str,
    config: StorageConfig,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Carrega dados publicados para uma curva, opcionalmente filtrando por datas.

    Args:
        curve_name: Nome da curva
        config: Configuração de armazenamento
        start_date: Data inicial (opcional)
        end_date: Data final (opcional)

    Returns:
        DataFrame com os dados publicados
    """
    safe_curve = curve_name.replace(" ", "_").replace("x", "x").lower()
    filepath = os.path.join(
        config.published_dir,
        f"taxas_{safe_curve}.{config.storage_format}",
    )

    if not os.path.exists(filepath):
        logger.warning(f"Arquivo não encontrado: {filepath}")
        return pd.DataFrame()

    try:
        if config.storage_format == "parquet":
            df = pd.read_parquet(filepath)
        elif config.storage_format == "csv":
            df = pd.read_csv(filepath, sep=";", encoding="utf-8")
        else:
            raise ValueError(f"Formato não suportado: {config.storage_format}")
    except Exception as e:
        logger.error(f"Erro ao ler {filepath}: {e}")
        return pd.DataFrame()

    # Filtrar por datas
    if "data_referencia" in df.columns:
        df["data_referencia"] = pd.to_datetime(df["data_referencia"])
        if start_date:
            df = df[df["data_referencia"] >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df["data_referencia"] <= pd.Timestamp(end_date)]

    return df


def get_existing_dates(
    curve_name: str,
    config: StorageConfig,
) -> List[date]:
    """
    Retorna lista de datas já existentes nos dados publicados para uma curva.
    Útil para detecção de gaps.
    """
    df = load_published(curve_name, config)
    if df.empty or "data_referencia" not in df.columns:
        return []

    dates = pd.to_datetime(df["data_referencia"]).dt.date.unique().tolist()
    return sorted(dates)
