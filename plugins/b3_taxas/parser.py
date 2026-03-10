"""
Parser de arquivos de taxas de swap da B3.
Faz parsing do arquivo posicional (largura fixa) para DataFrame.

Layout do arquivo "Mercado de Derivativos - Taxas de Mercado para Swaps":
O arquivo contém taxas referenciais para diferentes curvas (DI x Pre, DI x TR, etc.)
organizadas em registros de largura fixa com header, registros de detalhe e trailer.

Campos principais (baseado no layout oficial da B3):
- Tipo de registro (1 char)
- Data de referência (8 chars, DDMMYYYY ou YYYYMMDD)
- Identificação da curva (2-4 chars)
- Vértice / Prazo em dias corridos (5 chars)
- Vértice / Prazo em dias úteis (5 chars)
- Taxa (%a.a.) (campo numérico com decimais implícitos)
"""

import logging
import re
from datetime import date, datetime
from io import StringIO
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Mapeamento de identificadores de curva para nomes legíveis
CURVE_MAP = {
    "PRE": "DI x Pré",
    "DIC": "DI x Pré",  # Alias usado em alguns períodos
    "DIM": "DI x Pré",  # Outro alias
    "TP":  "DI x Pré",  # Taxa Pré
    "DOC": "Ajuste Pré",  # Ajuste
    "APR": "Ajuste Pré",
    "DAP": "Ajuste Pré",
    "TR":  "DI x TR",
    "DTR": "DI x TR",
}

# Curvas de interesse
TARGET_CURVES = {"DI x Pré", "Ajuste Pré", "DI x TR"}


def _detect_format(content: str) -> str:
    """
    Detecta o formato do arquivo baseado no conteúdo.
    O formato mudou ao longo dos anos na B3.
    """
    lines = content.strip().split('\n')
    if not lines:
        return "unknown"

    first_line = lines[0].strip()

    # Formato CSV com delimitador ';'
    if ';' in first_line:
        return "csv_semicolon"

    # Formato posicional antigo
    if len(first_line) > 50 and first_line[0].isdigit():
        return "fixed_width"

    # Formato com delimitador tabulação
    if '\t' in first_line:
        return "csv_tab"

    # Formato com delimitador '|'
    if '|' in first_line:
        return "csv_pipe"

    # Tentar como posicional por padrão
    return "fixed_width"


def _parse_csv_semicolon(content: str) -> pd.DataFrame:
    """
    Parseia arquivo no formato CSV com delimitador ';'.
    Este é o formato mais comum nos arquivos recentes da B3.
    """
    lines = content.strip().split('\n')
    records = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        parts = line.split(';')
        if len(parts) < 4:
            continue

        try:
            record = _extract_record_from_parts(parts)
            if record:
                records.append(record)
        except (ValueError, IndexError) as e:
            logger.debug(f"Ignorando linha com erro: {line} -> {e}")
            continue

    return pd.DataFrame(records)


def _extract_record_from_parts(parts: List[str]) -> Optional[Dict]:
    """
    Extrai um registro a partir das partes de uma linha CSV.
    Adapta-se a diferentes formatos de coluna encontrados nos arquivos B3.
    """
    # Identificar a data de referência (procura por padrão de data)
    data_ref = None
    curva_id = None
    dias_corridos = None
    dias_uteis = None
    taxa = None

    for i, part in enumerate(parts):
        part = part.strip()

        # Detectar data (DD/MM/YYYY ou YYYYMMDD ou DDMMYYYY)
        if data_ref is None:
            if re.match(r'^\d{2}/\d{2}/\d{4}$', part):
                data_ref = datetime.strptime(part, "%d/%m/%Y").date()
                continue
            elif re.match(r'^\d{8}$', part) and len(part) == 8:
                try:
                    data_ref = datetime.strptime(part, "%Y%m%d").date()
                except ValueError:
                    try:
                        data_ref = datetime.strptime(part, "%d%m%Y").date()
                    except ValueError:
                        pass
                continue

        # Detectar identificador da curva
        if curva_id is None and part.upper() in CURVE_MAP:
            curva_id = part.upper()
            continue

        # Detectar campos numéricos (dias ou taxa)
        if part.replace('.', '').replace(',', '').replace('-', '').isdigit():
            val = part.replace(',', '.')
            try:
                num = float(val)
                if dias_corridos is None and num > 0 and num < 20000:
                    dias_corridos = int(num)
                elif dias_uteis is None and num > 0 and num < 20000:
                    dias_uteis = int(num)
                elif taxa is None:
                    taxa = num
            except ValueError:
                continue

    if data_ref and taxa is not None:
        return {
            "data_referencia": data_ref,
            "curva_id": curva_id or "UNKNOWN",
            "curva_nome": CURVE_MAP.get(curva_id, "Desconhecida") if curva_id else "Desconhecida",
            "dias_corridos": dias_corridos or 0,
            "dias_uteis": dias_uteis or 0,
            "taxa": taxa,
        }
    return None


def _parse_fixed_width(content: str, reference_date: Optional[date] = None) -> pd.DataFrame:
    """
    Parseia arquivo no formato posicional (largura fixa) de Swap da B3 (TaxaSwap.txt).

    Exemplo linha:
    0147150010120260306T1PRE  DIxPRE         0000300001+00000149000000F00001
    """
    lines = content.strip().split('\n')
    records = []

    for line in lines:
        line = line.rstrip('\r')
        # Filtra header/trailer pelo tamanho
        if len(line) < 70:
            continue

        try:
            # Layout identificação (19-21) Record type (T1=Detail)
            if line[19:21] != "T1":
                continue

            data_str = line[11:19]
            if not data_str.isdigit():
                continue

            curva_id = line[21:26].strip()
            curva_nome_raw = line[26:41].strip()
            
            dias_corridos_str = line[41:46]
            dias_uteis_str = line[46:51]
            sinal_str = line[51:52]
            taxa_raw_str = line[52:66]
            
            # Se não tem números onde devia, descarta
            if not (dias_corridos_str.isdigit() and dias_uteis_str.isdigit() and taxa_raw_str.isdigit()):
                continue

            dias_corridos = int(dias_corridos_str)
            dias_uteis = int(dias_uteis_str)
            sinal = -1 if sinal_str == "-" else 1
            
            # Taxa tem 7 decimais implícitos
            taxa = sinal * (int(taxa_raw_str) / 10000000.0)

            try:
                data_ref = datetime.strptime(data_str, "%Y%m%d").date()
            except ValueError:
                data_ref = reference_date

            # Mapear curvas
            if curva_nome_raw == "DIxPRE":
                curva_nome = "DI x Pré"
            elif curva_nome_raw == "DIxPRE Aj. PRE":
                curva_nome = "Ajuste Pré"
            elif curva_nome_raw == "DIxTR":
                curva_nome = "DI x TR"
            else:
                # Usa fallback do mapeamento CURVE_MAP se bater pelo ID (para dados mais velhos)
                curva_nome = CURVE_MAP.get(curva_id, "Desconhecida")

            if curva_nome != "Desconhecida":
                records.append({
                    "data_referencia": data_ref,
                    "curva_id": curva_id,
                    "curva_nome": curva_nome,
                    "dias_corridos": dias_corridos,
                    "dias_uteis": dias_uteis,
                    "taxa": taxa,
                })

        except (ValueError, IndexError):
            continue

    return pd.DataFrame(records)


def _parse_generic(content: str, reference_date: Optional[date] = None) -> pd.DataFrame:
    """
    Parser genérico que tenta múltiplos delimitadores e formatos.
    Fallback quando o formato não é detectado claramente.
    """
    # Tentar com diferentes delimitadores
    for sep in [';', '\t', '|', ',']:
        try:
            df = pd.read_csv(
                StringIO(content),
                sep=sep,
                header=None,
                encoding='latin-1',
                on_bad_lines='skip',
            )
            if len(df.columns) >= 4 and len(df) > 0:
                logger.info(f"Parseado como CSV com delimitador '{sep}': {len(df)} linhas")
                return _normalize_generic_df(df, reference_date)
        except Exception:
            continue

    return pd.DataFrame()


def _normalize_generic_df(df: pd.DataFrame, reference_date: Optional[date] = None) -> pd.DataFrame:
    """
    Normaliza um DataFrame genérico para o formato padrão.
    Tenta identificar as colunas com base no conteúdo.
    """
    records = []

    for _, row in df.iterrows():
        data_ref = reference_date
        curva_id = None
        dias_corridos = 0
        dias_uteis = 0
        taxa = None

        for val in row:
            val_str = str(val).strip()

            # Detectar data
            if data_ref is None:
                if re.match(r'^\d{2}/\d{2}/\d{4}$', val_str):
                    data_ref = datetime.strptime(val_str, "%d/%m/%Y").date()
                    continue
                elif re.match(r'^\d{8}$', val_str):
                    try:
                        data_ref = datetime.strptime(val_str, "%Y%m%d").date()
                        continue
                    except ValueError:
                        pass

            # Detectar curva
            if curva_id is None and val_str.upper() in CURVE_MAP:
                curva_id = val_str.upper()
                continue

            # Detectar numéricos
            try:
                num = float(str(val).replace(',', '.'))
                if taxa is None and num != 0:
                    if dias_corridos == 0 and 0 < num < 20000:
                        dias_corridos = int(num)
                    elif dias_uteis == 0 and 0 < num < 20000:
                        dias_uteis = int(num)
                    else:
                        taxa = num
            except (ValueError, TypeError):
                continue

        if data_ref and taxa is not None:
            records.append({
                "data_referencia": data_ref,
                "curva_id": curva_id or "UNKNOWN",
                "curva_nome": CURVE_MAP.get(curva_id, "Desconhecida") if curva_id else "Desconhecida",
                "dias_corridos": dias_corridos,
                "dias_uteis": dias_uteis,
                "taxa": taxa,
            })

    return pd.DataFrame(records)


def parse_file(file_path: str, reference_date: Optional[date] = None) -> pd.DataFrame:
    """
    Faz parsing do arquivo de taxas de swap da B3.

    Args:
        file_path: Caminho para o arquivo texto extraído
        reference_date: Data de referência (usada quando a data não está no arquivo)

    Returns:
        DataFrame com colunas:
        - data_referencia (date)
        - curva_id (str)
        - curva_nome (str)
        - dias_corridos (int)
        - dias_uteis (int)
        - taxa (float)
    """
    logger.info(f"Parseando arquivo: {file_path}")

    # Tentar encodings comuns da B3
    content = None
    for encoding in ['latin-1', 'cp1252', 'utf-8', 'ascii']:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            break
        except UnicodeDecodeError:
            continue

    if content is None:
        raise ValueError(f"Não foi possível ler o arquivo {file_path} com nenhum encoding suportado")

    if not content.strip():
        logger.warning(f"Arquivo vazio: {file_path}")
        return pd.DataFrame()

    # Detectar formato e parsear
    fmt = _detect_format(content)
    logger.info(f"Formato detectado: {fmt}")

    if fmt == "csv_semicolon":
        df = _parse_csv_semicolon(content)
    elif fmt == "csv_tab":
        df = _parse_generic(content, reference_date)
    elif fmt == "csv_pipe":
        df = _parse_generic(content, reference_date)
    elif fmt == "fixed_width":
        df = _parse_fixed_width(content, reference_date)
    else:
        df = _parse_generic(content, reference_date)

    if df.empty:
        logger.warning(f"Nenhum registro extraído do arquivo {file_path}")
        return df

    # Garantir tipos corretos
    if "data_referencia" in df.columns:
        df["data_referencia"] = pd.to_datetime(df["data_referencia"]).dt.date
    if "dias_corridos" in df.columns:
        df["dias_corridos"] = pd.to_numeric(df["dias_corridos"], errors="coerce").fillna(0).astype(int)
    if "dias_uteis" in df.columns:
        df["dias_uteis"] = pd.to_numeric(df["dias_uteis"], errors="coerce").fillna(0).astype(int)
    if "taxa" in df.columns:
        df["taxa"] = pd.to_numeric(df["taxa"], errors="coerce")

    # Remover linhas sem taxa
    df = df.dropna(subset=["taxa"])

    logger.info(f"Parseados {len(df)} registros do arquivo {file_path}")
    return df
