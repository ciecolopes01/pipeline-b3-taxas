"""
Utilitários para o pipeline de taxas referenciais B3.
Inclui funções de calendário, formatação de datas e helpers gerais.
"""

import logging
from datetime import date, datetime, timedelta
from typing import List

import holidays

logger = logging.getLogger(__name__)

# Feriados brasileiros (ANBIMA-like calendar)
BR_HOLIDAYS = holidays.Brazil()

# Feriados adicionais específicos do mercado financeiro (não cobertos pela lib holidays)
# Esses são feriados da ANBIMA que afetam o calendário de dias úteis
MARKET_EXTRA_HOLIDAYS = {
    # Consciência Negra (feriado a nível municipal em SP, onde fica a B3)
    # A partir de 2024 é feriado nacional
}


def is_business_day(dt: date) -> bool:
    """
    Verifica se uma data é dia útil no calendário financeiro brasileiro.
    Dia útil = não é fim de semana e não é feriado.
    """
    if dt.weekday() >= 5:  # Sábado (5) ou Domingo (6)
        return False
    if dt in BR_HOLIDAYS:
        return False
    if dt in MARKET_EXTRA_HOLIDAYS:
        return False
    return True


def get_business_days(start_date: date, end_date: date) -> List[date]:
    """
    Retorna lista de dias úteis entre start_date e end_date (inclusive).
    """
    days = []
    current = start_date
    while current <= end_date:
        if is_business_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


def format_date_yymmdd(dt: date) -> str:
    """
    Formata data para o padrão YYMMDD usado pela B3 nos nomes de arquivo.
    Ex: 2026-03-06 -> '260306'
    """
    return dt.strftime("%y%m%d")


def format_date_ddmmyyyy(dt: date) -> str:
    """
    Formata data para o padrão DD/MM/YYYY.
    """
    return dt.strftime("%d/%m/%Y")


def parse_date_ddmmyyyy(s: str) -> date:
    """
    Parseia data no formato DD/MM/YYYY.
    """
    return datetime.strptime(s.strip(), "%d/%m/%Y").date()


def parse_date_yyyymmdd(s: str) -> date:
    """
    Parseia data no formato YYYYMMDD.
    """
    return datetime.strptime(s.strip(), "%Y%m%d").date()


def build_filename(dt: date) -> str:
    """
    Constrói o nome do arquivo de taxas de swap da B3 para uma data.
    Formato: TS{YYMMDD}.ex_
    """
    return f"TS{format_date_yymmdd(dt)}.ex_"


def build_download_url(dt: date) -> str:
    """
    Constrói a URL de download do arquivo de taxas de swap para uma data.
    """
    filename = build_filename(dt)
    return f"https://www.b3.com.br/pesquisapregao/download?filelist={filename}"


def find_missing_dates(existing_dates: List[date], start_date: date, end_date: date) -> List[date]:
    """
    Encontra dias úteis faltantes em um intervalo de datas.
    Útil para detecção de gaps no histórico.
    """
    expected = set(get_business_days(start_date, end_date))
    existing = set(existing_dates)
    missing = sorted(expected - existing)
    return missing


def batch_dates(dates: List[date], batch_size: int = 10) -> List[List[date]]:
    """
    Divide uma lista de datas em lotes de tamanho batch_size.
    Usado no backfill para evitar throttling da B3.
    """
    return [dates[i:i + batch_size] for i in range(0, len(dates), batch_size)]
