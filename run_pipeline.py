"""
Script standalone para executar o pipeline de taxas referenciais B3.
Pode ser executado diretamente (sem Airflow) para gerar dados.

Uso:
    python run_pipeline.py                          # Executa para última data útil
    python run_pipeline.py --date 2026-03-06        # Executa para data específica
    python run_pipeline.py --backfill 2026-01-01 2026-03-10   # Backfill de intervalo
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

# Adicionar raiz do projeto ao path
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_DIR)

from plugins.b3_taxas.utils import is_business_day, get_business_days
from plugins.b3_taxas.extractor import extract, B3EmptyFileError, B3DownloadError
from plugins.b3_taxas.parser import parse_file
from plugins.b3_taxas.transformer import transform
from plugins.b3_taxas.validator import validate
from plugins.b3_taxas.storage import StorageConfig, save_processed, save_published

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_pipeline")

DATA_DIR = os.path.join(PROJECT_DIR, "data")


def run_for_date(dt: date, config: StorageConfig) -> bool:
    """
    Executa o pipeline completo para uma data.
    Retorna True se bem-sucedido.
    """
    logger.info(f"{'='*60}")
    logger.info(f"Processando data: {dt}")
    logger.info(f"{'='*60}")

    try:
        # 1. Extrair
        logger.info("[1/5] Extraindo dados da B3...")
        txt_file = extract(dt, config.raw_dir)
        logger.info(f"  Arquivo extraído: {txt_file}")

        # 2. Parsear
        logger.info("[2/5] Parseando arquivo...")
        df = parse_file(txt_file, reference_date=dt)
        if df.empty:
            logger.warning(f"  Nenhum dado parseado para {dt}")
            return False
        logger.info(f"  {len(df)} registros parseados")

        # 3. Transformar
        logger.info("[3/5] Transformando dados por curva...")
        curves = transform(df)
        if not curves:
            logger.warning(f"  Nenhuma curva extraída para {dt}")
            return False
        for name, cdf in curves.items():
            logger.info(f"  {name}: {len(cdf)} vértices")

        # 4. Validar
        logger.info("[4/5] Validando qualidade...")
        report = validate(curves, expected_date=dt)
        logger.info(f"\n{report.summary()}")

        # 5. Salvar
        logger.info("[5/5] Salvando dados...")
        processed_paths = save_processed(curves, config, dt)
        published_paths = save_published(curves, config)
        for name, path in published_paths.items():
            logger.info(f"  Publicado: {path}")

        logger.info(f"✅ Data {dt} processada com sucesso!")
        return True

    except B3EmptyFileError as e:
        logger.warning(f"⚠️  Sem dados para {dt}: {e}")
        return False

    except B3DownloadError as e:
        logger.error(f"❌ Erro no download para {dt}: {e}")
        return False

    except Exception as e:
        logger.error(f"❌ Erro inesperado para {dt}: {e}", exc_info=True)
        return False


def find_last_business_day() -> date:
    """Encontra o último dia útil (hoje ou anterior)."""
    dt = date.today()
    while not is_business_day(dt):
        dt -= timedelta(days=1)
    return dt


def main():
    parser = argparse.ArgumentParser(description="Pipeline de Taxas Referenciais B3")
    parser.add_argument("--date", type=str, help="Data específica (YYYY-MM-DD)")
    parser.add_argument(
        "--backfill", nargs=2, metavar=("START", "END"),
        help="Backfill de intervalo (YYYY-MM-DD YYYY-MM-DD)"
    )
    parser.add_argument("--delay", type=float, default=3.0, help="Delay entre requests (backfill)")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet", help="Formato de saída")

    args = parser.parse_args()
    config = StorageConfig(base_dir=DATA_DIR, storage_format=args.format)

    if args.backfill:
        # Modo backfill
        start = datetime.strptime(args.backfill[0], "%Y-%m-%d").date()
        end = datetime.strptime(args.backfill[1], "%Y-%m-%d").date()
        dates = get_business_days(start, end)

        logger.info(f"Backfill: {start} a {end} ({len(dates)} dias úteis)")

        success = 0
        errors = 0
        skipped = 0

        for i, dt in enumerate(dates):
            result = run_for_date(dt, config)
            if result:
                success += 1
            else:
                skipped += 1

            if i < len(dates) - 1:
                time.sleep(args.delay)

        logger.info(f"\n{'='*60}")
        logger.info(f"BACKFILL COMPLETO: {success} ok, {skipped} sem dados, {errors} erros")
        logger.info(f"{'='*60}")

    else:
        # Modo single date
        if args.date:
            dt = datetime.strptime(args.date, "%Y-%m-%d").date()
        else:
            dt = find_last_business_day()

        run_for_date(dt, config)


if __name__ == "__main__":
    main()
