"""
DAG de Backfill: Pipeline de Taxas Referenciais B3

DAG otimizada para carga histórica de taxas referenciais.
Processa um intervalo de datas configurável via parâmetros,
com batching e delay para evitar throttling da B3.

Uso:
  Trigger com parâmetros:
    {
      "start_date": "2016-01-01",
      "end_date": "2026-03-01",
      "batch_size": 10,
      "delay_seconds": 3
    }
"""

import logging
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ─── Configurações ────────────────────────────────────────────────────────────

DATA_DIR = Variable.get("b3_taxas_data_dir", default_var=os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data"
))

STORAGE_FORMAT = Variable.get("b3_taxas_storage_format", default_var="parquet")

# ─── Argumentos padrão ────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=6),
}

# ─── Funções ──────────────────────────────────────────────────────────────────

def _generate_dates(**kwargs):
    """
    Gera a lista de dias úteis para backfill com base nos parâmetros.
    Detecta gaps em relação aos dados já existentes.
    """
    from plugins.b3_taxas.utils import get_business_days
    from plugins.b3_taxas.storage import StorageConfig, get_existing_dates

    params = kwargs.get("params", {})
    start_str = params.get("start_date", "2016-01-01")
    end_str = params.get("end_date", datetime.now().strftime("%Y-%m-%d"))

    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

    logger.info(f"Gerando datas para backfill: {start_date} a {end_date}")

    # Obter todos os dias úteis no intervalo
    all_business_days = get_business_days(start_date, end_date)
    logger.info(f"Total de dias úteis no intervalo: {len(all_business_days)}")

    # Verificar quais datas já existem
    config = StorageConfig(base_dir=DATA_DIR, storage_format=STORAGE_FORMAT)
    existing = set(get_existing_dates("DI x Pré", config))

    # Filtrar para processar apenas datas faltantes
    missing_dates = [d for d in all_business_days if d not in existing]
    logger.info(
        f"Datas existentes: {len(existing)}, "
        f"Datas faltantes: {len(missing_dates)}"
    )

    # Serializar datas para XCom
    dates_str = [d.isoformat() for d in missing_dates]
    kwargs["ti"].xcom_push(key="dates_to_process", value=dates_str)
    kwargs["ti"].xcom_push(key="total_dates", value=len(dates_str))


def _process_batch(**kwargs):
    """
    Processa um lote de datas.
    Cada data: download → parse → transform → validate → store.
    Com delay entre requisições para evitar bloqueio.
    """
    import pandas as pd
    from plugins.b3_taxas.extractor import extract, B3EmptyFileError, B3DownloadError
    from plugins.b3_taxas.parser import parse_file
    from plugins.b3_taxas.transformer import transform
    from plugins.b3_taxas.validator import validate
    from plugins.b3_taxas.storage import StorageConfig, save_processed, save_published

    ti = kwargs["ti"]
    params = kwargs.get("params", {})

    dates_str = ti.xcom_pull(task_ids="generate_dates", key="dates_to_process")
    batch_size = int(params.get("batch_size", 10))
    delay_seconds = float(params.get("delay_seconds", 3))

    if not dates_str:
        logger.info("Nenhuma data para processar")
        return

    config = StorageConfig(base_dir=DATA_DIR, storage_format=STORAGE_FORMAT)

    total = len(dates_str)
    success_count = 0
    error_count = 0
    skip_count = 0
    errors = []

    logger.info(f"Processando {total} datas com batch_size={batch_size}, delay={delay_seconds}s")

    for i, date_str in enumerate(dates_str):
        dt = datetime.strptime(date_str, "%Y-%m-%d").date()

        try:
            logger.info(f"[{i+1}/{total}] Processando {dt}...")

            # 1. Extrair
            txt_file = extract(dt, config.raw_dir)

            # 2. Parsear
            df = parse_file(txt_file, reference_date=dt)
            if df.empty:
                logger.warning(f"  -> Sem dados para {dt}, pulando")
                skip_count += 1
                continue

            # 3. Transformar
            curves = transform(df)
            if not curves:
                logger.warning(f"  -> Nenhuma curva extraída para {dt}, pulando")
                skip_count += 1
                continue

            # 4. Validar (modo leniente para backfill - não falha, só loga)
            report = validate(curves, expected_date=dt)
            if not report.passed:
                logger.warning(f"  -> Validação com avisos para {dt}")

            # 5. Salvar
            save_processed(curves, config, dt)
            save_published(curves, config)

            success_count += 1
            logger.info(f"  -> OK ({len(curves)} curvas)")

        except B3EmptyFileError as e:
            logger.info(f"  -> Sem dados para {dt}: {e}")
            skip_count += 1

        except B3DownloadError as e:
            logger.error(f"  -> Erro no download para {dt}: {e}")
            error_count += 1
            errors.append({"date": date_str, "error": str(e)})

        except Exception as e:
            logger.error(f"  -> Erro inesperado para {dt}: {e}")
            error_count += 1
            errors.append({"date": date_str, "error": str(e)})

        # Delay entre requests (para não sobrecarregar a B3)
        if i < total - 1:
            time.sleep(delay_seconds)

    # Resumo
    summary = (
        f"\nBackfill concluído:\n"
        f"  Total: {total}\n"
        f"  Sucesso: {success_count}\n"
        f"  Pulados: {skip_count}\n"
        f"  Erros: {error_count}\n"
    )
    logger.info(summary)

    ti.xcom_push(key="summary", value={
        "total": total,
        "success": success_count,
        "skipped": skip_count,
        "errors": error_count,
        "error_details": errors[:50],  # Limitar detalhes
    })


def _report(**kwargs):
    """Gera relatório final do backfill."""
    ti = kwargs["ti"]
    summary = ti.xcom_pull(task_ids="process_batch", key="summary")

    if summary:
        logger.info(
            f"\n{'='*60}\n"
            f"RELATÓRIO FINAL DE BACKFILL\n"
            f"{'='*60}\n"
            f"Total processado: {summary.get('total', 0)}\n"
            f"Sucesso: {summary.get('success', 0)}\n"
            f"Pulados (sem dados): {summary.get('skipped', 0)}\n"
            f"Erros: {summary.get('errors', 0)}\n"
            f"{'='*60}"
        )

        if summary.get('error_details'):
            logger.warning(
                f"Datas com erro:\n" +
                "\n".join(
                    f"  - {e['date']}: {e['error']}"
                    for e in summary['error_details']
                )
            )


# ─── Definição da DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id="backfill_taxas_referenciais_b3",
    description="Backfill de taxas referenciais B3 - carga histórica com batching e gap detection",
    schedule_interval=None,  # Trigger manual apenas
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["b3", "taxas-referenciais", "backfill"],
    params={
        "start_date": "2016-01-01",
        "end_date": "2026-03-10",
        "batch_size": 10,
        "delay_seconds": 3,
    },
    doc_md=__doc__,
) as dag:

    generate_dates = PythonOperator(
        task_id="generate_dates",
        python_callable=_generate_dates,
        provide_context=True,
    )

    process_batch = PythonOperator(
        task_id="process_batch",
        python_callable=_process_batch,
        provide_context=True,
    )

    report = PythonOperator(
        task_id="report",
        python_callable=_report,
        provide_context=True,
    )

    generate_dates >> process_batch >> report
