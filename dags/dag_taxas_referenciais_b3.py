"""
DAG principal: Pipeline de Taxas Referenciais B3

Executa diariamente em dias úteis às 20h (horário de Brasília),
coletando, processando e armazenando as curvas de taxas referenciais:
- DI x Pré
- Ajuste Pré
- DI x TR

Fonte: B3 - Mercado de Derivativos - Taxas de Mercado para Swaps
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ─── Configurações ────────────────────────────────────────────────────────────

# Diretório base de dados (pode ser sobrescrito via Airflow Variable)
DATA_DIR = Variable.get("b3_taxas_data_dir", default_var=os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data"
))

# Formato de armazenamento: "parquet" ou "csv"
STORAGE_FORMAT = Variable.get("b3_taxas_storage_format", default_var="parquet")

# ─── Argumentos padrão da DAG ─────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
}

# ─── Funções das Tasks ───────────────────────────────────────────────────────

def _check_business_day(**kwargs):
    """Verifica se a execution_date é dia útil. Se não for, encerra a DAG."""
    from plugins.b3_taxas.utils import is_business_day

    execution_date = kwargs["ds"]
    dt = datetime.strptime(execution_date, "%Y-%m-%d").date()

    is_bd = is_business_day(dt)
    logger.info(f"Data {dt}: {'dia útil' if is_bd else 'não é dia útil'}")
    return is_bd


def _extract(**kwargs):
    """Baixa e descomprime o arquivo de taxas da B3."""
    from plugins.b3_taxas.extractor import extract
    from plugins.b3_taxas.storage import StorageConfig

    execution_date = kwargs["ds"]
    dt = datetime.strptime(execution_date, "%Y-%m-%d").date()

    config = StorageConfig(base_dir=DATA_DIR, storage_format=STORAGE_FORMAT)
    txt_file = extract(dt, config.raw_dir)

    # Passar path para próxima task via XCom
    kwargs["ti"].xcom_push(key="txt_file", value=txt_file)
    kwargs["ti"].xcom_push(key="reference_date", value=execution_date)
    logger.info(f"Arquivo extraído: {txt_file}")


def _parse(**kwargs):
    """Faz parsing do arquivo de taxas."""
    from plugins.b3_taxas.parser import parse_file

    ti = kwargs["ti"]
    txt_file = ti.xcom_pull(task_ids="extract", key="txt_file")
    execution_date = ti.xcom_pull(task_ids="extract", key="reference_date")
    dt = datetime.strptime(execution_date, "%Y-%m-%d").date()

    df = parse_file(txt_file, reference_date=dt)

    if df.empty:
        raise ValueError(f"Nenhum dado parseado para {dt}")

    # Serializar para XCom via JSON
    ti.xcom_push(key="parsed_data", value=df.to_json(orient="records", date_format="iso"))
    logger.info(f"Parseados {len(df)} registros")


def _transform(**kwargs):
    """Transforma e enriquece os dados por curva."""
    import pandas as pd
    from plugins.b3_taxas.transformer import transform

    ti = kwargs["ti"]
    parsed_json = ti.xcom_pull(task_ids="parse", key="parsed_data")
    df = pd.read_json(parsed_json, orient="records")

    # Converter data_referencia de volta para date
    if "data_referencia" in df.columns:
        df["data_referencia"] = pd.to_datetime(df["data_referencia"]).dt.date

    curves = transform(df)

    # Serializar cada curva para XCom
    curves_json = {}
    for name, curve_df in curves.items():
        curves_json[name] = curve_df.to_json(orient="records", date_format="iso")

    ti.xcom_push(key="curves_data", value=curves_json)
    logger.info(f"Curvas transformadas: {list(curves.keys())}")


def _validate(**kwargs):
    """Executa validações de qualidade de dados."""
    import pandas as pd
    from plugins.b3_taxas.validator import validate

    ti = kwargs["ti"]
    execution_date = ti.xcom_pull(task_ids="extract", key="reference_date")
    dt = datetime.strptime(execution_date, "%Y-%m-%d").date()
    curves_json = ti.xcom_pull(task_ids="transform", key="curves_data")

    # Deserializar
    curves = {}
    for name, json_str in curves_json.items():
        df = pd.read_json(json_str, orient="records")
        if "data_referencia" in df.columns:
            df["data_referencia"] = pd.to_datetime(df["data_referencia"]).dt.date
        curves[name] = df

    report = validate(curves, expected_date=dt)

    if not report.passed:
        logger.error(f"Validação FALHOU:\n{report.summary()}")
        raise ValueError(f"Validação de qualidade falhou para {dt}")

    logger.info(f"Validação OK:\n{report.summary()}")
    ti.xcom_push(key="validation_passed", value=True)


def _store(**kwargs):
    """Persiste os dados processados e publicados."""
    import pandas as pd
    from plugins.b3_taxas.storage import StorageConfig, save_processed, save_published

    ti = kwargs["ti"]
    execution_date = ti.xcom_pull(task_ids="extract", key="reference_date")
    dt = datetime.strptime(execution_date, "%Y-%m-%d").date()
    curves_json = ti.xcom_pull(task_ids="transform", key="curves_data")

    # Deserializar
    curves = {}
    for name, json_str in curves_json.items():
        df = pd.read_json(json_str, orient="records")
        if "data_referencia" in df.columns:
            df["data_referencia"] = pd.to_datetime(df["data_referencia"]).dt.date
        curves[name] = df

    config = StorageConfig(base_dir=DATA_DIR, storage_format=STORAGE_FORMAT)

    # Salvar dados processados (particionados por data)
    processed_paths = save_processed(curves, config, dt)
    logger.info(f"Dados processados salvos: {processed_paths}")

    # Salvar/atualizar dados publicados (consolidados)
    published_paths = save_published(curves, config)
    logger.info(f"Dados publicados atualizados: {published_paths}")


# ─── Definição da DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id="taxas_referenciais_b3",
    description="Pipeline de coleta e processamento de taxas referenciais B3 (DI x Pré, Ajuste Pré, DI x TR)",
    schedule_interval="0 20 * * 1-5",  # 20h, seg-sex (horário do servidor)
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["b3", "taxas-referenciais", "swap", "renda-fixa"],
    doc_md=__doc__,
) as dag:

    check_business_day = ShortCircuitOperator(
        task_id="check_business_day",
        python_callable=_check_business_day,
        provide_context=True,
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        provide_context=True,
    )

    parse = PythonOperator(
        task_id="parse",
        python_callable=_parse,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform,
        provide_context=True,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=_validate,
        provide_context=True,
    )

    store = PythonOperator(
        task_id="store",
        python_callable=_store,
        provide_context=True,
    )

    # Pipeline linear
    check_business_day >> extract >> parse >> transform >> validate >> store
