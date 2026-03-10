"""
Validador de qualidade de dados para taxas referenciais B3.
Implementa 4 pilares: Completude, Continuidade, Atualização e Consistência.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Set

import pandas as pd

from plugins.b3_taxas.utils import get_business_days, is_business_day

logger = logging.getLogger(__name__)

# Número mínimo de vértices para considerar uma curva completa
MIN_VERTICES_PRE = 10  # DI x Pré normalmente tem 30+ vértices
MIN_VERTICES_APR = 5   # Ajuste Pré pode ter menos
MIN_VERTICES_TR = 5    # DI x TR pode ter menos

CURVE_MIN_VERTICES = {
    "DI x Pré": MIN_VERTICES_PRE,
    "Ajuste Pré": MIN_VERTICES_APR,
    "DI x TR": MIN_VERTICES_TR,
}


@dataclass
class ValidationResult:
    """Resultado de uma validação individual."""
    check_name: str
    passed: bool
    message: str
    severity: str = "WARNING"  # INFO, WARNING, ERROR
    details: Dict = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Relatório consolidado de validação."""
    reference_date: date
    timestamp: datetime
    results: List[ValidationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Validação geral passou se nenhum resultado ERROR falhou."""
        return all(
            r.passed for r in self.results if r.severity == "ERROR"
        )

    @property
    def warnings(self) -> List[ValidationResult]:
        return [r for r in self.results if not r.passed and r.severity == "WARNING"]

    @property
    def errors(self) -> List[ValidationResult]:
        return [r for r in self.results if not r.passed and r.severity == "ERROR"]

    def summary(self) -> str:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        status = "✅ PASSED" if self.passed else "❌ FAILED"
        lines = [
            f"Relatório de Validação - {self.reference_date}",
            f"Status: {status}",
            f"Checks: {passed}/{total} passaram, {failed} falharam",
            f"Erros: {len(self.errors)}, Avisos: {len(self.warnings)}",
        ]
        for r in self.results:
            icon = "✅" if r.passed else ("❌" if r.severity == "ERROR" else "⚠️")
            lines.append(f"  {icon} [{r.check_name}] {r.message}")
        return "\n".join(lines)


def check_completeness(
    curves: Dict[str, pd.DataFrame],
    expected_curves: Optional[Set[str]] = None,
) -> List[ValidationResult]:
    """
    Verifica completude dos dados:
    1. Todas as curvas esperadas estão presentes
    2. Cada curva tem o número mínimo de vértices
    """
    results = []

    if expected_curves is None:
        expected_curves = set(CURVE_MIN_VERTICES.keys())

    # Check 1: Presença de curvas
    available = set(curves.keys())
    missing = expected_curves - available

    results.append(ValidationResult(
        check_name="curves_present",
        passed=len(missing) == 0,
        message=(
            f"Todas as {len(expected_curves)} curvas presentes"
            if len(missing) == 0
            else f"Curvas faltantes: {missing}"
        ),
        severity="ERROR" if missing else "INFO",
        details={"expected": list(expected_curves), "missing": list(missing)},
    ))

    # Check 2: Número mínimo de vértices por curva
    for curve_name, df in curves.items():
        min_v = CURVE_MIN_VERTICES.get(curve_name, 5)
        n_vertices = len(df)
        passed = n_vertices >= min_v

        results.append(ValidationResult(
            check_name=f"min_vertices_{curve_name}",
            passed=passed,
            message=(
                f"{curve_name}: {n_vertices} vértices (mínimo: {min_v})"
            ),
            severity="WARNING" if not passed else "INFO",
            details={"curve": curve_name, "vertices": n_vertices, "minimum": min_v},
        ))

    return results


def check_continuity(
    existing_dates: List[date],
    start_date: date,
    end_date: date,
) -> List[ValidationResult]:
    """
    Verifica continuidade do histórico:
    Detecta dias úteis faltantes no intervalo.
    """
    expected = get_business_days(start_date, end_date)
    existing_set = set(existing_dates)
    missing = [d for d in expected if d not in existing_set]

    return [ValidationResult(
        check_name="continuity",
        passed=len(missing) == 0,
        message=(
            f"Histórico contínuo: {len(expected)} dias úteis sem lacunas"
            if len(missing) == 0
            else f"{len(missing)} dias úteis faltantes de {len(expected)} esperados"
        ),
        severity="WARNING" if missing else "INFO",
        details={
            "expected_days": len(expected),
            "existing_days": len(existing_dates),
            "missing_days": len(missing),
            "missing_dates": [d.isoformat() for d in missing[:20]],  # Limitar a 20
        },
    )]


def check_timeliness(
    curves: Dict[str, pd.DataFrame],
    expected_date: date,
) -> List[ValidationResult]:
    """
    Verifica atualização dos dados:
    A data de referência dos dados deve corresponder à data esperada.
    """
    results = []

    for curve_name, df in curves.items():
        if df.empty:
            continue

        dates_in_data = df["data_referencia"].unique()
        has_expected = expected_date in dates_in_data

        results.append(ValidationResult(
            check_name=f"timeliness_{curve_name}",
            passed=has_expected,
            message=(
                f"{curve_name}: dados para {expected_date} presentes"
                if has_expected
                else f"{curve_name}: data {expected_date} NÃO encontrada. Datas disponíveis: {list(dates_in_data)}"
            ),
            severity="ERROR" if not has_expected else "INFO",
            details={"curve": curve_name, "expected_date": str(expected_date)},
        ))

    return results


def check_consistency(
    curves: Dict[str, pd.DataFrame],
) -> List[ValidationResult]:
    """
    Verifica consistência entre curvas:
    Todas as curvas devem ter a mesma data de referência.
    """
    results = []

    dates_per_curve = {}
    for curve_name, df in curves.items():
        if not df.empty and "data_referencia" in df.columns:
            dates_per_curve[curve_name] = set(df["data_referencia"].unique())

    if len(dates_per_curve) < 2:
        results.append(ValidationResult(
            check_name="consistency_dates",
            passed=True,
            message="Menos de 2 curvas disponíveis, consistência não verificável",
            severity="INFO",
        ))
        return results

    # Todas as curvas devem compartilhar as mesmas datas
    all_dates = list(dates_per_curve.values())
    common_dates = all_dates[0]
    for d in all_dates[1:]:
        common_dates = common_dates.intersection(d)

    all_unique_dates = set()
    for d in all_dates:
        all_unique_dates = all_unique_dates.union(d)

    inconsistent = all_unique_dates - common_dates

    results.append(ValidationResult(
        check_name="consistency_dates",
        passed=len(inconsistent) == 0,
        message=(
            f"Todas as {len(dates_per_curve)} curvas consistentes nas datas"
            if len(inconsistent) == 0
            else f"Datas inconsistentes entre curvas: {inconsistent}"
        ),
        severity="WARNING" if inconsistent else "INFO",
        details={
            "curves": list(dates_per_curve.keys()),
            "common_dates": len(common_dates),
            "inconsistent_dates": [str(d) for d in inconsistent],
        },
    ))

    # Verificar se taxas estão em faixas razoáveis
    for curve_name, df in curves.items():
        if df.empty or "taxa_252" not in df.columns:
            continue

        taxa_col = "taxa_252" if "taxa_252" in df.columns else "taxa"
        min_taxa = df[taxa_col].min()
        max_taxa = df[taxa_col].max()

        # Taxas brasileiras tipicamente entre -5% e 50%
        reasonable = -5 <= min_taxa and max_taxa <= 100

        results.append(ValidationResult(
            check_name=f"consistency_range_{curve_name}",
            passed=reasonable,
            message=(
                f"{curve_name}: taxas entre {min_taxa:.4f}% e {max_taxa:.4f}%"
            ),
            severity="WARNING" if not reasonable else "INFO",
            details={
                "curve": curve_name,
                "min_taxa": float(min_taxa),
                "max_taxa": float(max_taxa),
            },
        ))

    return results


def validate(
    curves: Dict[str, pd.DataFrame],
    expected_date: date,
    existing_dates: Optional[List[date]] = None,
    history_start: Optional[date] = None,
) -> ValidationReport:
    """
    Executa todas as validações e retorna um relatório consolidado.

    Args:
        curves: Dicionário de DataFrames por curva
        expected_date: Data de referência esperada
        existing_dates: Lista de datas já existentes no histórico (para continuidade)
        history_start: Data de início do histórico (para continuidade)

    Returns:
        ValidationReport com todos os resultados
    """
    report = ValidationReport(
        reference_date=expected_date,
        timestamp=datetime.now(),
    )

    # 1. Completude
    report.results.extend(check_completeness(curves))

    # 2. Atualidade
    report.results.extend(check_timeliness(curves, expected_date))

    # 3. Consistência
    report.results.extend(check_consistency(curves))

    # 4. Continuidade (se houver dados históricos)
    if existing_dates and history_start:
        report.results.extend(
            check_continuity(existing_dates, history_start, expected_date)
        )

    logger.info(f"\n{report.summary()}")

    return report
