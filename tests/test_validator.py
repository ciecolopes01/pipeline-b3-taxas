"""
Testes para o módulo validator.
"""

from datetime import date

import pandas as pd
import pytest

from plugins.b3_taxas.validator import (
    check_completeness,
    check_continuity,
    check_timeliness,
    check_consistency,
    validate,
)


def _make_curves():
    """Cria dicionário de curvas de teste."""
    return {
        "DI x Pré": pd.DataFrame([
            {"data_referencia": date(2026, 3, 6), "dias_uteis": i, "taxa_252": 14.0 + i * 0.1}
            for i in range(1, 31)
        ]),
        "Ajuste Pré": pd.DataFrame([
            {"data_referencia": date(2026, 3, 6), "dias_uteis": i, "taxa_252": 13.0 + i * 0.1}
            for i in range(1, 11)
        ]),
        "DI x TR": pd.DataFrame([
            {"data_referencia": date(2026, 3, 6), "dias_uteis": i, "taxa_252": 0.1 + i * 0.01}
            for i in range(1, 11)
        ]),
    }


class TestCheckCompleteness:
    def test_all_curves_present(self):
        curves = _make_curves()
        results = check_completeness(curves)
        curve_check = [r for r in results if r.check_name == "curves_present"][0]
        assert curve_check.passed

    def test_missing_curve(self):
        curves = _make_curves()
        del curves["DI x TR"]
        results = check_completeness(curves)
        curve_check = [r for r in results if r.check_name == "curves_present"][0]
        assert not curve_check.passed
        assert "DI x TR" in str(curve_check.details["missing"])

    def test_min_vertices_ok(self):
        curves = _make_curves()
        results = check_completeness(curves)
        pre_check = [r for r in results if "DI x Pré" in r.check_name][0]
        assert pre_check.passed

    def test_min_vertices_fail(self):
        curves = _make_curves()
        curves["DI x Pré"] = curves["DI x Pré"].head(3)  # Apenas 3 vértices
        results = check_completeness(curves)
        pre_check = [r for r in results if "DI x Pré" in r.check_name][0]
        assert not pre_check.passed


class TestCheckContinuity:
    def test_no_gaps(self):
        # 5 dias úteis da semana de 3-7 Mar 2026
        dates = [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4),
                 date(2026, 3, 5), date(2026, 3, 6)]
        results = check_continuity(dates, date(2026, 3, 2), date(2026, 3, 6))
        assert results[0].passed

    def test_with_gap(self):
        # Faltando 4 Mar
        dates = [date(2026, 3, 2), date(2026, 3, 3),
                 date(2026, 3, 5), date(2026, 3, 6)]
        results = check_continuity(dates, date(2026, 3, 2), date(2026, 3, 6))
        assert not results[0].passed
        assert results[0].details["missing_days"] >= 1


class TestCheckTimeliness:
    def test_date_present(self):
        curves = _make_curves()
        results = check_timeliness(curves, date(2026, 3, 6))
        assert all(r.passed for r in results)

    def test_date_missing(self):
        curves = _make_curves()
        results = check_timeliness(curves, date(2026, 3, 7))
        assert any(not r.passed for r in results)


class TestCheckConsistency:
    def test_consistent_dates(self):
        curves = _make_curves()
        results = check_consistency(curves)
        date_check = [r for r in results if r.check_name == "consistency_dates"][0]
        assert date_check.passed

    def test_consistent_ranges(self):
        curves = _make_curves()
        results = check_consistency(curves)
        range_checks = [r for r in results if "range" in r.check_name]
        assert all(r.passed for r in range_checks)

    def test_unreasonable_rates(self):
        curves = _make_curves()
        # Taxa de 200% é fora da faixa razoável
        curves["DI x Pré"]["taxa_252"] = 200.0
        results = check_consistency(curves)
        range_check = [r for r in results if "range_DI x Pré" in r.check_name][0]
        assert not range_check.passed


class TestValidate:
    def test_full_validation_pass(self):
        curves = _make_curves()
        report = validate(curves, expected_date=date(2026, 3, 6))
        assert report.passed

    def test_full_validation_with_issues(self):
        curves = _make_curves()
        del curves["DI x TR"]
        report = validate(curves, expected_date=date(2026, 3, 6))
        assert not report.passed

    def test_report_summary(self):
        curves = _make_curves()
        report = validate(curves, expected_date=date(2026, 3, 6))
        summary = report.summary()
        assert "PASSED" in summary or "FAILED" in summary
