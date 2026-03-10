"""
Testes para o módulo transformer.
"""

import math
from datetime import date

import pandas as pd
import pytest

from plugins.b3_taxas.transformer import (
    filter_curve,
    enrich_curve,
    transform,
    _calculate_discount_factor,
    _calculate_rate_360,
)


# ─── Dados de teste ──────────────────────────────────────────────────────────

def _make_sample_df():
    """Cria DataFrame de amostra para testes."""
    return pd.DataFrame([
        {"data_referencia": date(2026, 3, 6), "curva_id": "PRE", "curva_nome": "DI x Pré",
         "dias_corridos": 30, "dias_uteis": 21, "taxa": 14.25},
        {"data_referencia": date(2026, 3, 6), "curva_id": "PRE", "curva_nome": "DI x Pré",
         "dias_corridos": 60, "dias_uteis": 42, "taxa": 14.35},
        {"data_referencia": date(2026, 3, 6), "curva_id": "PRE", "curva_nome": "DI x Pré",
         "dias_corridos": 90, "dias_uteis": 63, "taxa": 14.45},
        {"data_referencia": date(2026, 3, 6), "curva_id": "APR", "curva_nome": "Ajuste Pré",
         "dias_corridos": 30, "dias_uteis": 21, "taxa": 13.80},
        {"data_referencia": date(2026, 3, 6), "curva_id": "APR", "curva_nome": "Ajuste Pré",
         "dias_corridos": 60, "dias_uteis": 42, "taxa": 13.90},
        {"data_referencia": date(2026, 3, 6), "curva_id": "TR", "curva_nome": "DI x TR",
         "dias_corridos": 30, "dias_uteis": 21, "taxa": 0.15},
        {"data_referencia": date(2026, 3, 6), "curva_id": "TR", "curva_nome": "DI x TR",
         "dias_corridos": 60, "dias_uteis": 42, "taxa": 0.25},
    ])


class TestCalculateDiscountFactor:
    def test_basic_calculation(self):
        """FD = 1 / (1 + 14.25/100) ^ (21/252)"""
        fd = _calculate_discount_factor(14.25, 21)
        expected = 1.0 / math.pow(1.1425, 21 / 252.0)
        assert abs(fd - expected) < 1e-10

    def test_zero_taxa(self):
        assert _calculate_discount_factor(0, 21) == 1.0

    def test_zero_dias(self):
        assert _calculate_discount_factor(14.25, 0) == 1.0


class TestCalculateRate360:
    def test_conversion(self):
        """Taxa_360 = ((1 + 14.25/100)^(360/252) - 1) * 100"""
        r360 = _calculate_rate_360(14.25)
        expected = (math.pow(1.1425, 360.0 / 252.0) - 1.0) * 100
        assert abs(r360 - expected) < 1e-10

    def test_zero_taxa(self):
        assert _calculate_rate_360(0) == 0.0


class TestFilterCurve:
    def test_filter_pre(self):
        df = _make_sample_df()
        result = filter_curve(df, "DI x Pré")
        assert len(result) == 3
        assert all(result["curva_nome"] == "DI x Pré")

    def test_filter_apr(self):
        df = _make_sample_df()
        result = filter_curve(df, "Ajuste Pré")
        assert len(result) == 2

    def test_filter_tr(self):
        df = _make_sample_df()
        result = filter_curve(df, "DI x TR")
        assert len(result) == 2

    def test_filter_unknown(self):
        df = _make_sample_df()
        result = filter_curve(df, "Inexistente")
        assert result.empty


class TestEnrichCurve:
    def test_adds_taxa_360(self):
        df = _make_sample_df()
        pre = filter_curve(df, "DI x Pré")
        enriched = enrich_curve(pre)
        assert "taxa_360" in enriched.columns
        # Taxa 360 deve ser maior que taxa 252
        assert all(enriched["taxa_360"] > enriched["taxa_252"])

    def test_adds_discount_factor(self):
        df = _make_sample_df()
        pre = filter_curve(df, "DI x Pré")
        enriched = enrich_curve(pre)
        assert "fator_desconto" in enriched.columns
        # Fator de desconto deve estar entre 0 e 1
        assert all(enriched["fator_desconto"] > 0)
        assert all(enriched["fator_desconto"] < 1)

    def test_adds_timestamp(self):
        df = _make_sample_df()
        pre = filter_curve(df, "DI x Pré")
        enriched = enrich_curve(pre)
        assert "data_processamento" in enriched.columns


class TestTransform:
    def test_returns_all_curves(self):
        df = _make_sample_df()
        curves = transform(df)
        assert "DI x Pré" in curves
        assert "Ajuste Pré" in curves
        assert "DI x TR" in curves

    def test_curves_are_enriched(self):
        df = _make_sample_df()
        curves = transform(df)
        for name, curve_df in curves.items():
            assert "taxa_252" in curve_df.columns
            assert "taxa_360" in curve_df.columns
            assert "fator_desconto" in curve_df.columns

    def test_empty_df(self):
        curves = transform(pd.DataFrame())
        assert curves == {}

    def test_curves_sorted_by_dias_uteis(self):
        df = _make_sample_df()
        curves = transform(df)
        for name, curve_df in curves.items():
            dias = curve_df["dias_uteis"].tolist()
            assert dias == sorted(dias)
