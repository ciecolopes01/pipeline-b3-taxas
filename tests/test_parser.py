"""
Testes para o módulo parser.
"""

import os
import tempfile
from datetime import date

import pandas as pd
import pytest

from plugins.b3_taxas.parser import (
    parse_file,
    _detect_format,
    _parse_csv_semicolon,
    _parse_fixed_width,
    CURVE_MAP,
)


# ─── Dados de teste ──────────────────────────────────────────────────────────

SAMPLE_CSV_SEMICOLON = """06/03/2026;PRE;30;21;14.250000
06/03/2026;PRE;60;42;14.350000
06/03/2026;PRE;90;63;14.450000
06/03/2026;PRE;120;84;14.550000
06/03/2026;PRE;180;126;14.650000
06/03/2026;APR;30;21;13.800000
06/03/2026;APR;60;42;13.900000
06/03/2026;TR;30;21;0.150000
06/03/2026;TR;60;42;0.250000
"""

SAMPLE_FIXED_WIDTH = """0060320260                                                  
1060320260PRE  00030000211425000000
1060320260PRE  00060000421435000000
1060320260PRE  00090000631445000000
1060320260APR  00030000211380000000
1060320260TR   00030000210015000000
9060320260                                                  
"""


class TestDetectFormat:
    def test_csv_semicolon(self):
        assert _detect_format("a;b;c;d\n1;2;3;4") == "csv_semicolon"

    def test_fixed_width(self):
        line = "1" + "0" * 60
        assert _detect_format(line) == "fixed_width"

    def test_csv_tab(self):
        assert _detect_format("a\tb\tc\td\n1\t2\t3\t4") == "csv_tab"

    def test_csv_pipe(self):
        assert _detect_format("a|b|c|d\n1|2|3|4") == "csv_pipe"


class TestParseCsvSemicolon:
    def test_parse_basic(self):
        df = _parse_csv_semicolon(SAMPLE_CSV_SEMICOLON)
        assert len(df) > 0
        assert "data_referencia" in df.columns
        assert "curva_id" in df.columns
        assert "taxa" in df.columns

    def test_pre_records(self):
        df = _parse_csv_semicolon(SAMPLE_CSV_SEMICOLON)
        pre = df[df["curva_nome"] == "DI x Pré"]
        assert len(pre) >= 5

    def test_multiple_curves(self):
        df = _parse_csv_semicolon(SAMPLE_CSV_SEMICOLON)
        curves = df["curva_nome"].unique()
        assert "DI x Pré" in curves
        assert "Ajuste Pré" in curves
        assert "DI x TR" in curves


class TestParseFile:
    def test_parse_csv_file(self):
        """Testa parsing de arquivo CSV real."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(SAMPLE_CSV_SEMICOLON)
            f.flush()
            filepath = f.name

        try:
            df = parse_file(filepath, reference_date=date(2026, 3, 6))
            assert not df.empty
            assert "data_referencia" in df.columns
            assert "taxa" in df.columns
        finally:
            os.unlink(filepath)

    def test_parse_empty_file(self):
        """Testa parsing de arquivo vazio."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False
        ) as f:
            f.write("")
            f.flush()
            filepath = f.name

        try:
            df = parse_file(filepath)
            assert df.empty
        finally:
            os.unlink(filepath)

    def test_parse_maintains_types(self):
        """Verifica se os tipos de dados são corretos após parsing."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(SAMPLE_CSV_SEMICOLON)
            f.flush()
            filepath = f.name

        try:
            df = parse_file(filepath, reference_date=date(2026, 3, 6))
            if not df.empty:
                assert df["dias_corridos"].dtype in ('int64', 'int32')
                assert df["dias_uteis"].dtype in ('int64', 'int32')
                assert df["taxa"].dtype == 'float64'
        finally:
            os.unlink(filepath)


class TestCurveMap:
    def test_pre_aliases(self):
        """Verifica que aliases de DI x Pré estão mapeados."""
        for key in ["PRE", "DIC", "TP"]:
            assert key in CURVE_MAP
            assert CURVE_MAP[key] == "DI x Pré"

    def test_tr_aliases(self):
        """Verifica que aliases de DI x TR estão mapeados."""
        for key in ["TR", "DTR"]:
            assert key in CURVE_MAP
            assert CURVE_MAP[key] == "DI x TR"

    def test_apr_aliases(self):
        """Verifica que aliases de Ajuste Pré estão mapeados."""
        for key in ["APR", "DAP", "DOC"]:
            assert key in CURVE_MAP
            assert CURVE_MAP[key] == "Ajuste Pré"
