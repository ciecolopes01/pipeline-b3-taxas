"""
Testes para o módulo extractor.
"""

import os
import tempfile
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from plugins.b3_taxas.extractor import (
    B3DownloadError,
    B3EmptyFileError,
    download_file,
    extract,
    _decompress_ex_,
)
from plugins.b3_taxas.utils import build_download_url, build_filename


class TestBuildFilename:
    def test_format_2026(self):
        assert build_filename(date(2026, 3, 6)) == "TS260306.ex_"

    def test_format_2016(self):
        assert build_filename(date(2016, 1, 4)) == "TS160104.ex_"

    def test_format_year_2000(self):
        assert build_filename(date(2000, 12, 29)) == "TS001229.ex_"


class TestBuildUrl:
    def test_url_format(self):
        url = build_download_url(date(2026, 3, 6))
        expected = "https://www.b3.com.br/pesquisapregao/download?filelist=TS260306.ex_"
        assert url == expected


class TestDownloadFile:
    @patch("plugins.b3_taxas.extractor.requests.get")
    def test_download_success(self, mock_get):
        """Testa download bem-sucedido."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b"x" * 200]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = download_file(date(2026, 3, 6), tmpdir)
            assert os.path.exists(filepath)
            assert filepath.endswith("TS260306.ex_")

    @patch("plugins.b3_taxas.extractor.requests.get")
    def test_download_empty_file(self, mock_get):
        """Testa comportamento com arquivo muito pequeno."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b"tiny"]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(B3EmptyFileError):
                download_file(date(2026, 3, 7), tmpdir)  # Dia sem dados

    @patch("plugins.b3_taxas.extractor.requests.get")
    def test_download_http_error(self, mock_get):
        """Testa comportamento com erro HTTP."""
        import requests
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.HTTPError("Server Error")
        mock_get.return_value = mock_response

        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(B3DownloadError):
                download_file(date(2026, 3, 6), tmpdir)


class TestDecompressEx:
    def test_decompress_text_file(self):
        """Testa descompressão de arquivo texto puro."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Criar um arquivo de teste que parece texto
            test_file = os.path.join(tmpdir, "TS260306.ex_")
            content = "1;06/03/2026;PRE;30;21;14.250000\n" * 10
            with open(test_file, 'w', encoding='latin-1') as f:
                f.write(content)

            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            result = _decompress_ex_(test_file, output_dir)
            assert result is not None
            assert os.path.exists(result)


class TestExtractIntegration:
    """Testes de integração (requerem acesso à B3)."""

    @pytest.mark.skipif(
        not os.environ.get("RUN_INTEGRATION_TESTS"),
        reason="Testes de integração desabilitados. "
               "Defina RUN_INTEGRATION_TESTS=1 para habilitar."
    )
    def test_real_download(self):
        """Testa download real de um arquivo da B3."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = os.path.join(tmpdir, "raw")
            # Usar data antiga que sabemos ter dados
            txt_file = extract(date(2025, 3, 3), raw_dir)
            assert os.path.exists(txt_file)
            assert os.path.getsize(txt_file) > 100
