"""
Extrator de dados da B3.
Responsável pelo download e descompressão dos arquivos de taxas de swap.
"""

import logging
import os
import shutil
import struct
import tempfile
import time
import zipfile
from datetime import date
from pathlib import Path
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from plugins.b3_taxas.utils import build_download_url, build_filename, format_date_yymmdd

logger = logging.getLogger(__name__)

# Headers para evitar bloqueio pela B3
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/octet-stream, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/boletins-diarios/pesquisa-por-pregao/pesquisa-por-pregao/",
}

# Tamanho mínimo do arquivo para considerar válido (em bytes)
# Arquivos menores que isso geralmente indicam erro ou data sem dados
MIN_FILE_SIZE = 100


class B3DownloadError(Exception):
    """Erro no download de dados da B3."""
    pass


class B3EmptyFileError(Exception):
    """Arquivo da B3 está vazio ou não contém dados válidos."""
    pass


def _decompress_ex_(file_path: str, output_dir: str) -> Optional[str]:
    """
    Descomprime arquivo .ex_ da B3.

    O formato .ex_ da B3 pode ser:
    1. ZIP padrão
    2. MS-DOS compress (LZH)
    3. Arquivo texto puro (em alguns casos)

    Tenta múltiplas estratégias de descompressão.
    """
    # Estratégia 1: Tentar como ZIP
    try:
        with zipfile.ZipFile(file_path, 'r') as zf:
            zf.extractall(output_dir)
            extracted = zf.namelist()
            logger.info(f"Descomprimido como ZIP: {extracted}")
            
            # Se extraiu apenas 1 arquivo arquivado, podemos ter zip dentro de zip (típico da B3)
            # Extrair recursivamente para obter o txt final
            if len(extracted) == 1 and extracted[0].lower().endswith(('.ex_', '.zip')):
                inner_file = os.path.join(output_dir, extracted[0])
                logger.info(f"Arquivo extraído é {extracted[0]}, extraindo recursivamente...")
                return _decompress_ex_(inner_file, output_dir)
            
            # Retorna o primeiro arquivo .txt encontrado
            for name in extracted:
                if name.lower().endswith('.txt'):
                    return os.path.join(output_dir, name)
            # Se não tem .txt, retorna o primeiro arquivo
            if extracted:
                return os.path.join(output_dir, extracted[0])
    except zipfile.BadZipFile:
        logger.debug("Arquivo não é ZIP, tentando outras estratégias...")

    # Estratégia 2: Tentar com py7zr (7-Zip)
    try:
        import py7zr
        with py7zr.SevenZipFile(file_path, mode='r') as z:
            z.extractall(path=output_dir)
            extracted = z.getnames()
            logger.info(f"Descomprimido como 7z: {extracted}")
            for name in extracted:
                if name.lower().endswith('.txt'):
                    return os.path.join(output_dir, name)
            if extracted:
                return os.path.join(output_dir, extracted[0])
    except Exception:
        logger.debug("Arquivo não é 7z, tentando como MS-DOS compress...")

    # Estratégia 3: MS-DOS compress format
    # O formato .ex_ antigo da B3 usa MS-DOS COMPRESS.EXE (SZDD format)
    # Header: 0x53 0x5A 0x44 0x44 0x88 0xF0 0x27 0x33 ('SZDD\x88\xf0\x27\x33')
    try:
        with open(file_path, 'rb') as f:
            header = f.read(8)

        if header[:4] == b'SZDD':
            logger.info("Detectado formato MS-DOS COMPRESS (SZDD)")
            output_file = _decompress_szdd(file_path, output_dir)
            if output_file:
                return output_file
    except Exception as e:
        logger.debug(f"Falha na descompressão SZDD: {e}")

    # Estratégia 4: Pode ser um arquivo texto puro
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            content = f.read(200)
        # Se conseguir ler como texto e parecer ter conteúdo, é texto puro
        if len(content) > 50 and any(c.isdigit() for c in content):
            logger.info("Arquivo parece ser texto puro, copiando...")
            output_file = os.path.join(output_dir, build_filename(date.today()).replace('.ex_', '.txt'))
            shutil.copy2(file_path, output_file)
            return output_file
    except Exception:
        pass

    raise B3DownloadError(f"Não foi possível descomprimir o arquivo: {file_path}")


def _decompress_szdd(file_path: str, output_dir: str) -> Optional[str]:
    """
    Descomprime arquivo no formato MS-DOS COMPRESS (SZDD/KWAJ).
    O formato SZDD usa compressão LZ (Lempel-Ziv).
    """
    with open(file_path, 'rb') as f:
        magic = f.read(8)
        if magic[:4] != b'SZDD':
            return None

        # Byte 8: caractere que completa a extensão original
        # Ex: se o arquivo é .ex_ o caracter faltante seria algum char
        missing_char = chr(magic[7]) if magic[7] != 0 else ''

        # Byte 9-12: tamanho do arquivo descomprimido (little-endian)
        uncompressed_size_bytes = f.read(4)
        uncompressed_size = struct.unpack('<I', uncompressed_size_bytes)[0]

        # O resto do arquivo são os dados comprimidos (LZ77 variant)
        compressed_data = f.read()

    # Decodificar LZ
    output = bytearray()
    ring_buffer = bytearray(4096)
    ring_pos = 4096 - 16
    # Inicializar ring buffer com espaços
    for i in range(len(ring_buffer)):
        ring_buffer[i] = 0x20

    pos = 0
    while pos < len(compressed_data) and len(output) < uncompressed_size:
        # Ler byte de controle
        if pos >= len(compressed_data):
            break
        control = compressed_data[pos]
        pos += 1

        for bit in range(8):
            if pos >= len(compressed_data) or len(output) >= uncompressed_size:
                break

            if control & (1 << bit):
                # Literal byte
                byte = compressed_data[pos]
                pos += 1
                output.append(byte)
                ring_buffer[ring_pos % 4096] = byte
                ring_pos += 1
            else:
                # Referência (offset, length)
                if pos + 1 >= len(compressed_data):
                    break
                b1 = compressed_data[pos]
                b2 = compressed_data[pos + 1]
                pos += 2

                offset = b1 | ((b2 & 0xF0) << 4)
                length = (b2 & 0x0F) + 3

                for j in range(length):
                    if len(output) >= uncompressed_size:
                        break
                    byte = ring_buffer[(offset + j) % 4096]
                    output.append(byte)
                    ring_buffer[ring_pos % 4096] = byte
                    ring_pos += 1

    # Determinar nome do arquivo de saída
    base_name = os.path.basename(file_path)
    if base_name.endswith('.ex_'):
        out_name = base_name.replace('.ex_', '.txt')
    else:
        out_name = base_name + '.txt'

    output_file = os.path.join(output_dir, out_name)
    with open(output_file, 'wb') as f:
        f.write(bytes(output[:uncompressed_size]))

    logger.info(f"Descomprimido SZDD: {uncompressed_size} bytes -> {output_file}")
    return output_file


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    reraise=True,
)
def download_file(dt: date, output_dir: str, timeout: int = 60) -> str:
    """
    Faz download do arquivo de taxas de swap da B3 para uma data específica.

    Args:
        dt: Data de referência
        output_dir: Diretório para salvar o arquivo
        timeout: Timeout da requisição em segundos

    Returns:
        Path do arquivo baixado

    Raises:
        B3DownloadError: Se o download falhar
        B3EmptyFileError: Se o arquivo estiver vazio
    """
    url = build_download_url(dt)
    filename = build_filename(dt)
    filepath = os.path.join(output_dir, filename)

    logger.info(f"Baixando {url} -> {filepath}")

    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, stream=True)
        response.raise_for_status()
    except requests.HTTPError as e:
        if response.status_code == 404:
            raise B3EmptyFileError(
                f"Arquivo não encontrado para data {dt} (HTTP 404). "
                f"Pode ser feriado ou dia sem dados."
            )
        raise B3DownloadError(f"Erro HTTP {response.status_code} ao baixar {url}: {e}")
    except requests.RequestException as e:
        raise B3DownloadError(f"Erro ao baixar {url}: {e}")

    # Salvar arquivo
    os.makedirs(output_dir, exist_ok=True)
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    # Verificar tamanho
    file_size = os.path.getsize(filepath)
    if file_size < MIN_FILE_SIZE:
        raise B3EmptyFileError(
            f"Arquivo muito pequeno ({file_size} bytes) para data {dt}. "
            f"Provavelmente não há dados para esta data."
        )

    logger.info(f"Download concluído: {filepath} ({file_size} bytes)")
    return filepath


def extract(dt: date, raw_dir: str, temp_dir: Optional[str] = None) -> str:
    """
    Pipeline completo de extração: download + descompressão.

    Args:
        dt: Data de referência
        raw_dir: Diretório para salvar arquivos brutos
        temp_dir: Diretório temporário (opcional)

    Returns:
        Path do arquivo texto extraído
    """
    # Criar estrutura de diretórios: raw/YYYY/MM/DD/
    date_dir = os.path.join(
        raw_dir,
        str(dt.year),
        f"{dt.month:02d}",
        f"{dt.day:02d}"
    )
    os.makedirs(date_dir, exist_ok=True)

    # Download
    compressed_file = download_file(dt, date_dir)

    # Descompressão
    extract_dir = temp_dir or tempfile.mkdtemp(prefix="b3_taxas_")
    txt_file = _decompress_ex_(compressed_file, extract_dir)

    if txt_file is None:
        raise B3DownloadError(f"Falha ao extrair arquivo para data {dt}")

    # Mover arquivo texto para o diretório de dados brutos
    final_txt = os.path.join(date_dir, os.path.basename(txt_file))
    if txt_file != final_txt:
        shutil.move(txt_file, final_txt)

    logger.info(f"Extração concluída para {dt}: {final_txt}")
    return final_txt
