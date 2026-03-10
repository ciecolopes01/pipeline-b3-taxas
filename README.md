# Pipeline de Taxas Referenciais B3

Pipeline de dados em Apache Airflow para coleta, processamento e disponibilização de curvas de taxas referenciais publicadas pela B3.

## Curvas Suportadas

| Curva | Código | Descrição |
|-------|--------|-----------|
| DI x Pré | PRE | Curva de juros prefixada derivada dos contratos de swap DI x Pré |
| Ajuste Pré | APR | Taxas de ajuste para contratos prefixados |
| DI x TR | TR | Curva de juros indexada à Taxa Referencial |

## Estrutura do Projeto

```
plgn/
├── dags/                              # DAGs do Airflow
│   ├── dag_taxas_referenciais_b3.py   # Execução diária (20h, seg-sex)
│   └── dag_backfill_taxas_referenciais_b3.py  # Backfill histórico
├── plugins/b3_taxas/                  # Módulos de processamento
│   ├── extractor.py                   # Download e descompressão
│   ├── parser.py                      # Parsing multi-formato
│   ├── transformer.py                 # Separação por curva e cálculos
│   ├── validator.py                   # Validação de qualidade
│   ├── storage.py                     # Persistência (Parquet/CSV)
│   └── utils.py                       # Utilitários
├── tests/                             # Testes unitários
├── docs/                              # Documentação técnica
├── data/                              # Dados (raw/processed/published)
└── requirements.txt
```

## Setup

```bash
# Instalar dependências
pip install -r requirements.txt

# Executar testes
python -m pytest tests/ -v
```

## DAGs

### `taxas_referenciais_b3` (Diária)
- **Schedule**: `0 20 * * 1-5` (20h, seg-sex)
- **Fluxo**: `check_business_day → extract → parse → transform → validate → store`

### `backfill_taxas_referenciais_b3` (Manual)
- **Trigger**: manual com parâmetros
- **Parâmetros**: `start_date`, `end_date`, `batch_size`, `delay_seconds`

```json
{
  "start_date": "2016-01-01",
  "end_date": "2026-03-10",
  "batch_size": 10,
  "delay_seconds": 3
}
```

## Fonte de Dados

Arquivo `TS{YYMMDD}.ex_` da B3 (Mercado de Derivativos - Taxas de Mercado para Swaps):
```
https://www.b3.com.br/pesquisapregao/download?filelist=TS{YYMMDD}.ex_
```

## Documentação

- [Documentação Técnica](docs/documentacao_tecnica.md) — Arquitetura, modelagem, backfill, qualidade e trade-offs

## Próximos Passos (Visão Futura com Inteligência Artificial)

Para evoluir a resiliência e a usabilidade do pipeline, os seguintes casos de uso de IA (LLMs) estão mapeados:

1. **Parser Auto-Reparável:** Módulo que detecta falhas nas mudanças de formato da B3 e usa LLMs para gerar dinamicamente o novo dicionário/layout posicional de extração.
2. **Validador Semântico (Agente de Qualidade):** Interceptação de alertas do Data Quality por um Agente IA que cruza saltos nas taxas com notícias de mercado, silenciando falsos positivos.
3. **Assistente Text-to-SQL/Pandas:** Chatbot integrado às tabelas Parquet publicadas, permitindo que as mesas de operação cruzem safras de crédito usando linguagem natural.
4. **Resumo Inteligente de Incidentes:** Webhook no Airflow que envia logs de erro resumidos no Slack com sugestões acionáveis (ex: "Aumentar timer de backoff devido a rate-limit").
5. **Data Catalog Automatizado:** Agente em CI/CD que mantém a documentação atualizada conforme novas lógicas de curva são adicionadas na transformação.
