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
- [Manual de Testes Locais](docs/manual_de_testes_locais.md) — Passo a passo para executar testes de Extração Única e de Carga Histórica (Backfill) no próprio terminal.

## Próximos Passos (Visão Futura com Inteligência Artificial)

Foi elaborado um plano estratégico detalhado de como integrar IA (LLMs e Agentes Autônomos) para tornar a arquitetura cognitiva, robusta e escalável para os usuários analíticos da empresa. 

👉 **[Ver Planejamento Detalhado: Visão Futura de IA](docs/visao_futura/planejamento_ia.md)**

Casos mapeados:
1. **Parser Auto-Reparável:** Módulo que detecta falhas e regenera mapeamentos.
2. **Validador Semântico:** Interceptação inteligente de alertas que cruza com notícias.
3. **Assistente Text-to-SQL/Pandas:** Chatbot de cruzamento de safras em linguagem natural.
4. **Resumo Inteligente de Incidentes:** Alertas de DAGs traduzidos por LLM.
5. **Data Catalog Automatizado:** CI/CD Agents que criam e atualizam documentação técnica a cada Pull Request.
