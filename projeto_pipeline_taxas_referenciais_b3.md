# Case Técnico

### Pipeline de Taxas Referenciais B3

## Contexto

As taxas referenciais publicadas pela B3 são ferramentas fundamentais para precificação de ativos, gestão de risco e análise macroeconômica. No mercado brasileiro, a curva de juros derivada dos contratos de swap DI x Pré é uma das principais referências para o mercado de renda fixa.

Na Polígono, avaliamos continuamente safras de crédito comparando as taxas atuais com as taxas vigentes no momento da originação de cada operação. Para viabilizar essa análise, precisamos manter um histórico longo e confiável dessas curvas.

Fonte de dados:

https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/mercado-de-derivativos/precos-referenciais/taxas-referenciais-bm-fbovespa/

## Objetivo

Projetar e implementar pipelines de dados capazes de coletar, processar e disponibilizar as seguintes curvas de taxas referenciais:

- DI x Pré
- Ajuste Pré
- DI x TR

## Requisitos

### Funcionais

1. Os pipelines devem ser implementados em **Apache Airflow**
2. A execução deve ser agendada para **dias úteis às 20h** (horário de Brasília)
3. O sistema deve suportar **backfill de até 10 anos** com custo operacional viável

### Não-funcionais

1. **Completude**: garantia de que não há lacunas no histórico
2. **Continuidade**: detecção e tratamento de dias faltantes
3. **Atualização**: dados do dia disponíveis de forma tempestiva
4. **Consistência**: alinhamento entre as diferentes curvas para uma mesma data-base

### Restrição de custo


Para referência, o custo de processamento em AWS Glue é de **US$ 0,44 por DPU-hora**. O desenho da solução deve considerar essa restrição ao dimensionar o backfill histórico.

## Entregáveis

1. Código funcional dos DAGs de Airflow e scripts de extração/transformação
2. Documentação técnica descrevendo:
- Arquitetura da solução
- Modelagem de dados: quantas tabelas serão criadas, quais serão publicadas para consumo, e a granularidade de cada uma
- Estratégia de backfill e estimativa de custo
- Mecanismos de garantia de qualidade dos dados (completude, continuidade, consistência)
- Trade-offs considerados e decisões de design
