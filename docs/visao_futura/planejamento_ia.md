# Planejamento: Visão Futura com Inteligência Artificial (IA)

Este documento detalha o planejamento estratégico e a arquitetura sugerida para a implementação da camada cognitiva no pipeline de Taxas Referenciais B3. O objetivo é utilizar Large Language Models (LLMs) e Agentes Autônomos para aumentar a resiliência, a qualidade de dados e a facilidade de acesso às informações consolidadas.

---

## 1. Resolução Dinâmica de Layouts (Self-Healing Parser)

**Contexto:** A B3 muda os formatos de seus arquivos periodicamente (ex: de TSV para CSV, larguras fixas diferentes). Atualmente, isso quebra o pipeline.
**Objetivo:** Permitir que o pipeline se adapte automaticamente a mudanças de layout estruturais.

**Plano de Ação:**
- **Fase 1 (Detecção):** Modificar o bloco `try/except` do módulo `parser.py` para capturar exceções persistentes de *Format Error* ou *Parsing Error*.
- **Fase 2 (Agente Parser):** Construir um script auxiliar (ex: `auto_repair_agent.py`) que é acionado quando a Fase 1 falha.
- **Fase 3 (Integração LLM):** O script envia as primeiras 100 linhas do arquivo bruto, junto com a estrutura de saída esperada (DataFrame de 5 colunas), para a API de um LLM (GPT-4 / Claude / Gemini).
- **Fase 4 (Recuperação):** O LLM responde com um novo dicionário de mapeamento posicional ou o novo caracter delimitador detectado. O agente testa esse layout, e caso seja bem sugerido, salva localmente e re-injeta na DAG, garantindo que ela volte a rodar verde no dia seguinte.

---

## 2. Validador Semântico Múltiplas Fontes (Validator 2.0)

**Contexto:** Limites de Data Quality hardcoded (ex: taxas entre -5% e 100%) geram falsos positivos durantes crises econômicas fortes, exigindo análise braçal de um engenheiro para dar *bypass* na pipeline.
**Objetivo:** Adicionar contexto macroeconômico aos alertas.

**Plano de Ação:**
- **Fase 1 (Interceptação):** Quando o `validator.py` levanta um `WARNING` por "Salto de Taxa", o alerta não vai direto para o Slack/Email. Ele vai para um Tópico (ex: Kafka/SNS) ou uma fila de validação secundária.
- **Fase 2 (Agente Jornalista):** Um agente autônomo é triggado pelo alerta. Ele consulta APIs de notícias econômicas gratuitas ou provedores como Bloomberg/Reuters buscando acontecimentos nas datas relacionadas ao salto.
- **Fase 3 (Parecer):** O agente faz uma correlação semântica ("O salto de 15% nas taxas futuras reflete a ata do Copom").
- **Fase 4 (Notificação Inteligente):** O Slack recebe o alerta acompanhado do parecer do Agente: *"Anomalia detectada justificada pelo cenário de mercado"*. Opção de supressão automática configurável.

---

## 3. Assistente Text-to-SQL (Chatbot para as Mesas)

**Contexto:** As Mesas de Operação da Polígono não sabem usar Parquet/Python nativamente para extrair comparativos históricos pesados. Elaboram tickets para engenharia.
**Objetivo:** Democratizar o acesso e a velocidade de cruzamento das safras de crédito.

**Plano de Ação:**
- **Fase 1 (Infra de Query):** Conectar os arquivos `published/*.parquet` em um motor como AWS Athena, DuckDB ou Trino.
- **Fase 2 (Schema Context):** Mapear via prompt o catálogo de dados, traduzindo o linguajar ("Fator de desconto PRE de 2 anos") para os campos corretos (`curva='DI x Pré'`, `dias_uteis=504`, etc).
- **Fase 3 (Interface LlamaIndex/LangChain):** Criar uma UI simples (Gradio, Streamlit ou Bot de Slack) onde analistas fazem as perguntas.
- **Fase 4 (Validação):** Implementar lógicas de segurança (guardrails) de forma que o LLM crie queries *read-only* seguras e plotagens automáticas de matplotlib.

---

## 4. Resumo Inteligente de Alertas do Airflow

**Contexto:** Engenharia perde muito tempo lendo stacktraces brutos do Airflow em notificações quando ocorrem problemas bobos de conexão.
**Objetivo:** Transformar as notificações em ações corretivas imediatas.

**Plano de Ação:**
- **Fase 1 (Callback Hook):** Usar o `on_failure_callback` do Airflow nas definições de Tasks para interceptar os erros.
- **Fase 2 (Context Extraction):** Capturar os últimos 50 logs de traceback e a task correspondente.
- **Fase 3 (LLM Prompting):** Enviar via API pedindo para resumir o problema e elencar a provável causa raiz e solução recomendada.
- **Fase 4 (Routing):** Enviar o output resumido (markdown) para o webhook do Slack, encurtando o tempo de resolução (MTTR) da equipe de sustentação.

---

## 5. Data Catalog Automatizado

**Contexto:** Sistemas de dados tendem a virar legados escuros porque novos desenvolvedores alteram as transformações e esquecem de atualizar a doc `documentacao_tecnica.md`.
**Objetivo:** Transformar o repositório em documentação viva (Living Documentation).

**Plano de Ação:**
- **Fase 1 (CI/CD Hook):** Configurar no Github Actions um workflow que dispara toda vez que um PR impactar as pastas `dags/` ou `plugins/`.
- **Fase 2 (Diff Analysis):** Agente captura o PULL REQUEST DIFF e analisa se algo lógico foi alterado nas regras de negócio de `transformer.py` ou `storage.py`.
- **Fase 3 (Update):** Agente comita uma sugestão atualizando diretamente o markdown técnico ou os próprios docstrings Python usando AI (via Github Copilot PR Helper ou GitHub Action de LLM).
