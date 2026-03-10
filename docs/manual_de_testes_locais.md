# Manual de Execução: Testes do Pipeline B3

Este manual descreve o passo a passo para você executar a extração de dados reais no seu computador, tanto para um único dia específico quanto para a simulação de carga histórica (Backfill) rodando no terminal.

> **Pré-requisito:** Certifique-se de estar na pasta raiz do projeto (`C:\seu\caminho\para\plgn`) e com o seu ambiente virtual Python ativado (caso utilize um). Garanta que os pacotes do `requirements.txt` estão instalados.

---

## 🟢 Teste 1: Execução para 1 Único Dia (Single-Date)

Este teste aciona o pipeline de ponta a ponta para baixar, transformar, validar e salvar os dados de um dia específico que você escolher. Ele é excelente para auditar a qualidade de um dia específico sem onerar as APIs.

### Passo a Passo:

1. Abra o Terminal (PowerShell, Git Bash ou CMD) dentro do VS Code.
2. Certifique-se de estar na pasta `plgn`:
   ```bash
   cd C:\seu\caminho\para\plgn
   ```
3. Execute o script `run_pipeline.py` com a flag `--date` indicando a data alvo (no formato `YYYY-MM-DD`):
   ```bash
   python run_pipeline.py --date 2026-03-05
   ```

### O que esperar:
O script imprimirá um log detalhado de cada uma das 5 etapas no seu terminal:
- `[1/5] Extraindo dados da B3...` (Onde ele baixa e descomprime os 3 formatos de arquivos misturados).
- `[2/5] Fazendo parsing...`
- `[3/5] Transformando dados...`
- `[4/5] Validando qualidade...` (Vai exibir o nosso *Data Quality Check*, avisando se alguma taxa ultrapassa 100% ao ano, por exemplo).
- `[5/5] Salvando dados...`

No final, você receberá a mensagem de "✅ Data processada com sucesso". Os dados estarão no formato Parquet aguardando você na pasta: `data/published/`.

---

## 🟣 Teste 2: Execução de Carga Histórica (Backfill Local)

O Backfill no Airflow original faz batchings e gaps detection usando a estrutura complexa de banco de dados do Airflow. Nós criamos no mesmo `run_pipeline.py` um modo de **Backfill de Desenvolvimento** para rodar no terminal, simulando a nuvem.

### Passo a Passo:

1. No mesmo terminal da pasta `plgn`, execute o comando usando a flag `--backfill`:
   ```bash
   python run_pipeline.py --backfill --start-date 2026-03-01 --end-date 2026-03-06 --delay 3
   ```
   > Parâmetros importantes:
   > * `--start-date`: Data inicial do seu histórico.
   > * `--end-date`: Data final (opcional, por padrão usa 'hoje').
   > * `--delay`: O tempo em segundos que o Python vai *"dormir"* entre o download das planilhas. Mantive 3 segundos de padrão como segurança, para que o firewall da B3 não bloqueie o seu IP por requisições muito agressivas.

### O que esperar:
O script irá primeiro descobrir quantos dias úteis (desconsiderando finais de semana e feriados) existem na sua janela entre `--start-date` e `--end-date`. Em seguida, para cada dia útil:
- Ele varre todo o processo ETL 1 ao 5 descrito acima.
- O validador se torna "Leniente" e apenas joga alertas amarelos (`WARNING`) em vez de interromper o programa, visto que arquivos históricos muito antigos da B3 sempre correm risco de terem falhas humanas e não devem parar 10 anos de carregamento.
- Todos os arquivos Parquet serão enfileirados fazendo *Upsert* progressivo e seguro na pasta `data/published`.

📝 **Dica:** Se quiser testar integridade massiva, jogue `--start-date 2016-01-01` e deixe a tela rodando (esteja avisado que baixar os 10 anos vai levar cerca de ~2.500 arquivos de 3s de delay da B3, durando aproximadamente algumas horas localmente!).
