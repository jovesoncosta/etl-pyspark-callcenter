# Pipeline de Analise/Engenharia de Dados: Análise de Call Center e Vendas

Este repositório contém o desenvolvimento de um pipeline de Extração, Transformação e Carga (ETL) em PySpark, concebido para processar dados de atendimento e faturamento de um Call Center. O objetivo central é transformar dados transacionais brutos (CSV) num Data Lake otimizado (Parquet), garantindo a integridade financeira e a qualidade da informação.

## 🏗️ Arquitetura e Tecnologias
* **Ambiente Nativo:** macOS / Linux (Baseado em Unix)
* **Linguagem:** Python 3.9+
* **Framework de Processamento:** Apache Spark (PySpark 3.5.0)
* **Testes Automatizados:** Pytest
* **Armazenamento de Destino:** Arquivos colunares `.parquet`
* **Estratégia de Particionamento:** Particionamento dinâmico por Liderança da Equipe, otimizado para evitar o problema de *Small Files* (1 ficheiro por partição).

## 📂 > **Estrutura de Diretórios**
> ```text
> teste_engenharia_dados/
> │
> ├── data/                        # Dados locais (Ignorados no Git)
> │   ├── raw/                     # CSVs de origem (Pessoas, Telefonia, Avaliações)
> │   └── output/vendas_diarias/   # Data Lake (Tabelas Parquet particionadas)
> │
> ├── src/                         # Pipeline de Dados
> │   ├── config.py                # Tipagem forte (Schemas) e rotas
> │   ├── spark_utils.py           # Sessão do Spark e Logs
> │   ├── analise_exploratoria.py  # Descobertas de negócio e Data Quality
> │   ├── vendas_diarias.py        # ETL Principal (Geração do Parquet)
> │   └── auditar_lideres.py       # Conciliação financeira automatizada
> │
> └── tests/                       # Módulo de Qualidade de Código
>     └── test_transformations.py  # Testes unitários do ETL em memória
> ```

## Qualidade de Dados (Data Quality) e Decisões de Engenharia
Durante a fase de *Data Profiling*, a abordagem analítica revelou anomalias críticas nas bases de origem. Foram implementadas as seguintes soluções no pipeline para garantir a resiliência dos dados:

1. **Correção de Anomalias Temporais (Viagens no Tempo):**
   * *Problema:* Registros no sistema de telefonia continham datas de término anteriores às datas de início (ex: início em 2025, fim em 2022), gerando durações negativas na ordem dos milhões de minutos.
   * *Solução:* Implementação de um filtro de qualidade rigoroso (`duracao_min >= 0`) na Análise Exploratória, impedindo a distorção da métrica de Tempo Médio Geral da empresa (corrigido para o valor real de ~9.92 minutos).

2. **Tratamento de Nulos em Chaves de Particionamento (O "Líder Fantasma"):**
   * *Problema:* O cargo de Gerência Geral não possui um Líder de Equipe cadastrado no RH (valor nulo). No Spark, particionar dinamicamente por uma coluna com valores nulos pode gerar falhas de gravação ou diretórios corrompidos no Data Lake.
   * *Solução:* Injeção do método `.fillna("Sem_Lider_Informado")` no script de ETL, garantindo a integridade estrutural das pastas Parquet.

3. **Mapeamento de Colisão de Chaves Primárias:**
   * *Problema:* A base de avaliações reciclou o `ID_Monitoria` (ex: ID 448180) para diferentes usuários e datas.
   * *Ação:* A inconsistência foi mapeada e isolada para report

## Validação Financeira e Auditoria
O projeto não termina na execução do ETL. Para atestar a precisão das transformações, foi construído um módulo de auditoria (`auditar_lideres.py`) que realiza o teste de conciliação financeira ("Prova Real").

O teste cruza os valores brutos esperados nos CSVs com os valores efetivamente gravados no Data Lake, validando partição por partição. 

---
## Como Executar o Projeto (macOS / Linux)

O projeto foi construído para rodar nativamente em terminais Unix, sem necessidade de emuladores ou configurações complexas de caminhos.

### 1. Pré-requisitos
Certifique-se de ter o **Java 11** e o **Python 3** instalados na sua máquina.
* No macOS (via Homebrew): `brew install openjdk@11 python`
* No Linux (Ubuntu/Debian): `sudo apt install openjdk-11-jdk python3 python3-venv`

### 2. Configurando o Ambiente Virtual
Abra o seu terminal, navegue até a raiz do projeto e execute:

# Cria o ambiente virtual isolado
python3 -m venv venv

# Ativa o ambiente virtual
source venv/bin/activate

# Instala as dependências (PySpark e Pytest)
pip install pyspark==3.5.0 pytest


### 3. Executando os Testes Unitários
Antes de rodar o pipeline, valide as regras de negócio executando a suíte de testes (com Mocks em memória):

pytest tests/


### 4. Executando os Pipelines Principais
Com o ambiente ativado `(venv)`, você pode rodar os módulos diretamente da raiz do projeto:

**Gerar Análises e Métricas Exploratórias:**

python -m src.analise_exploratoria


![WhatsApp Image 2026-03-02 at 09 01 32](https://github.com/user-attachments/assets/14bd58b9-4836-4bde-aab2-ce93d91fa0e5)


![WhatsApp Image 2026-03-02 at 09 01 33](https://github.com/user-attachments/assets/8d0a27cf-d326-44a3-9f82-6e8b0563e65b)


![WhatsApp Image 2026-03-02 at 09 01 33 (1)](https://github.com/user-attachments/assets/96f4c9ce-2b62-4d37-bad3-df011fb46e57)


![WhatsApp Image 2026-03-02 at 09 01 33 (2)](https://github.com/user-attachments/assets/4099ef58-0da4-436b-b3b0-6ca9f3329d69)


**Rodar o Pipeline ETL (Gerar Data Lake Parquet):**

python -m src.vendas_diarias


![WhatsApp Image 2026-03-02 at 09 01 33 (3)](https://github.com/user-attachments/assets/dc15ad04-7cc2-4c97-885c-9a1501dcd833)

**Executar a Auditoria Financeira Final:**

python -m src.auditar_lideres


![WhatsApp Image 2026-03-02 at 09 13 16](https://github.com/user-attachments/assets/31efe787-5a8b-422e-823f-4187af4455b8)

