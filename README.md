# 💰 Financial Pipeline - Data Lake

Pipeline de dados financeiros com arquitetura **Medallion** (Bronze/Silver/Gold) para consolidar dados de mercado e carteiras de investimento.

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                           │
├──────────────────────────┬──────────────────────────────────────┤
│  APIs Externas           │  Postgres (CRUD)                      │
│  • Stocks (B3)           │  • Clientes                           │
│  • Cryptocurrencies      │  • Portfolios                         │
│  • Moedas (FX)           │  • Posições                           │
└──────────────────────────┴──────────────────────────────────────┘
                    ↓                          ↓
        ┌───────────────────────┐  ┌──────────────────────┐
        │  RAW (Iceberg)        │  │  RAW (Iceberg)       │
        │  • stocks_raw         │  │  • clients_raw       │
        │  • coins_raw          │  │  • portfolios_raw    │
        │  • crypto_raw         │  │  • positions_raw     │
        │  (preços brutos)      │  │  • transactions_raw  │
        └───────┬───────────────┘  │  (dados brutos)      │
                │                  └──────┬───────────────┘
                │ PySpark Jobs             │ PySpark Jobs
                ↓                          ↓
        ┌───────────────────────┐  ┌──────────────────────┐
        │  CURATED (Iceberg)    │  │  CURATED (Iceberg)   │
        │  • stocks_curated     │  │  • clients_curated   │
        │  • coins_curated      │  │  (limpos &           │
        │  • crypto_curated     │  │   normalizados)      │
        │  (limpos &            │  └──────┬───────────────┘
        │   normalizados)       │         │
        └───────┬───────────────┘         │
                └──────────────┬──────────┘
                               ↓
                  ┌──────────────────────────┐
                  │  GOLD (dbt + Iceberg)    │
                  │  ┌────────────────────┐  │
                  │  │  STAGING           │  │
                  │  │  • stg_clients     │  │
                  │  │  • stg_positions   │  │
                  │  │  • stg_prices      │  │
                  │  └────────────────────┘  │
                  │  ┌────────────────────┐  │
                  │  │  MARTS             │  │
                  │  │  • dim_clients     │  │
                  │  │  • dim_assets      │  │
                  │  │  • fct_positions   │  │
                  │  │  • fct_transactions│  │
                  │  │  • fct_portfolio   │  │
                  │  └────────────────────┘  │
                  └──────────┬────────────────┘
                             ↓
                         BI / APIs
```

![Diagrama da Arquitetura](datalake/docs/finance-datalakehouse-service.png)

## 📂 Estrutura do Projeto

```
financial_pipeline/
├── airflow/
│   ├── dags/
│   │   ├── clients_dag.py          # Pipeline de clientes
│   │   ├── coins_dag.py            # Pipeline de moedas
│   │   ├── crypto_dag.py           # Pipeline de criptmoedas
│   │   └── stocks_dag.py           # Pipeline de ações
│   ├── config/
│   │   └── airflow.cfg
│   ├── docker-compose.yaml
│   └── dockerfile
│
├── datalake/
│   ├── src/
│   │   ├── jobs/
│   │   │   ├── clients/            # Pipeline de clientes
│   │   │   │   ├── raw/
│   │   │   │   │   ├── clients_job.py
│   │   │   │   │   └── clients_table.py
│   │   │   │   └── curated/
│   │   │   │       ├── clients_job.py
│   │   │   │       └── clients_table.py
│   │   │   ├── stocks/             # Pipeline de ações
│   │   │   ├── coins/              # Pipeline de moedas
│   │   │   └── crypto/             # Pipeline de criptmoedas
│   │   │
│   │   └── dbt/                    # Transformações (Gold layer)
│   │       ├── models/
│   │       │   ├── staging/
│   │       │   ├── marts/
│   │       │   └── tests/
│   │       ├── macros/
│   │       ├── dbt_project.yml
│   │       └── profiles.yml
│   │
│   ├── notebooks/                  # Análises e testes
│   ├── docs/
│   └── requirements.txt
│
└── lakehouse/
    ├── raw/                        # Bronze - dados brutos
    │   ├── clients/
    │   ├── stocks/
    │   ├── coins/
    │   └── crypto/
    ├── curated/                    # Silver - dados limpos
    │   ├── clients/
    │   ├── stocks/
    │   ├── coins/
    │   └── crypto/
    └── gold/                       # Gold - dados prontos
        ├── staging/
        └── marts/
```

## 🚀 Como Começar

### Pré-requisitos
- Docker & Docker Compose
- Python 3.11+
- Git

### Setup

1. **Clone o repositório**
```bash
git clone https://github.com/ArthurCoutinho15/financial-pipeline.git
cd financial_pipeline
```

2. **Configure o ambiente Python**
```bash
cd datalake
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. **Inicie os containers do Airflow**
```bash
cd ../airflow
docker compose up -d
```

4. **Acesse o Airflow**
- URL: `http://localhost:8080`
- Usuário: `airflow`
- Senha: `airflow`

## 🔄 DAGs Disponíveis

### 1. `clients_pipeline`
Orquestra o pipeline completo de clientes:
- `raw_clients` → raw layer
- `raw_portfolios` → raw layer
- `raw_positions` → raw layer
- `raw_transactions` → raw layer
- `curated_clients` → curated layer (join de todas as tabelas)

### 2. `stocks_pipeline`
Pipeline de dados de ações da B3, com backfill diário.

### 3. `coins_pipeline`
Pipeline de cotações de moedas/FX.

### 4. `crypto_pipeline`
Pipeline de criptmoedas via APIs públicas.

## 🛠️ Estrutura de Jobs (PySpark)

Cada job segue o padrão:

```python
class RawClientsJob:
    def __init__(self, date: date)
    def _get_data(self) -> DataFrame
    def transform(self) -> DataFrame
    def save(self) -> None
    def run(self) -> None
```

**Camadas:**
- **Raw**: Ingestão de dados brutos do banco/APIs
- **Curated**: Limpeza, validação e joins de múltiplas fontes

## 📊 DBT (Camada Gold)

A camada `gold` utiliza **dbt** para transformações SQL com:
- Staging models (limpeza adicional)
- Mart models (fatos e dimensões)
- Testes de data quality
- Documentação automática

```bash
cd datalake/src/dbt

# Rodar models
dbt run

# Rodar testes
dbt test

# Gerar documentação
dbt docs generate
```

## 📋 Estrutura de Dados

### Raw Tables
- `hadoop_catalog.raw.clients` - Dados brutos de clientes
- `hadoop_catalog.raw.portfolios` - Dados brutos de portfolios
- `hadoop_catalog.raw.positions` - Dados brutos de posições
- `hadoop_catalog.raw.transactions` - Histórico de transações

### Curated Tables
- `hadoop_catalog.curated.clients` - Clientes com portfolios, posições e transações consolidadas

### Gold Tables (via dbt)
- `hadoop_catalog.gold.dim_clients` - Dimensão de clientes
- `hadoop_catalog.gold.dim_assets` - Dimensão de ativos
- `hadoop_catalog.gold.fct_positions` - Fato de posições atualizadas
- `hadoop_catalog.gold.fct_transactions` - Fato de transações históricas

## 🧪 Testes

```bash
# Rodar suite de testes
cd datalake
pytest tests/

# Rodar com coverage
pytest --cov=src tests/
```

## 📝 Logs

Logs do Airflow estão disponíveis em:
- UI do Airflow: `http://localhost:8080/admin/log`
- Disco local: `./airflow/logs/`

## 🔐 Variáveis de Ambiente

Criar arquivo `.env` na raiz:
```env
SPARK_MASTER=spark://localhost:7077
ICEBERG_CATALOG=hadoop_catalog
POSTGRES_HOST=localhost
POSTGRES_DB=financial_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
```

## 📚 Documentação

- Diagrama da arquitetura: `datalake/docs/finance.excalidraw`
- DBT docs: `datalake/src/dbt/target/index.html`

## 👤 Autor

Arthur Coutinho

## 📄 Licença

MIT
