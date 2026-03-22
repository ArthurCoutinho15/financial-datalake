<h1>Data lake dados financeiros</h1>
<img source="https://github.com/ArthurCoutinho15/financial-datalake/blob/main/datalake/docs/finance.excalidraw">
<img src="https://github.com/ArthurCoutinho15/financial-datalake/blob/main/datalake/docs/finance.excalidraw?raw=true" width="50px" height="50px"/>
<text>APIs externas          Postgres (CRUD)
(stocks/crypto/fx)     (clientes/portfolios/positions/transactions)
       ↓                        ↓
   Raw (Iceberg)            Raw (Iceberg)
   preços brutos            clientes brutos
       ↓                        ↓
  Curated (Iceberg)        Curated (Iceberg)
  preços limpos/           clientes/posições
  normalizados             normalizados
       ↓                        ↓
       └────────────────────────┘
                  ↓
            Gold (dbt + DuckDB)
            lê os dois Icebergs
                  ↓
       ┌──────────────────────┐
       │  Dimensões           │
       │  dim_clients         │
       │  dim_assets          │
       │  dim_portfolios      │
       └──────────┬───────────┘
                  ↓
       ┌──────────────────────┐
       │  Fatos / Métricas    │
       │  fct_positions       │  ← quantidade × cotação = valor atual
       │  fct_transactions    │  ← histórico de operações
       │  fct_portfolio_pnl   │  ← P&L por portfolio
       │  fct_client_wealth   │  ← patrimônio total por cliente
       └──────────┬───────────┘
                  ↓
              FastAPI
         (data as a service)
</text>
