"""
Gerador de dados fake para datalake financeiro.
Gera: clientes, portfolios, posições e transações históricas.

Dependências:
    pip install faker psycopg2-binary python-dotenv

Uso:
    # Gera arquivo SQL (sem banco):
    python fake_data.py --output sql

    # Gera arquivos CSV:
    python fake_data.py --output csv

    # Insere direto no Postgres:
    python fake_data.py --output postgres

Variáveis de ambiente (para --output postgres):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

from faker import Faker
from faker.providers import person, internet, address

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

fake = Faker("pt_BR")
fake.add_provider(person)
fake.add_provider(internet)
fake.add_provider(address)

random.seed(42)
Faker.seed(42)

NUM_CLIENTS       = 150
MIN_PORTFOLIOS    = 1
MAX_PORTFOLIOS    = 3
MIN_POSITIONS     = 3
MAX_POSITIONS     = 10
MIN_TRANSACTIONS  = 3   # por posição
MAX_TRANSACTIONS  = 15  # por posição

START_DATE = datetime(2026, 3, 1)
END_DATE   = datetime(2026, 3, 31)

# Tickers por tipo — ajuste conforme os tickers da sua curated
TICKERS = {
    "stock": [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
        "META", "TSLA", "JPM", "V", "WMT",
        "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "WEGE3.SA",
        "ABEV3.SA", "RENT3.SA", "MGLU3.SA", "LREN3.SA", "B3SA3.SA",
    ],
    "crypto": [
        "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD",
        "ADA-USD", "XRP-USD", "DOGE-USD", "AVAX-USD",
    ],
    "fx": [
        "USD-BRL", "EUR-BRL", "GBP-BRL", "JPY-BRL", "CHF-BRL",
    ],
}

# Preço médio de referência em BRL por ticker (aproximado, para simular realismo)
PRICE_REFERENCE_BRL = {
    # Stocks US
    "AAPL": 950.0, "MSFT": 1800.0, "GOOGL": 900.0, "AMZN": 950.0,
    "NVDA": 4500.0, "META": 2300.0, "TSLA": 1100.0, "JPM": 1000.0,
    "V": 1400.0, "WMT": 850.0,
    # Stocks BR
    "PETR4.SA": 38.0, "VALE3.SA": 62.0, "ITUB4.SA": 33.0,
    "BBDC4.SA": 14.0, "WEGE3.SA": 52.0, "ABEV3.SA": 13.0,
    "RENT3.SA": 55.0, "MGLU3.SA": 7.0, "LREN3.SA": 19.0, "B3SA3.SA": 12.0,
    # Crypto
    "BTC-USD": 280000.0, "ETH-USD": 17000.0, "BNB-USD": 1500.0,
    "SOL-USD": 700.0, "ADA-USD": 2.5, "XRP-USD": 3.2,
    "DOGE-USD": 0.9, "AVAX-USD": 450.0,
    # FX (unidade = 1000 unidades da moeda estrangeira)
    "USD-BRL": 5.1, "EUR-BRL": 5.6, "GBP-BRL": 6.5,
    "JPY-BRL": 0.035, "CHF-BRL": 5.8,
}

# Quantidade padrão por tipo de ativo
QUANTITY_RANGE = {
    "stock":  (1,   200),
    "crypto": (0.01, 5.0),
    "fx":     (100, 10000),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def fmt_decimal(value: float, places: int = 2) -> str:
    d = Decimal(str(value)).quantize(Decimal(10) ** -places, rounding=ROUND_HALF_UP)
    return str(d)


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def new_id() -> str:
    return str(uuid.uuid4())


def cpf_fake() -> str:
    """CPF formatado, puramente aleatório (não validado)."""
    digits = [random.randint(0, 9) for _ in range(11)]
    return f"{''.join(map(str, digits[:3]))}.{''.join(map(str, digits[3:6]))}.{''.join(map(str, digits[6:9]))}-{''.join(map(str, digits[9:]))}"


def pick_asset() -> tuple[str, str]:
    """Retorna (ticker, asset_type)."""
    asset_type = random.choices(
        ["stock", "crypto", "fx"], weights=[60, 30, 10], k=1
    )[0]
    ticker = random.choice(TICKERS[asset_type])
    return ticker, asset_type


def simulate_price_history(base_price: float, n: int, volatility: float = 0.04) -> list[float]:
    """Simula n preços históricos com random walk a partir de base_price."""
    prices = [base_price]
    for _ in range(n - 1):
        change = random.gauss(0, volatility)
        prices.append(round(prices[-1] * (1 + change), 6))
    return prices


# ---------------------------------------------------------------------------
# Geração de entidades
# ---------------------------------------------------------------------------

def generate_clients(n: int) -> list[dict]:
    clients = []
    for _ in range(n):
        created_at = random_date(START_DATE, END_DATE - timedelta(days=30))
        clients.append({
            "id":         new_id(),
            "name":       fake.name(),
            "email":      fake.email(),
            "cpf":        cpf_fake(),
            "phone":      fake.phone_number(),
            "city":       fake.city(),
            "state":      fake.estado_sigla(),
            "created_at": created_at,
            "updated_at": created_at,
        })
    return clients


def generate_portfolios(clients: list[dict]) -> list[dict]:
    portfolios = []
    names = ["Conservador", "Moderado", "Arrojado", "Previdência", "Renda Variável", "Internacional"]
    for client in clients:
        n = random.randint(MIN_PORTFOLIOS, MAX_PORTFOLIOS)
        for i in range(n):
            created_at = client["created_at"] + timedelta(days=random.randint(0, 10))
            portfolios.append({
                "id":         new_id(),
                "client_id":  client["id"],
                "name":       random.choice(names),
                "created_at": created_at,
                "updated_at": created_at,
            })
    return portfolios


def generate_positions_and_transactions(
    portfolios: list[dict],
) -> tuple[list[dict], list[dict]]:
    positions     = []
    transactions  = []

    for portfolio in portfolios:
        n_positions = random.randint(MIN_POSITIONS, MAX_POSITIONS)
        used_tickers: set[str] = set()

        for _ in range(n_positions):
            # evita ticker duplicado no mesmo portfolio
            for attempt in range(20):
                ticker, asset_type = pick_asset()
                if ticker not in used_tickers:
                    used_tickers.add(ticker)
                    break
            else:
                continue

            base_price = PRICE_REFERENCE_BRL.get(ticker, 100.0)
            q_min, q_max = QUANTITY_RANGE[asset_type]

            pos_id = new_id()
            # garante que pos_created não ultrapasse END_DATE
            days_until_end = max(0, (END_DATE - portfolio["created_at"]).days - 1)
            pos_created = portfolio["created_at"] + timedelta(days=random.randint(0, min(30, days_until_end)))

            # Gera transações históricas
            n_tx = random.randint(MIN_TRANSACTIONS, MAX_TRANSACTIONS)
            tx_dates = sorted([random_date(pos_created, END_DATE) for _ in range(n_tx)])
            prices   = simulate_price_history(base_price, n_tx)

            total_qty   = 0.0
            total_cost  = 0.0
            last_update = pos_created

            for i, (tx_date, price) in enumerate(zip(tx_dates, prices)):
                # Lógica: maioria buy, alguns sell se já houver posição
                if total_qty <= 0 or random.random() < 0.70:
                    tx_type = "buy"
                    qty = round(random.uniform(q_min, q_max), 6)
                    total_qty  += qty
                    total_cost += qty * price
                else:
                    tx_type = "sell"
                    qty = round(random.uniform(q_min, min(q_max, total_qty * 0.5)), 6)
                    if qty > total_qty:
                        qty = total_qty
                    total_qty  -= qty
                    total_cost -= qty * price  # simplificado

                last_update = tx_date

                transactions.append({
                    "id":           new_id(),
                    "position_id":  pos_id,
                    "type":         tx_type,
                    "quantity":     qty,
                    "price_brl":    round(price, 6),
                    "executed_at":  tx_date,
                    "created_at":   tx_date,
                })

            if total_qty <= 0:
                total_qty  = 0.0
                total_cost = 0.0

            avg_price = (total_cost / total_qty) if total_qty > 0 else 0.0

            positions.append({
                "id":            pos_id,
                "portfolio_id":  portfolio["id"],
                "ticker":        ticker,
                "asset_type":    asset_type,
                "quantity":      round(total_qty, 6),
                "avg_price_brl": round(avg_price, 6),
                "created_at":    pos_created,
                "updated_at":    last_update,
            })

    return positions, transactions


# ---------------------------------------------------------------------------
# SQL output
# ---------------------------------------------------------------------------

DDL = """
-- ============================================================
-- DDL — Base fake de clientes para datalake financeiro
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS positions    CASCADE;
DROP TABLE IF EXISTS portfolios   CASCADE;
DROP TABLE IF EXISTS clients      CASCADE;

CREATE TABLE clients (
    id          UUID PRIMARY KEY,
    name        VARCHAR(200)        NOT NULL,
    email       VARCHAR(200) UNIQUE NOT NULL,
    cpf         VARCHAR(14)  UNIQUE NOT NULL,
    phone       VARCHAR(30),
    city        VARCHAR(100),
    state       CHAR(2),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE portfolios (
    id          UUID PRIMARY KEY,
    client_id   UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    name        VARCHAR(100) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE positions (
    id              UUID PRIMARY KEY,
    portfolio_id    UUID NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
    ticker          VARCHAR(20)  NOT NULL,
    asset_type      VARCHAR(10)  NOT NULL CHECK (asset_type IN ('stock','crypto','fx')),
    quantity        NUMERIC(20,6) NOT NULL DEFAULT 0,
    avg_price_brl   NUMERIC(20,6) NOT NULL DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (portfolio_id, ticker)
);

CREATE TABLE transactions (
    id           UUID PRIMARY KEY,
    position_id  UUID NOT NULL REFERENCES positions(id) ON DELETE CASCADE,
    type         VARCHAR(4) NOT NULL CHECK (type IN ('buy','sell')),
    quantity     NUMERIC(20,6) NOT NULL,
    price_brl    NUMERIC(20,6) NOT NULL,
    executed_at  TIMESTAMP NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_portfolios_client_id    ON portfolios(client_id);
CREATE INDEX idx_positions_portfolio_id  ON positions(portfolio_id);
CREATE INDEX idx_positions_ticker        ON positions(ticker);
CREATE INDEX idx_transactions_position   ON transactions(position_id);
CREATE INDEX idx_transactions_executed   ON transactions(executed_at);

"""


def to_sql_string(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, datetime):
        return f"'{fmt_ts(value)}'"
    if isinstance(value, float):
        return fmt_decimal(value, 6)
    return str(value)


def records_to_insert(table: str, records: list[dict], batch: int = 500) -> list[str]:
    if not records:
        return []
    cols = list(records[0].keys())
    col_str = ", ".join(cols)
    statements = []
    for i in range(0, len(records), batch):
        chunk = records[i:i + batch]
        rows = []
        for r in chunk:
            vals = ", ".join(to_sql_string(r[c]) for c in cols)
            rows.append(f"    ({vals})")
        statements.append(f"INSERT INTO {table} ({col_str}) VALUES\n" + ",\n".join(rows) + ";")
    return statements


def write_sql(clients, portfolios, positions, transactions, path: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(DDL)
        f.write("\n-- CLIENTS\n")
        for s in records_to_insert("clients", clients):
            f.write(s + "\n\n")
        f.write("\n-- PORTFOLIOS\n")
        for s in records_to_insert("portfolios", portfolios):
            f.write(s + "\n\n")
        f.write("\n-- POSITIONS\n")
        for s in records_to_insert("positions", positions):
            f.write(s + "\n\n")
        f.write("\n-- TRANSACTIONS\n")
        for s in records_to_insert("transactions", transactions, batch=1000):
            f.write(s + "\n\n")
    print(f"✅ SQL gerado em: {path}")


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def write_csv(clients, portfolios, positions, transactions, base_path: str = "fake_data"):
    """Escreve dados fake em arquivos CSV separados por entidade."""
    
    # Clients
    clients_file = f"{base_path}_clients.csv"
    with open(clients_file, "w", newline="", encoding="utf-8") as f:
        if clients:
            writer = csv.DictWriter(f, fieldnames=clients[0].keys())
            writer.writeheader()
            writer.writerows(clients)
    print(f"  ✅ Clientes: {len(clients)} registros em {clients_file}")
    
    # Portfolios
    portfolios_file = f"{base_path}_portfolios.csv"
    with open(portfolios_file, "w", newline="", encoding="utf-8") as f:
        if portfolios:
            writer = csv.DictWriter(f, fieldnames=portfolios[0].keys())
            writer.writeheader()
            writer.writerows(portfolios)
    print(f"  ✅ Portfolios: {len(portfolios)} registros em {portfolios_file}")
    
    # Positions
    positions_file = f"{base_path}_positions.csv"
    with open(positions_file, "w", newline="", encoding="utf-8") as f:
        if positions:
            writer = csv.DictWriter(f, fieldnames=positions[0].keys())
            writer.writeheader()
            writer.writerows(positions)
    print(f"  ✅ Posições: {len(positions)} registros em {positions_file}")
    
    # Transactions
    transactions_file = f"{base_path}_transactions.csv"
    with open(transactions_file, "w", newline="", encoding="utf-8") as f:
        if transactions:
            writer = csv.DictWriter(f, fieldnames=transactions[0].keys())
            writer.writeheader()
            writer.writerows(transactions)
    print(f"  ✅ Transações: {len(transactions)} registros em {transactions_file}")




def insert_postgres(clients, portfolios, positions, transactions):
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError:
        raise ImportError("Instale psycopg2-binary: pip install psycopg2-binary")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "datalake"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # DDL
        cur.execute(DDL)

        def _insert(table: str, records: list[dict]):
            if not records:
                return
            cols = list(records[0].keys())
            sql  = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
            data = [tuple(r[c] for c in cols) for r in records]
            execute_values(cur, sql, data, page_size=500)
            print(f"  → {table}: {len(records)} registros inseridos")

        _insert("clients",      clients)
        _insert("portfolios",   portfolios)
        _insert("positions",    positions)
        _insert("transactions", transactions)

        conn.commit()
        print("✅ Dados inseridos com sucesso no Postgres!")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Gerador de dados fake financeiros")
    parser.add_argument(
        "--output",
        choices=["sql", "csv", "postgres"],
        default="sql",
        help="'sql' gera arquivo .sql | 'csv' gera arquivos .csv | 'postgres' insere direto no banco",
    )
    parser.add_argument(
        "--sql-file",
        default="fake_data.sql",
        help="Caminho do arquivo SQL gerado (padrão: fake_data.sql)",
    )
    parser.add_argument(
        "--csv-file",
        default="fake_data",
        help="Prefixo dos arquivos CSV gerados (padrão: fake_data) — gera fake_data_clients.csv, etc.",
    )
    args = parser.parse_args()

    print("🔧 Gerando dados fake...")
    clients                  = generate_clients(NUM_CLIENTS)
    portfolios               = generate_portfolios(clients)
    positions, transactions  = generate_positions_and_transactions(portfolios)

    print(f"  clients:      {len(clients)}")
    print(f"  portfolios:   {len(portfolios)}")
    print(f"  positions:    {len(positions)}")
    print(f"  transactions: {len(transactions)}")

    if args.output == "sql":
        write_sql(clients, portfolios, positions, transactions, args.sql_file)
    elif args.output == "csv":
        print("✅ Gerando CSV...")
        write_csv(clients, portfolios, positions, transactions, args.csv_file)
    else:
        insert_postgres(clients, portfolios, positions, transactions)


if __name__ == "__main__":
    main()