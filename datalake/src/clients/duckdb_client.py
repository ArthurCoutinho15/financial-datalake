import duckdb
import os


class DuckDBClient:
    def __init__(self, base_path: str, db_path: str = "datalake.db"):
        self.conn = duckdb.connect(db_path)
        self.base_path = base_path

    def query(self, sql: str):
        return self.conn.execute(sql).fetchdf()

    def register_table(self, table_name: str, relative_path: str):
        full_path = os.path.join(self.base_path, relative_path)

        self.conn.execute(f"""
            CREATE OR REPLACE VIEW {table_name} AS
            SELECT * FROM read_parquet('{full_path}')
        """)

    def execute(self, sql: str):
        self.conn.execute(sql)
