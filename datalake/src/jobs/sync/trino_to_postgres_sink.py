from datetime import date
import logging

from clients.trino_client import TrinoClient
from clients.postgres_client import PostgresClient


class DataSync:
    def __init__(self, date: date):
        self.date = date
        self.trino = TrinoClient()
        self.postgres = PostgresClient()

    def get_trino_data(self):
        query = """
            SELECT *
            FROM gold.fct_positions
        """

        conn = self.trino.get_conn()
        rows, columns = self.trino.execute_query(conn, query=query)
        logging.info(f"Dados encontrados: {len(rows)}")
        return rows, columns

    def _clean_postgres_data(self):
        query = "TRUNCATE TABLE fct_positions"

        conn = self.postgres.get_conn()
        self.postgres.execute_query(conn, query)

    def _create_table_if_not_exists(self, columns):
        cols_sql = ", ".join([f"{col} TEXT" for col in columns])

        query = f"""
        CREATE TABLE IF NOT EXISTS fct_positions (
            {cols_sql}
        )
        """

        conn = self.postgres.get_conn()
        self.postgres.execute_query(conn, query)

    def insert_data_into_postgres(self):
        trino_data, columns = self.get_trino_data()

        if not trino_data:
            print("Nenhum dado encontrado")
            return

        self._create_table_if_not_exists(columns)

        placeholders = ",".join(["%s"] * len(trino_data[0]))

        self._clean_postgres_data()

        insert_query = f"""
            INSERT INTO fct_positions
            VALUES ({placeholders})
        """

        conn = self.postgres.get_conn()

        self.postgres.execute_many(conn, insert_query, trino_data)

        print(f"{len(trino_data)} registros inseridos com sucesso!")
