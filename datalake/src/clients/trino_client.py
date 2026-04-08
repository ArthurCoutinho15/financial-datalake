import trino


class TrinoClient:
    def __init__(self):
        self.host = "trino"
        self.port = 8080
        self.user = "airflow"
        self.catalog = "iceberg"
        self.schema = "gold"

    def get_conn(self):
        conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
        )

        return conn

    def execute_query(self, conn, query: str) -> tuple:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        columns = [col[0] for col in cursor.description]

        return rows, columns
