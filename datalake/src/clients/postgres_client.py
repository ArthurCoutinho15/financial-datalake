import psycopg2


class PostgresClient:
    def __init__(self):
        self.host = "host.docker.internal"
        self.dbname = "financial"
        self.user = "financial"
        self.password = "financial"
        self.port = 5433

    def get_conn(self):
        return psycopg2.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            port=self.port,
        )

    def execute_query(self, conn, query: str):
        cursor = conn.cursor()
        cursor.execute(query)

        try:
            result = cursor.fetchall()
        except Exception:
            result = None

        conn.commit()
        cursor.close()

        return result

    def execute_many(self, conn, query: str, data: list):
        cursor = conn.cursor()

        cursor.executemany(query, data)

        conn.commit()
        cursor.close()
