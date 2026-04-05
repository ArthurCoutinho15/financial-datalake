import trino 

class TrinoClient:

    def get_conn(self):
        conn = trino.dbapi.connect(
            host="localhost",
            port=8080,
            user="airflow",
            catalog="iceberg",
            schema="gold"
        )
        
        return conn
    
    def execute_query(self, conn ,query: str) -> dict:
        cursor = conn.cursor()
        
        rows = cursor.execute(query)
        
        return rows.fetchall()