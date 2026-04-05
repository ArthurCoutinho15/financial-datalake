import psycopg2 

class PostgresClient:

    def get_conn(self):
        pg_conn = psycopg2.connect(
            host="localhost",
            dbname="your_db",
            user="user",
            password="password",
            port=5432
        )
        
        return pg_conn
    
    def execute_query(self, conn ,query: str) -> dict:
        cursor = conn.cursor()
        
        rows = cursor.execute(query)
        
        return rows.fetchall()