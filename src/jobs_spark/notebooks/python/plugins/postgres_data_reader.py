import psycopg2
import pandas as pd

class PostgresDataReader:
    def __init__(self, db_params):
        self.db_params = db_params
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            return True
        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return False

    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def read_data(self, table_name):
        try:
            if not self.conn:
                self.connect()

            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            print(f"Error reading data from table {table_name}: {str(e)}")
            return None