
import duckdb
from dotenv import load_dotenv
import os


load_dotenv()

conn_str = (
    f"host={os.getenv('DB_HOST')} "
    f"port={os.getenv('DB_PORT')} "
    f"dbname={os.getenv('DB_NAME')} "
    f"user={os.getenv('DB_USER')} "
    f"password={os.getenv('DB_PASSWORD')}"
)

PATH = './data/'


attach_result = duckdb.sql(f"ATTACH '{conn_str}' AS pg_db (TYPE postgres)")












def main():
    pass





if __name__ == '__main__':
    main()