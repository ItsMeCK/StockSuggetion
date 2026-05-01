import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def apply_schema():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    with open("db/schema.sql", "r") as f:
        sql = f.read()
    
    # Remove psql-specific commands if any
    sql = "\n".join([line for line in sql.split("\n") if not line.strip().startswith("\\c")])
    
    try:
        cur.execute(sql)
        conn.commit()
        print("Schema applied successfully")
    except Exception as e:
        conn.rollback()
        print(f"Failed to apply schema: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    apply_schema()
