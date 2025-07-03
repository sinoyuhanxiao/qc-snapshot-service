# db/postgres.py

import psycopg2
from config.db_config import DB_CONFIG
from sqlalchemy import create_engine

pg_engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

def get_postgres_connection():
    """Establish and return a PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)

def test_query():
    conn = get_postgres_connection()
    cursor = conn.cursor()

    query = """
        SELECT id, alert_code, alert_time, inspection_item_label, inspection_value
        FROM quality_management.qc_alert_record
        ORDER BY created_at DESC
        LIMIT 5
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    print("✅ 查询结果:")
    for row in rows:
        print(row)

    cursor.close()
    conn.close()

def get_name_by_id(cursor, table_name, id_column, name_column, id_value):
    cursor.execute(
        f"SELECT {name_column} FROM quality_management.{table_name} WHERE {id_column} = %s",
        (id_value,)
    )
    result = cursor.fetchone()
    return result[0] if result else None

if __name__ == "__main__":
    test_query()
