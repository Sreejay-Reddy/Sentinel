def init_db(conn):
    from .schema import SCHEMA_SQL

    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)

    conn.commit()