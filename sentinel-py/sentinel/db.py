def init_db(conn):
    from .schema import SCHEMA_SQL

    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)

    conn.commit()

async def async_init_db(conn):
    from .schema import SCHEMA_SQL

    async with conn.cursor() as cur:
        await cur.execute(SCHEMA_SQL)

    await conn.commit()