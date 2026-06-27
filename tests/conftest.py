import pytest
import asyncio
import pytest_asyncio
import sys
import psycopg
from sentinel import init_db
from sentinel.heartbeat_config import shutdown_manager

DSN = "postgresql://sentinel_test:sentinel_test@localhost/sentinel_test"


def get_conn():
    return psycopg.connect(DSN)


def clean_db(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sentinel_leases;")
        cur.execute("ALTER SEQUENCE sentinel_token_seq RESTART WITH 1;")
        cur.execute("DELETE FROM sentinel_events;")
    conn.commit()


@pytest.fixture(scope="session", autouse=True)
def setup_db():
    conn = get_conn()
    init_db(conn)
    conn.close()


@pytest.fixture(autouse=True)
def reset_db():
    conn = get_conn()
    clean_db(conn)
    conn.close()
    yield
    shutdown_manager()


@pytest.fixture
def conn():
    c = get_conn()
    yield c
    c.close()


@pytest.fixture
def get_conn_fixture():
    return get_conn

def pytest_asyncio_loop_factories():
    if sys.platform == "win32":
        return {"selector": lambda: asyncio.SelectorEventLoop()}
    
    # Return a valid loop factory for non-Windows platforms
    return {"default": lambda: asyncio.new_event_loop()}

async def get_async_conn():
    return await psycopg.AsyncConnection.connect(DSN)

@pytest.fixture
def get_async_conn_fixture():
    return get_async_conn

@pytest.fixture(autouse=True)
def reset_heartbeat_manager():
    from sentinel.heartbeat_config import shutdown_manager
    shutdown_manager()
    yield
    shutdown_manager()

@pytest.fixture(autouse=True)
def clean_db_():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sentinel_leases")
    conn.commit()
    conn.close()

@pytest_asyncio.fixture
async def aconn():
    conn = await psycopg.AsyncConnection.connect(DSN)
    yield conn
    await conn.close()