import logging
from contextlib import contextmanager
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("pipeline")

def get_engine(conn_str: str):
    return create_engine(conn_str, pool_pre_ping=True)

@contextmanager
def db_connect(conn_str: str):
    engine = get_engine(conn_str)
    try:
        with engine.begin() as conn:
            yield conn
    finally:
        engine.dispose()
