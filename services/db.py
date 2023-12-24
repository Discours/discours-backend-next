import math
import time
import logging

# from contextlib import contextmanager
from typing import Any, Callable, Dict, TypeVar

# from psycopg2.errors import UniqueViolation
from sqlalchemy import Column, Integer, create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql.schema import Table
from sqlalchemy.engine import Engine

from settings import DB_URL

logging.basicConfig()
logger = logging.getLogger("\t [sqlalchemy.profiler]\t")
logger.setLevel(logging.DEBUG)


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault("query_start_time", []).append(time.time())
    logger.debug(f" {statement}")


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    total = time.time() - conn.info["query_start_time"].pop(-1)
    logger.debug(f" Finished in {math.floor(total*10000)/10} ms ")


engine = create_engine(DB_URL, echo=False, pool_size=10, max_overflow=20)

T = TypeVar("T")

REGISTRY: Dict[str, type] = {}


# @contextmanager
def local_session(src=""):
    return Session(bind=engine, expire_on_commit=False)

    # try:
    #     yield session
    #     session.commit()
    # except Exception as e:
    #     if not (src == "create_shout" and isinstance(e, UniqueViolation)):
    #         import traceback

    #         session.rollback()
    #         print(f"[services.db] {src}: {e}")

    #         traceback.print_exc()

    #         raise Exception("[services.db] exception")

    # finally:
    #     session.close()


class Base(declarative_base()):
    __table__: Table
    __tablename__: str
    __new__: Callable
    __init__: Callable
    __allow_unmapped__ = True
    __abstract__ = True
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)

    def __init_subclass__(cls, **kwargs):
        REGISTRY[cls.__name__] = cls

    def dict(self) -> Dict[str, Any]:
        column_names = self.__table__.columns.keys()
        if "_sa_instance_state" in column_names:
            column_names.remove("_sa_instance_state")
        try:
            return {c: getattr(self, c) for c in column_names}
        except Exception as e:
            print(f"[services.db] Error dict: {e}")
            return {}

    def update(self, values: Dict[str, Any]) -> None:
        for key, value in values.items():
            if hasattr(self, key):
                setattr(self, key, value)
