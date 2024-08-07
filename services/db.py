import json
import math
import time
import traceback
import warnings
from typing import Any, Callable, Dict, TypeVar

from sqlalchemy import JSON, Column, Engine, Integer, create_engine, event, exc, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, configure_mappers
from sqlalchemy.sql.schema import Table

from utils.logger import root_logger as logger
from settings import DB_URL

# from sqlalchemy_searchable import make_searchable


# Подключение к базе данных SQLAlchemy
engine = create_engine(
    DB_URL,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,  # Время ожидания свободного соединения
    pool_recycle=1800,  # Время жизни соединения
    connect_args={"sslmode": "disable"},
)
inspector = inspect(engine)
configure_mappers()
T = TypeVar("T")
REGISTRY: Dict[str, type] = {}
FILTERED_FIELDS = ["_sa_instance_state", "search_vector"]


# noinspection PyUnusedLocal
def local_session(src=""):
    return Session(bind=engine, expire_on_commit=False)


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
        column_names = filter(lambda x: x not in FILTERED_FIELDS, self.__table__.columns.keys())
        data = {}
        try:
            for column_name in column_names:
                value = getattr(self, column_name)
                # Check if the value is JSON and decode it if necessary
                if isinstance(value, (str, bytes)) and isinstance(self.__table__.columns[column_name].type, JSON):
                    try:
                        data[column_name] = json.loads(value)
                    except (TypeError, json.JSONDecodeError) as e:
                        logger.error(f"Error decoding JSON for column '{column_name}': {e}")
                        data[column_name] = value
                else:
                    data[column_name] = value
            # Add synthetic field .stat if it exists
            if hasattr(self, "stat"):
                data["stat"] = self.stat
        except Exception as e:
            logger.error(f"Error occurred while converting object to dictionary: {e}")
        return data

    def update(self, values: Dict[str, Any]) -> None:
        for key, value in values.items():
            if hasattr(self, key):
                setattr(self, key, value)


# make_searchable(Base.metadata)
Base.metadata.create_all(bind=engine)


# Функция для вывода полного трейсбека при предупреждениях
def warning_with_traceback(message: Warning | str, category, filename: str, lineno: int, file=None, line=None):
    tb = traceback.format_stack()
    tb_str = "".join(tb)
    return f"{message} ({filename}, {lineno}): {category.__name__}\n{tb_str}"


# Установка функции вывода трейсбека для предупреждений SQLAlchemy
warnings.showwarning = warning_with_traceback
warnings.simplefilter("always", exc.SAWarning)


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.query_start_time = time.time()
    conn.last_statement = None


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    compiled_statement = context.compiled.string
    compiled_parameters = context.compiled.params
    if compiled_statement:
        elapsed = time.time() - conn.query_start_time
        if compiled_parameters is not None:
            query = compiled_statement.format(*compiled_parameters)
        else:
            query = compiled_statement  # or handle this case in a way that makes sense for your application

        if elapsed > 1 and conn.last_statement != query:
            conn.last_statement = query
            logger.debug(f"\n{query}\n{'*' * math.floor(elapsed)} {elapsed:.3f} s\n")
