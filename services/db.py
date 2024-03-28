import json
import math
import time

from typing import Any, Callable, Dict, TypeVar
from sqlalchemy import exc, event, Engine
from sqlalchemy import inspect, Column, Integer, create_engine, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, configure_mappers
from sqlalchemy.sql.schema import Table
from sqlalchemy_searchable import make_searchable

from services.logger import root_logger as logger
from settings import DB_URL
import warnings
import traceback

# Подключение к базе данных SQLAlchemy
engine = create_engine(DB_URL, echo=False, pool_size=10, max_overflow=20)
inspector = inspect(engine)
configure_mappers()
T = TypeVar('T')
REGISTRY: Dict[str, type] = {}
FILTERED_FIELDS = ['_sa_instance_state', 'search_vector']


# noinspection PyUnusedLocal
def local_session(src=''):
    return Session(bind=engine, expire_on_commit=False)


class Base(declarative_base()):
    __table__: Table
    __tablename__: str
    __new__: Callable
    __init__: Callable
    __allow_unmapped__ = True
    __abstract__ = True
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)

    def __init_subclass__(cls, **kwargs):
        REGISTRY[cls.__name__] = cls

    def dict(self) -> Dict[str, Any]:
        column_names = filter(
            lambda x: x not in FILTERED_FIELDS, self.__table__.columns.keys()
        )
        try:
            data = {}
            for c in column_names:
                value = getattr(self, c)
                if isinstance(value, JSON):
                    # save JSON column as dict
                    data[c] = json.loads(str(value))
                else:
                    data[c] = value
            # Add synthetic field .stat
            if hasattr(self, 'stat'):
                data['stat'] = self.stat
            return data
        except Exception as e:
            logger.error(f'Error occurred while converting object to dictionary: {e}')
            return {}

    def update(self, values: Dict[str, Any]) -> None:
        for key, value in values.items():
            if hasattr(self, key):
                setattr(self, key, value)


make_searchable(Base.metadata)
Base.metadata.create_all(bind=engine)


# Функция для вывода полного трейсбека при предупреждениях
def warning_with_traceback(
    message: Warning | str, category, filename: str, lineno: int, file=None, line=None
):
    tb = traceback.format_stack()
    tb_str = ''.join(tb)
    return f'{message} ({filename}, {lineno}): {category.__name__}\n{tb_str}'


# Установка функции вывода трейсбека для предупреждений SQLAlchemy
warnings.showwarning = warning_with_traceback
warnings.simplefilter('always', exc.SAWarning)


@event.listens_for(Engine, 'before_cursor_execute')
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.query_start_time = time.time()
    conn.last_statement = ''

@event.listens_for(Engine, 'after_cursor_execute')
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    compiled_statement = context.compiled.string
    compiled_parameters = context.compiled.params
    if compiled_statement:
        elapsed = time.time() - conn.query_start_time
        query = compiled_statement % compiled_parameters

        if elapsed > 1 and conn.last_statement != query:
            conn.last_statement = query
            logger.debug(f"\n{query}\n{'*' * math.floor(elapsed)} {elapsed:.3f} s\n")
