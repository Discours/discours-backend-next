import math
import time
from functools import wraps
from sqlalchemy import event, Engine
from typing import Any, Callable, Dict, TypeVar

from dogpile.cache import make_region
from sqlalchemy import exc, Column, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql.schema import Table
from services.logger import root_logger as logger
from settings import DB_URL
import warnings
import traceback


# Функция для вывода полного трейсбека при предупреждениях
def warning_with_traceback(message, category, filename, lineno, line=None):
    tb = traceback.format_stack()
    tb_str = ''.join(tb)
    return f'{message} ({filename}, {lineno}): {category.__name__}\n{tb_str}'


# Установка функции вывода трейсбека для предупреждений SQLAlchemy
warnings.formatwarning = warning_with_traceback
warnings.simplefilter('always', exc.SAWarning)


# Установка функции вывода трейсбека для предупреждений SQLAlchemy
warnings.showwarning = warning_with_traceback
warnings.simplefilter('always', exc.SAWarning)

# Создание региона кэша с TTL 300 секунд
cache_region = make_region().configure('dogpile.cache.memory', expiration_time=300)

# Подключение к базе данных SQLAlchemy
engine = create_engine(DB_URL, echo=False, pool_size=10, max_overflow=20)
T = TypeVar('T')
REGISTRY: Dict[str, type] = {}


# Перехватчики для журнала запросов SQLAlchemy
# noinspection PyUnusedLocal
@event.listens_for(Engine, 'before_cursor_execute')
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.query_start_time = time.time()


# noinspection PyUnusedLocal
@event.listens_for(Engine, 'after_cursor_execute')
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    if hasattr(conn, '_query_start_time'):
        elapsed = time.time() - conn.query_start_time
        conn.query_start_time = None
        query = f'{statement}'.replace('\n', ' ')
        logger.debug(f"\n{query}\n{'*' * math.floor(elapsed)} {elapsed:.3f} s\n")


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
        column_names = self.__table__.columns.keys()
        if '_sa_instance_state' in column_names:
            column_names.remove('_sa_instance_state')
        try:
            return {c: getattr(self, c) for c in column_names}
        except Exception as e:
            logger.error(f'Error occurred while converting object to dictionary: {e}')
            return {}

    def update(self, values: Dict[str, Any]) -> None:
        for key, value in values.items():
            if hasattr(self, key):
                setattr(self, key, value)


# Декоратор для кэширования методов
def cache_method(cache_key: str):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Генерация ключа для кэширования
            key = cache_key.format(*args, **kwargs)
            # Получение значения из кэша
            result = cache_region.get(key)
            if result is None:
                # Если значение отсутствует в кэше, вызываем функцию и кэшируем результат
                result = f(*args, **kwargs)
                cache_region.set(key, result)
            return result

        return decorated_function

    return decorator
