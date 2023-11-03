from contextlib import contextmanager
import logging
from typing import TypeVar, Any, Dict, Generic, Callable
from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import Table
from settings import DB_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_engine(DB_URL, echo=False, pool_size=10, max_overflow=20)
Session = sessionmaker(bind=engine, expire_on_commit=False)

T = TypeVar("T")

REGISTRY: Dict[str, type] = {}


@contextmanager
def local_session():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        print(f"[services.db] Error session: {e}")
        session.rollback()
        raise
    finally:
        session.close()


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

    @classmethod
    def create(cls: Generic[T], **kwargs) -> Generic[T]:
        try:
            instance = cls(**kwargs)
            return instance.save()
        except Exception as e:
            print(f"[services.db] Error create: {e}")
            return None

    def save(self) -> Generic[T]:
        with local_session() as session:
            try:
                session.add(self)
            except Exception as e:
                print(f"[services.db] Error save: {e}")
        return self

    def update(self, input):
        column_names = self.__table__.columns.keys()
        for name, value in input.items():
            if name in column_names:
                setattr(self, name, value)
        with local_session() as session:
            try:
                session.commit()
            except Exception as e:
                print(f"[services.db] Error update: {e}")

    def dict(self) -> Dict[str, Any]:
        column_names = self.__table__.columns.keys()
        try:
            return {c: getattr(self, c) for c in column_names}
        except Exception as e:
            print(f"[services.db] Error dict: {e}")
            return {}
