import time

from sqlalchemy import Column, ForeignKey, Integer, String

from services.db import Base


class ShoutCollection(Base):
    __tablename__ = 'shout_collection'

    id = None  # type: ignore
    shout = Column(ForeignKey('shout.id'), primary_key=True)
    collection = Column(ForeignKey('collection.id'), primary_key=True)


class Collection(Base):
    __tablename__ = 'collection'

    slug = Column(String, unique=True)
    title = Column(String, nullable=False, comment='Title')
    body = Column(String, nullable=True, comment='Body')
    pic = Column(String, nullable=True, comment='Picture')
    created_at = Column(Integer, default=lambda: int(time.time()))
    created_by = Column(ForeignKey('author.id'), comment='Created By')
    published_at = Column(Integer, default=lambda: int(time.time()))
