import time

from sqlalchemy import JSON, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy_utils import TSVectorType

from services.db import Base


class AuthorRating(Base):
    __tablename__ = 'author_rating'

    id = None  # type: ignore
    rater = Column(ForeignKey('author.id'), primary_key=True)
    author = Column(ForeignKey('author.id'), primary_key=True)
    plus = Column(Boolean)


class AuthorFollower(Base):
    __tablename__ = 'author_follower'

    id = None  # type: ignore
    follower = Column(ForeignKey('author.id'), primary_key=True)
    author = Column(ForeignKey('author.id'), primary_key=True)
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    auto = Column(Boolean, nullable=False, default=False)


class Author(Base):
    __tablename__ = 'author'

    user = Column(String)  # unbounded link with authorizer's User type

    name = Column(String, nullable=True, comment='Display name')
    slug = Column(String, unique=True, comment="Author's slug")
    bio = Column(String, nullable=True, comment='Bio')  # status description
    about = Column(String, nullable=True, comment='About')  # long and formatted
    pic = Column(String, nullable=True, comment='Picture')
    links = Column(JSON, nullable=True, comment='Links')
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    last_seen = Column(Integer, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    deleted_at = Column(Integer, nullable=True, comment='Deleted at')

    search_vector = Column(TSVectorType("name", "slug", "bio", "about"))
