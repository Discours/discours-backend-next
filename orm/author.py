import time

from sqlalchemy import JSON, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from services.db import Base


class AuthorRating(Base):
    __tablename__ = 'author_rating'

    id = None  # type: ignore
    rater = Column(ForeignKey('author.id'), primary_key=True, index=True)
    author = Column(ForeignKey('author.id'), primary_key=True, index=True)
    plus = Column(Boolean)


class AuthorFollower(Base):
    __tablename__ = 'author_follower'

    id = None  # type: ignore
    follower = Column(ForeignKey('author.id'), primary_key=True, index=True)
    author = Column(ForeignKey('author.id'), primary_key=True, index=True)
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    auto = Column(Boolean, nullable=False, default=False)


class Author(Base):
    __tablename__ = 'author'

    user = Column(String, unique=True)  # unbounded link with authorizer's User type

    name = Column(String, nullable=True, comment='Display name')
    slug = Column(String, unique=True, comment="Author's slug", index=True)
    bio = Column(String, nullable=True, comment='Bio')  # status description
    about = Column(String, nullable=True, comment='About')  # long and formatted
    pic = Column(String, nullable=True, comment='Picture')
    links = Column(JSON, nullable=True, comment='Links')

    ratings = relationship(AuthorRating, foreign_keys=AuthorRating.author)

    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    last_seen = Column(Integer, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    deleted_at = Column(Integer, nullable=True, comment='Deleted at')
