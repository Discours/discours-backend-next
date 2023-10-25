from datetime import datetime
from sqlalchemy import JSON as JSONType
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from services.db import Base


class AuthorRating(Base):
    __tablename__ = "author_rating"

    id = None  # type: ignore
    rater = Column(ForeignKey("author.id"), primary_key=True, index=True)
    author = Column(ForeignKey("author.id"), primary_key=True, index=True)
    value = Column(Integer)

    @staticmethod
    def init_table():
        pass


class AuthorFollower(Base):
    __tablename__ = "author_follower"

    id = None  # type: ignore
    follower = Column(ForeignKey("author.id"), primary_key=True, index=True)
    author = Column(ForeignKey("author.id"), primary_key=True, index=True)
    createdAt = Column(DateTime, nullable=False, default=datetime.now)
    auto = Column(Boolean, nullable=False, default=False)


class Author(Base):
    __tablename__ = "author"

    user = Column(String, nullable=False)  # unbounded link with authorizer's User type
    bio = Column(String, nullable=True, comment="Bio")  # status description
    about = Column(String, nullable=True, comment="About")  # long and formatted
    pic = Column(String, nullable=True, comment="Userpic")
    name = Column(String, nullable=True, comment="Display name")
    slug = Column(String, unique=True, comment="Author's slug")
    
    createdAt = Column(DateTime, nullable=False, default=datetime.now)
    lastSeen = Column(DateTime, nullable=False, default=datetime.now)  # Td se 0e
    deletedAt = Column(DateTime, nullable=True, comment="Deleted at")

    links = Column(JSONType, nullable=True, comment="Links")
    ratings = relationship(AuthorRating, foreign_keys=AuthorRating.author)
