import time

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from services.db import Base
from orm.author import Author


class CommunityAuthor(Base):
    __tablename__ = 'community_author'

    id = None  # type: ignore
    author = Column(ForeignKey('author.id'), primary_key=True)
    community = Column(ForeignKey('community.id'), primary_key=True)
    joined_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    role = Column(String, nullable=False)


class Community(Base):
    __tablename__ = 'community'

    name = Column(String, nullable=False)
    slug = Column(String, nullable=False, unique=True)
    desc = Column(String, nullable=False, default='')
    pic = Column(String, nullable=False, default='')
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))

    authors = relationship(Author, secondary='shout_author')
