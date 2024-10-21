import time

from sqlalchemy import JSON, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from orm.author import Author
from orm.reaction import Reaction
from orm.topic import Topic
from services.db import Base


class ShoutTopic(Base):
    __tablename__ = "shout_topic"

    id = None  # type: ignore
    shout = Column(ForeignKey("shout.id"), primary_key=True, index=True)
    topic = Column(ForeignKey("topic.id"), primary_key=True, index=True)
    main = Column(Boolean, nullable=True)


class ShoutReactionsFollower(Base):
    __tablename__ = "shout_reactions_followers"

    id = None  # type: ignore
    follower = Column(ForeignKey("author.id"), primary_key=True, index=True)
    shout = Column(ForeignKey("shout.id"), primary_key=True, index=True)
    auto = Column(Boolean, nullable=False, default=False)
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    deleted_at = Column(Integer, nullable=True)


class ShoutAuthor(Base):
    __tablename__ = "shout_author"

    id = None  # type: ignore
    shout = Column(ForeignKey("shout.id"), primary_key=True, index=True)
    author = Column(ForeignKey("author.id"), primary_key=True, index=True)
    caption = Column(String, nullable=True, default="")


class Shout(Base):
    __tablename__ = "shout"

    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(Integer, nullable=True, index=True)
    published_at = Column(Integer, nullable=True, index=True)
    featured_at = Column(Integer, nullable=True, index=True)
    deleted_at = Column(Integer, nullable=True, index=True)

    created_by = Column(ForeignKey("author.id"), nullable=False)
    updated_by = Column(ForeignKey("author.id"), nullable=True)
    deleted_by = Column(ForeignKey("author.id"), nullable=True)
    community = Column(ForeignKey("community.id"), nullable=False)

    body = Column(String, nullable=False, comment="Body")
    slug = Column(String, unique=True)
    cover = Column(String, nullable=True, comment="Cover image url")
    cover_caption = Column(String, nullable=True, comment="Cover image alt caption")
    lead = Column(String, nullable=True)
    description = Column(String, nullable=True)
    title = Column(String, nullable=False)
    subtitle = Column(String, nullable=True)
    layout = Column(String, nullable=False, default="article")
    media = Column(JSON, nullable=True)

    authors = relationship(Author, secondary="shout_author")
    topics = relationship(Topic, secondary="shout_topic")
    reactions = relationship(Reaction)

    lang = Column(String, nullable=False, default="ru", comment="Language")
    version_of = Column(ForeignKey("shout.id"), nullable=True)
    oid = Column(String, nullable=True)

    seo = Column(String, nullable=True)  # JSON
