import time
from enum import Enum as Enumeration

from sqlalchemy import JSON, Boolean, Column, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from orm.author import Author
from orm.community import Community
from orm.reaction import Reaction
from orm.topic import Topic
from services.db import Base


class ShoutTopic(Base):
    __tablename__ = "shout_topic"

    id = None  # type: ignore
    shout = Column(ForeignKey("shout.id"), primary_key=True, index=True)
    topic = Column(ForeignKey("topic.id"), primary_key=True, index=True)


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


class ShoutCommunity(Base):
    __tablename__ = "shout_community"

    id = None  # type: ignore
    shout = Column(ForeignKey("shout.id"), primary_key=True, index=True)
    community = Column(ForeignKey("community.id"), primary_key=True, index=True)


class ShoutVisibility(Enumeration):
    AUTHORS = "AUTHORS"
    COMMUNITY = "COMMUNITY"
    PUBLIC = "PUBLIC"


class Shout(Base):
    __tablename__ = "shout"

    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(Integer, nullable=True)
    published_at = Column(Integer, nullable=True)
    deleted_at = Column(Integer, nullable=True)

    created_by = Column(ForeignKey("author.id"), nullable=False)
    updated_by = Column(ForeignKey("author.id"), nullable=True)
    deleted_by = Column(ForeignKey("author.id"), nullable=True)

    body = Column(String, nullable=False, comment="Body")
    slug = Column(String, unique=True)
    cover = Column(String, nullable=True, comment="Cover image url")
    lead = Column(String, nullable=True)
    description = Column(String, nullable=True)
    title = Column(String, nullable=True)
    subtitle = Column(String, nullable=True)
    layout = Column(String, nullable=True)
    media = Column(JSON, nullable=True)

    authors = relationship(lambda: Author, secondary="shout_author")
    topics = relationship(lambda: Topic, secondary="shout_topic")
    communities = relationship(lambda: Community, secondary="shout_community")
    reactions = relationship(lambda: Reaction)

    visibility = Column(String, default=ShoutVisibility.AUTHORS.value)

    lang = Column(String, nullable=False, default="ru", comment="Language")
    version_of = Column(ForeignKey("shout.id"), nullable=True)
    oid = Column(String, nullable=True)
