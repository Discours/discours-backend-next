import time

from sqlalchemy import JSON, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from orm.author import Author
from orm.topic import Topic
from services.db import Base


class DraftTopic(Base):
    __tablename__ = "draft_topic"

    id = None  # type: ignore
    shout = Column(ForeignKey("draft.id"), primary_key=True, index=True)
    topic = Column(ForeignKey("topic.id"), primary_key=True, index=True)
    main = Column(Boolean, nullable=True)


class DraftAuthor(Base):
    __tablename__ = "draft_author"

    id = None  # type: ignore
    shout = Column(ForeignKey("draft.id"), primary_key=True, index=True)
    author = Column(ForeignKey("author.id"), primary_key=True, index=True)
    caption = Column(String, nullable=True, default="")


class Draft(Base):
    __tablename__ = "draft"

    created_at: int = Column(Integer, nullable=False, default=lambda: int(time.time()))
    updated_at: int | None = Column(Integer, nullable=True, index=True)
    deleted_at: int | None = Column(Integer, nullable=True, index=True)

    body: str = Column(String, nullable=False, comment="Body")
    slug: str = Column(String, unique=True)
    cover: str | None = Column(String, nullable=True, comment="Cover image url")
    cover_caption: str | None = Column(String, nullable=True, comment="Cover image alt caption")
    lead: str | None = Column(String, nullable=True)
    description: str | None = Column(String, nullable=True)
    title: str = Column(String, nullable=False)
    subtitle: str | None = Column(String, nullable=True)
    layout: str = Column(String, nullable=False, default="article")
    media: dict | None = Column(JSON, nullable=True)

    lang: str = Column(String, nullable=False, default="ru", comment="Language")
    oid: str | None = Column(String, nullable=True)
    seo: str | None = Column(String, nullable=True)  # JSON

    created_by: int = Column(ForeignKey("author.id"), nullable=False)
    updated_by: int | None = Column(ForeignKey("author.id"), nullable=True)
    deleted_by: int | None = Column(ForeignKey("author.id"), nullable=True)
    authors = relationship(Author, secondary="draft_author")
    topics = relationship(Topic, secondary="draft_topic")
    shout: int | None = Column(ForeignKey("shout.id"), nullable=True)
