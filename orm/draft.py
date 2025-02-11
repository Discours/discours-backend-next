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
    # required
    created_at: int = Column(Integer, nullable=False, default=lambda: int(time.time()))
    created_by: int = Column(ForeignKey("author.id"), nullable=False)

    # optional
    layout: str = Column(String, nullable=True, default="article")
    slug: str = Column(String, unique=True)
    title: str = Column(String, nullable=True)
    subtitle: str | None = Column(String, nullable=True)
    lead: str | None = Column(String, nullable=True)
    description: str | None = Column(String, nullable=True)
    body: str = Column(String, nullable=False, comment="Body")
    media: dict | None = Column(JSON, nullable=True)
    cover: str | None = Column(String, nullable=True, comment="Cover image url")
    cover_caption: str | None = Column(String, nullable=True, comment="Cover image alt caption")
    lang: str = Column(String, nullable=False, default="ru", comment="Language")
    seo: str | None = Column(String, nullable=True)  # JSON

    # auto
    updated_at: int | None = Column(Integer, nullable=True, index=True)
    deleted_at: int | None = Column(Integer, nullable=True, index=True)
    updated_by: int | None = Column(ForeignKey("author.id"), nullable=True)
    deleted_by: int | None = Column(ForeignKey("author.id"), nullable=True)
    authors = relationship(Author, secondary="draft_author")
    topics = relationship(Topic, secondary="draft_topic")
