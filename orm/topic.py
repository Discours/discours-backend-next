import time

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String

from services.db import Base


class TopicFollower(Base):
    __tablename__ = "topic_followers"

    id = None  # type: ignore
    follower = Column(ForeignKey("author.id"), primary_key=True, index=True)
    topic = Column(ForeignKey("topic.id"), primary_key=True, index=True)
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    auto = Column(Boolean, nullable=False, default=False)


class Topic(Base):
    __tablename__ = "topic"

    slug = Column(String, unique=True)
    title = Column(String, nullable=False, comment="Title")
    body = Column(String, nullable=True, comment="Body")
    pic = Column(String, nullable=True, comment="Picture")
    community = Column(ForeignKey("community.id"), default=1)
    oid = Column(String, nullable=True, comment="Old ID")
