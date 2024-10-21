import time

from requests import Session
from sqlalchemy import Column, ForeignKey, Integer, String, func
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.orm import relationship

from orm.author import Author
from services.db import Base


class CommunityFollower(Base):
    __tablename__ = "community_author"

    id = None  # type: ignore
    author = Column(ForeignKey("author.id"), primary_key=True)
    community = Column(ForeignKey("community.id"), primary_key=True)
    joined_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    role = Column(String, nullable=False)


class Community(Base):
    __tablename__ = "community"

    name = Column(String, nullable=False)
    slug = Column(String, nullable=False, unique=True)
    desc = Column(String, nullable=False, default="")
    pic = Column(String, nullable=False, default="")
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))

    authors = relationship(Author, secondary="community_author")

    @hybrid_method
    def get_stats(self, session: Session):
        from orm.shout import ShoutCommunity  # Импорт здесь во избежание циклических зависимостей

        shouts_count = (
            session.query(func.count(ShoutCommunity.shout_id)).filter(ShoutCommunity.community_id == self.id).scalar()
        )

        followers_count = (
            session.query(func.count(CommunityFollower.author)).filter(CommunityFollower.community == self.id).scalar()
        )

        return {"shouts": shouts_count, "followers": followers_count}
