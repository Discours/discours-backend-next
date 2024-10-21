from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import ARRAY, Column, ForeignKey, Integer, String, func, Enum
from sqlalchemy.orm import relationship
import enum
import time

from orm.author import Author
from services.db import Base


class CommunityRole(enum.Enum):
    AUTHOR = "author"
    READER = "reader"
    EDITOR = "editor"
    CRITIC = "critic"
    EXPERT = "expert"
    ARTIST = "artist"


class CommunityFollower(Base):
    __tablename__ = "community_author"

    author = Column(ForeignKey("author.id"), primary_key=True)
    community = Column(ForeignKey("community.id"), primary_key=True)
    joined_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    roles = Column(ARRAY(Enum(CommunityRole)), nullable=False, default=[CommunityRole.READER])


class Community(Base):
    __tablename__ = "community"

    name = Column(String, nullable=False)
    slug = Column(String, nullable=False, unique=True)
    desc = Column(String, nullable=False, default="")
    pic = Column(String, nullable=False, default="")
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))

    authors = relationship(Author, secondary="community_author")

    @hybrid_property
    def stat(self):
        return CommunityStats(self)


class CommunityStats:
    def __init__(self, community):
        self.community = community

    @property
    def shouts(self):
        from orm.shout import ShoutCommunity

        return (
            self.community.session.query(func.count(ShoutCommunity.shout_id))
            .filter(ShoutCommunity.community_id == self.community.id)
            .scalar()
        )

    @property
    def followers(self):
        return (
            self.community.session.query(func.count(CommunityFollower.author))
            .filter(CommunityFollower.community == self.community.id)
            .scalar()
        )
