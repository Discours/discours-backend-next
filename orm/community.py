import enum
import time

from sqlalchemy import ARRAY, Column, ForeignKey, Integer, String, distinct, func
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from orm.author import Author
from orm.shout import Shout
from services.db import Base


class CommunityRole(enum.Enum):
    AUTHOR = "author"
    READER = "reader"
    EDITOR = "editor"
    CRITIC = "critic"
    EXPERT = "expert"
    ARTIST = "artist"

    @classmethod
    def as_string_array(cls, roles):
        return [role.value for role in roles]


class CommunityFollower(Base):
    __tablename__ = "community_author"

    author = Column(ForeignKey("author.id"), primary_key=True)
    community = Column(ForeignKey("community.id"), primary_key=True)
    joined_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    roles = Column(ARRAY(String), nullable=False, default=lambda: CommunityRole.as_string_array([CommunityRole.READER]))

    def set_roles(self, roles):
        self.roles = CommunityRole.as_string_array(roles)

    def get_roles(self):
        return [CommunityRole(role) for role in self.roles]


class Community(Base):
    __tablename__ = "community"

    name = Column(String, nullable=False)
    slug = Column(String, nullable=False, unique=True)
    desc = Column(String, nullable=False, default="")
    pic = Column(String, nullable=False, default="")
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    created_by = Column(ForeignKey("author.id"), nullable=False)

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

    @property
    def authors(self):
        # author has a shout with community id and featured_at is not null
        return (
            self.community.session.query(func.count(distinct(Author.id)))
            .join(Shout)
            .filter(
                Shout.community_id == self.community.id, Shout.featured_at.is_not(None), Author.id.in_(Shout.authors)
            )
            .scalar()
        )
