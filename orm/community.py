import enum
import time

from sqlalchemy import Column, ForeignKey, Integer, String, Text, distinct, func
from sqlalchemy.ext.hybrid import hybrid_property

from orm.author import Author
from services.db import Base


class CommunityRole(enum.Enum):
    READER = "reader"  # can read and comment
    AUTHOR = "author"  # + can vote and invite collaborators
    ARTIST = "artist"  # + can be credited as featured artist
    EXPERT = "expert"  # + can add proof or disproof to shouts, can manage topics
    EDITOR = "editor"  # + can manage topics, comments and community settings

    @classmethod
    def as_string_array(cls, roles):
        return [role.value for role in roles]


class CommunityFollower(Base):
    __tablename__ = "community_author"

    author = Column(ForeignKey("author.id"), primary_key=True)
    community = Column(ForeignKey("community.id"), primary_key=True)
    joined_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    roles = Column(Text, nullable=True, comment="Roles (comma-separated)")

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

    @property
    def role_list(self):
        return self.roles.split(",") if self.roles else []

    @role_list.setter
    def role_list(self, value):
        self.roles = ",".join(value) if value else None


class CommunityStats:
    def __init__(self, community):
        self.community = community

    @property
    def shouts(self):
        from orm.shout import Shout

        return self.community.session.query(func.count(Shout.id)).filter(Shout.community == self.community.id).scalar()

    @property
    def followers(self):
        return (
            self.community.session.query(func.count(CommunityFollower.author))
            .filter(CommunityFollower.community == self.community.id)
            .scalar()
        )

    @property
    def authors(self):
        from orm.shout import Shout

        # author has a shout with community id and its featured_at is not null
        return (
            self.community.session.query(func.count(distinct(Author.id)))
            .join(Shout)
            .filter(Shout.community == self.community.id, Shout.featured_at.is_not(None), Author.id.in_(Shout.authors))
            .scalar()
        )


class CommunityAuthor(Base):
    __tablename__ = "community_author"

    id = Column(Integer, primary_key=True)
    community_id = Column(Integer, ForeignKey("community.id"))
    author_id = Column(Integer, ForeignKey("author.id"))
    roles = Column(Text, nullable=True, comment="Roles (comma-separated)")

    @property
    def role_list(self):
        return self.roles.split(",") if self.roles else []

    @role_list.setter
    def role_list(self, value):
        self.roles = ",".join(value) if value else None
