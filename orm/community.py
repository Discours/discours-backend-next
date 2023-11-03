import time
from sqlalchemy import Column, String, ForeignKey, Integer
from sqlalchemy.orm import relationship

from services.db import Base, local_session
from orm.author import Author


class CommunityAuthor(Base):
    __tablename__ = "community_author"

    id = None  # type: ignore
    follower = Column(ForeignKey("author.id"), primary_key=True)
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

    authors = relationship(lambda: Author, secondary=CommunityAuthor.__tablename__)

    @staticmethod
    def init_table():
        with local_session() as session:
            d = session.query(Community).filter(Community.slug == "discours").first()
            if not d:
                d = Community.create(name="Дискурс", slug="discours")
                print("[orm] created community %s" % d.slug)
            Community.default_community = d
            print("[orm] default community is %s" % d.slug)
