import time

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from orm.author import Author
from services.db import Base, local_session


class CommunityAuthor(Base):
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

    authors = relationship(lambda: Author, secondary=CommunityAuthor.__tablename__)

    @staticmethod
    def init_table():
        with local_session("orm.community") as session:
            d = session.query(Community).filter(Community.slug == "discours").first()
            if not d:
                d = Community(name="Дискурс", slug="discours")
                session.add(d)
                session.commit()
                print("[orm.community] created community %s" % d.slug)
            Community.default_community = d
            print("[orm.community] default community is %s" % d.slug)
