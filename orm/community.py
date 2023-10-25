from datetime import datetime
from sqlalchemy import Column, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship

from services.db import Base, local_session
from orm.author import Author


class CommunityAuthor(Base):
    __tablename__ = "community_author"

    id = None  # type: ignore
    follower = Column(ForeignKey("author.id"), primary_key=True)
    community = Column(ForeignKey("community.id"), primary_key=True)
    joinedAt = Column(DateTime, nullable=False, default=datetime.now)
    role = Column(String, nullable=False)


class Community(Base):
    __tablename__ = "community"

    name = Column(String, nullable=False)
    slug = Column(String, nullable=False, unique=True)
    desc = Column(String, nullable=False, default="")
    pic = Column(String, nullable=False, default="")
    createdAt = Column(DateTime, nullable=False, default=datetime.now)

    authors = relationship(lambda: Author, secondary=CommunityAuthor.__tablename__, nullable=True)

    @staticmethod
    def init_table():
        with local_session() as session:
            d = (session.query(Community).filter(Community.slug == "discours").first())
            if not d:
                d = Community.create(name="Дискурс", slug="discours")
                session.add(d)
                session.commit()
            Community.default_community = d
            print('[orm] default community id: %s' % d.id)
