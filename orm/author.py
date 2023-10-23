from datetime import datetime
from sqlalchemy import JSON as JSONType
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from base.orm import Base, local_session


class AuthorRating(Base):
    __tablename__ = "author_rating"

    id = None  # type: ignore
    rater = Column(ForeignKey("author.id"), primary_key=True, index=True)
    author = Column(ForeignKey("author.id"), primary_key=True, index=True)
    value = Column(Integer)

    @staticmethod
    def init_table():
        pass


class AuthorFollower(Base):
    __tablename__ = "author_follower"

    id = None  # type: ignore
    follower = Column(ForeignKey("author.id"), primary_key=True, index=True)
    author = Column(ForeignKey("author.id"), primary_key=True, index=True)
    createdAt = Column(DateTime, nullable=False, default=datetime.now)
    auto = Column(Boolean, nullable=False, default=False)


class Author(Base):
    __tablename__ = "author"

    user = Column(Integer, nullable=False)  # unbounded link with authorizer's User type
    bio = Column(String, nullable=True, comment="Bio")  # status description
    about = Column(String, nullable=True, comment="About")  # long and formatted
    userpic = Column(String, nullable=True, comment="Userpic")
    name = Column(String, nullable=True, comment="Display name")
    slug = Column(String, unique=True, comment="Author's slug")
    muted = Column(Boolean, default=False)
    createdAt = Column(DateTime, nullable=False, default=datetime.now)
    lastSeen = Column(DateTime, nullable=False, default=datetime.now)  # Td se 0e
    deletedAt = Column(DateTime, nullable=True, comment="Deleted at")
    links = Column(JSONType, nullable=True, comment="Links")
    ratings = relationship(AuthorRating, foreign_keys=AuthorRating.author)

    @staticmethod
    def init_table():
        with local_session() as session:
            default = session.query(Author).filter(Author.slug == "anonymous").first()
            if not default:
                default_dict = {
                    "user": 0,
                    "name": "Аноним",
                    "slug": "anonymous",
                }
                default = Author.create(**default_dict)
                session.add(default)
                discours_dict = {
                    "user": 1,
                    "name": "Дискурс",
                    "slug": "discours",
                }
                discours = Author.create(**discours_dict)
                session.add(discours)
                session.commit()
            Author.default_author = default
