import time

from sqlalchemy import JSON, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy import event
from sqlalchemy.orm import relationship

from services.db import Base
from orm.community import Community
from orm.author import Author
from orm.author import get_object, update_follows, update_app_data
from orm.reaction import Reaction
from orm.topic import Topic


class ShoutTopic(Base):
    __tablename__ = 'shout_topic'

    id = None  # type: ignore
    shout = Column(ForeignKey('shout.id'), primary_key=True)
    topic = Column(ForeignKey('topic.id'), primary_key=True)
    main = Column(Boolean, nullable=True)


class ShoutReactionsFollower(Base):
    __tablename__ = 'shout_reactions_followers'

    id = None  # type: ignore
    follower = Column(ForeignKey('author.id'), primary_key=True)
    shout = Column(ForeignKey('shout.id'), primary_key=True)
    auto = Column(Boolean, nullable=False, default=False)
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    deleted_at = Column(Integer, nullable=True)


class ShoutAuthor(Base):
    __tablename__ = 'shout_author'

    id = None  # type: ignore
    shout = Column(ForeignKey('shout.id'), primary_key=True)
    author = Column(ForeignKey('author.id'), primary_key=True)
    caption = Column(String, nullable=True, default='')


class ShoutCommunity(Base):
    __tablename__ = 'shout_community'

    id = None  # type: ignore
    shout = Column(ForeignKey('shout.id'), primary_key=True)
    community = Column(ForeignKey('community.id'), primary_key=True)


class Shout(Base):
    __tablename__ = 'shout'

    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(Integer, nullable=True)
    published_at = Column(Integer, nullable=True)
    featured_at = Column(Integer, nullable=True)
    deleted_at = Column(Integer, nullable=True)

    created_by = Column(ForeignKey('author.id'), nullable=False)
    updated_by = Column(ForeignKey('author.id'), nullable=True)
    deleted_by = Column(ForeignKey('author.id'), nullable=True)

    body = Column(String, nullable=False, comment='Body')
    slug = Column(String, unique=True)
    cover = Column(String, nullable=True, comment='Cover image url')
    cover_caption = Column(String, nullable=True, comment='Cover image alt caption')
    lead = Column(String, nullable=True)
    description = Column(String, nullable=True)
    title = Column(String, nullable=False)
    subtitle = Column(String, nullable=True)
    layout = Column(String, nullable=False, default='article')
    media = Column(JSON, nullable=True)

    authors = relationship(Author, secondary='shout_author')
    topics = relationship(Topic, secondary='shout_topic')
    communities = relationship(Community, secondary='shout_community')
    reactions = relationship(Reaction)

    lang = Column(String, nullable=False, default='ru', comment='Language')
    version_of = Column(ForeignKey('shout.id'), nullable=True)
    oid = Column(String, nullable=True)

    seo = Column(String, nullable=True)  # JSON


@event.listens_for(ShoutReactionsFollower, 'after_insert')
@event.listens_for(ShoutReactionsFollower, 'after_delete')
def after_topic_follower_change(mapper, connection, target):
    shout_id = target.shout
    follower_id = target.follower
    user = get_object(connection, 'authorizer_users', follower_id)
    if user:
        app_data = update_follows(user, 'shout', get_object(connection, 'shout', shout_id))
        update_app_data(connection, follower_id, app_data)
