import time
from enum import Enum as Enumeration

from sqlalchemy import JSON, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.exc import ProgrammingError

from orm.author import Author
from services.db import Base, engine
from services.logger import root_logger as logger


class NotificationEntity(Enumeration):
    REACTION = 'reaction'
    SHOUT = 'shout'
    FOLLOWER = 'follower'


class NotificationAction(Enumeration):
    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'
    SEEN = 'seen'
    FOLLOW = 'follow'
    UNFOLLOW = 'unfollow'


class NotificationSeen(Base):
    __tablename__ = 'notification_seen'

    viewer = Column(ForeignKey('author.id'))
    notification = Column(ForeignKey('notification.id'))


class Notification(Base):
    __tablename__ = 'notification'

    created_at = Column(Integer, server_default=str(int(time.time())))
    entity = Column(String, nullable=False)
    action = Column(String, nullable=False)
    payload = Column(JSON, nullable=True)

    seen = relationship(lambda: Author, secondary='notification_seen')


try:
    Notification.__table__.create(engine)
    logger.info("Table `notification` was created.")
except ProgrammingError:
    # Handle the exception here, for example by printing a message
    logger.info("Table `notification` already exists.")
