import time
from enum import Enum as Enumeration

from sqlalchemy import JSON, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from orm.author import Author
from services.db import Base, create_table_if_not_exists, engine
from utils.logger import root_logger as logger


class NotificationEntity(Enumeration):
    REACTION = "reaction"
    SHOUT = "shout"
    FOLLOWER = "follower"


class NotificationAction(Enumeration):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    SEEN = "seen"
    FOLLOW = "follow"
    UNFOLLOW = "unfollow"


class NotificationSeen(Base):
    __tablename__ = "notification_seen"

    viewer = Column(ForeignKey("author.id"))
    notification = Column(ForeignKey("notification.id"))


class Notification(Base):
    __tablename__ = "notification"

    created_at = Column(Integer, server_default=str(int(time.time())))
    entity = Column(String, nullable=False)
    action = Column(String, nullable=False)
    payload = Column(JSON, nullable=True)

    seen = relationship(lambda: Author, secondary="notification_seen")
