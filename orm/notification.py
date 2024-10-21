import enum
import time

from sqlalchemy import JSON, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from orm.author import Author
from services.db import Base


class NotificationEntity(enum.Enum):
    REACTION = "reaction"
    SHOUT = "shout"
    FOLLOWER = "follower"
    COMMUNITY = "community"

    @classmethod
    def from_string(cls, value):
        return cls(value)


class NotificationAction(enum.Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    SEEN = "seen"
    FOLLOW = "follow"
    UNFOLLOW = "unfollow"

    @classmethod
    def from_string(cls, value):
        return cls(value)


class NotificationSeen(Base):
    __tablename__ = "notification_seen"

    viewer = Column(ForeignKey("author.id"), primary_key=True)
    notification = Column(ForeignKey("notification.id"), primary_key=True)


class Notification(Base):
    __tablename__ = "notification"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(Integer, server_default=str(int(time.time())))
    entity = Column(String, nullable=False)
    action = Column(String, nullable=False)
    payload = Column(JSON, nullable=True)

    seen = relationship(Author, secondary="notification_seen")

    def set_entity(self, entity: NotificationEntity):
        self.entity = entity.value

    def get_entity(self) -> NotificationEntity:
        return NotificationEntity.from_string(self.entity)

    def set_action(self, action: NotificationAction):
        self.action = action.value

    def get_action(self) -> NotificationAction:
        return NotificationAction.from_string(self.action)
