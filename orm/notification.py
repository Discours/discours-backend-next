from datetime import datetime
from sqlalchemy import Column, Enum, ForeignKey, DateTime, Boolean, Integer
from sqlalchemy.dialects.postgresql import JSONB

from base.orm import Base
from enum import Enum as Enumeration


class NotificationType(Enumeration):
    NEW_COMMENT = 1
    NEW_REPLY = 2


class Notification(Base):
    __tablename__ = "notification"

    shout = Column(ForeignKey("shout.id"), index=True)
    reaction = Column(ForeignKey("reaction.id"), index=True)
    user = Column(ForeignKey("user.id"), index=True)
    createdAt = Column(DateTime, nullable=False, default=datetime.now, index=True)
    seen = Column(Boolean, nullable=False, default=False, index=True)
    type = Column(Enum(NotificationType), nullable=False)
    data = Column(JSONB, nullable=True)
    occurrences = Column(Integer, default=1)
