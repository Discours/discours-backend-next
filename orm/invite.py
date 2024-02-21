from enum import Enum as Enumeration

from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship

from services.db import Base


class InviteStatus(Enumeration):
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


class Invite(Base):
    __tablename__ = "invite"

    inviter_id = Column(ForeignKey("author.id"), primary_key=True)
    author_id = Column(ForeignKey("author.id"), primary_key=True)
    shout_id = Column(ForeignKey("shout.id"), primary_key=True)
    status = Column(String, default=InviteStatus.PENDING.value)

    inviter = relationship("author", foreign_keys=[inviter_id])
    author = relationship("author", foreign_keys=[author_id])
    shout = relationship("shout")
