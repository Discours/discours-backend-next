from enum import Enum as Enumeration

from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship

from orm.author import Author
from orm.shout import Shout
from services.db import Base


class InviteStatus(Enumeration):
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


class Invite(Base):
    __tablename__ = "invite"

    inviter_id = Column(ForeignKey("author.id"), nullable=False, index=True)
    author_id = Column(ForeignKey("author.id"), nullable=False, index=True)
    shout_id = Column(ForeignKey("shout.id"), nullable=False, index=True)
    status = Column(String, default=InviteStatus.PENDING.value)

    inviter = relationship(Author, foreign_keys=[inviter_id])
    author = relationship(Author, foreign_keys=[author_id])
    shout = relationship(Shout)
