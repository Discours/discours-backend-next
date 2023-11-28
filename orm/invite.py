from sqlalchemy import Column, ForeignKey, Enum
from sqlalchemy.orm import relationship
from services.db import Base
from orm.author import Author
from orm.shout import Shout
from enum import Enum as Enumeration


class InviteStatus(Enumeration):
    PENDING = 0
    ACCEPTED = 1
    REJECTED = 2


class Invite(Base):
    __tablename__ = "invite"

    inviter_id = Column(ForeignKey("author.id"), nullable=False, index=True)
    author_id = Column(ForeignKey("author.id"), nullable=False, index=True)
    shout_id = Column(ForeignKey("shout.id"), nullable=False, index=True)
    status = Column(Enum(InviteStatus), default=InviteStatus.PENDING)

    inviter = relationship(Author, foreign_keys=[inviter_id])
    author = relationship(Author, foreign_keys=[author_id])
    shout = relationship(Shout)
